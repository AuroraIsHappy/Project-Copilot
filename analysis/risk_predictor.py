from datetime import date
from datetime import datetime


def _parse_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value[:10]).date()
        except ValueError:
            return None
    return None


def _to_progress_percent(raw_progress) -> int:
    try:
        value = float(raw_progress)
    except (TypeError, ValueError):
        return 0

    if value <= 1:
        value *= 100
    return max(0, min(100, int(round(value))))


def _expected_progress_percent(task: dict, today: date) -> int:
    start_date = _parse_date(task.get("start"))
    end_date = _parse_date(task.get("end"))

    if not start_date or not end_date or end_date <= start_date:
        duration_weeks = task.get("duration") or task.get("duration_weeks") or 1
        try:
            duration_days = max(1, int(duration_weeks) * 7)
        except (TypeError, ValueError):
            duration_days = 7

        if start_date and today >= start_date:
            elapsed_days = (today - start_date).days
            if elapsed_days >= duration_days:
                return 100
            return max(0, min(100, int(round(elapsed_days * 100 / duration_days))))
        return 0

    total_days = max(1, (end_date - start_date).days)
    elapsed_days = (today - start_date).days

    if elapsed_days <= 0:
        return 0
    if elapsed_days >= total_days:
        return 100
    return max(0, min(100, int(round(elapsed_days * 100 / total_days))))


def _derive_status(original_status: str, actual_progress: int, is_risk: bool) -> str:
    if actual_progress >= 100:
        return "Done"
    if is_risk:
        return "At Risk"
    if actual_progress > 0:
        return "In Progress"

    status = str(original_status or "").strip()
    return status or "Planned"


def predict_risk(schedule, current_state):
    tasks = list(schedule or [])
    state = current_state if isinstance(current_state, dict) else {}
    threshold = state.get("risk_lag_threshold", 30)

    try:
        risk_lag_threshold = max(0, min(100, int(threshold)))
    except (TypeError, ValueError):
        risk_lag_threshold = 30

    today = _parse_date(state.get("today")) or date.today()

    updated_tasks = []
    risk_items = []

    for task in tasks:
        task_copy = dict(task)
        expected_progress = _expected_progress_percent(task_copy, today)
        actual_progress = _to_progress_percent(task_copy.get("progress", 0))
        lag_percent = expected_progress - actual_progress
        is_risk = lag_percent >= risk_lag_threshold

        task_copy["expected_progress"] = expected_progress
        task_copy["lag_percent"] = lag_percent
        task_copy["status"] = _derive_status(task_copy.get("status", "Planned"), actual_progress, is_risk)
        updated_tasks.append(task_copy)

        if not is_risk:
            continue

        task_id = str(task_copy.get("task_id") or "").strip() or "Unknown"
        task_name = str(task_copy.get("task") or "").strip() or "Unnamed Task"
        risk_items.append(
            {
                "task_id": task_id,
                "task": task_name,
                "expected_progress": expected_progress,
                "actual_progress": actual_progress,
                "lag_percent": lag_percent,
                "message": f"{task_name} 出现进度风险，应完成 {expected_progress}%，实际完成 {actual_progress}%（落后 {lag_percent}%）",
            }
        )

    return {
        "today": today.isoformat(),
        "risk_lag_threshold": risk_lag_threshold,
        "tasks": updated_tasks,
        "risk_items": risk_items,
        "risk_count": len(risk_items),
    }