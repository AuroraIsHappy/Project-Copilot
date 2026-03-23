from datetime import date
from datetime import timedelta


DEFAULT_STATUS = "Planned"
DEFAULT_OWNERS = ("Strategy", "Product", "Engineering", "Data")


def _normalize_target_total_days(target_total_days) -> int | None:
    if target_total_days is None:
        return None
    try:
        return max(1, int(target_total_days))
    except (TypeError, ValueError):
        return None


def _task_id(task: dict, index: int) -> str:
    return str(task.get("task_id") or f"T{index}").strip() or f"T{index}"


def _days_to_weeks(raw_value, *, allow_negative: bool) -> int:
    try:
        days = int(raw_value)
    except (TypeError, ValueError):
        return 0

    sign = -1 if days < 0 else 1
    weeks = (abs(days) + 6) // 7
    if not allow_negative and sign < 0:
        return 0
    return sign * weeks


def _duration_days(task: dict) -> int:
    raw_weeks = task.get("duration_weeks")
    if raw_weeks is None:
        raw_weeks = task.get("duration")

    if raw_weeks is not None:
        try:
            weeks = max(1, int(raw_weeks))
            return weeks * 7
        except (TypeError, ValueError):
            pass

    raw_days = task.get("duration_days")
    try:
        days = max(1, int(raw_days))
    except (TypeError, ValueError):
        return 7

    weeks = max(1, (days + 6) // 7)
    return weeks * 7


def _manual_start_offset_days(task: dict) -> int:
    raw_start_week = task.get("start_week")
    try:
        start_week = int(raw_start_week)
    except (TypeError, ValueError):
        return 0
    return max(0, start_week - 1) * 7


def _has_manual_start_week(task: dict) -> bool:
    raw_start_week = task.get("start_week")
    if raw_start_week is None:
        return False
    try:
        return int(raw_start_week) >= 1
    except (TypeError, ValueError):
        return False


def _constraint_weight(dep: dict, duration_a: int, duration_b: int) -> int:
    dep_type = str(dep.get("type") or "FS").upper().strip()

    raw_lag_weeks = dep.get("lag_weeks")
    if raw_lag_weeks is None:
        raw_lag_weeks = _days_to_weeks(dep.get("lag_days", 0), allow_negative=True)
    try:
        lag_weeks = int(raw_lag_weeks)
    except (TypeError, ValueError):
        lag_weeks = 0
    lag_days = lag_weeks * 7

    raw_overlap_weeks = dep.get("overlap_weeks")
    if raw_overlap_weeks is None:
        raw_overlap_weeks = dep.get("overlap")
    if raw_overlap_weeks is None:
        raw_overlap_weeks = _days_to_weeks(dep.get("overlap_days", 0), allow_negative=False)
    try:
        overlap_weeks = max(0, int(raw_overlap_weeks))
    except (TypeError, ValueError):
        overlap_weeks = 0
    overlap_days = overlap_weeks * 7

    if dep_type == "SS":
        return lag_days - overlap_days
    if dep_type == "FF":
        return duration_a - duration_b + lag_days - overlap_days
    if dep_type == "SF":
        return -duration_b + lag_days - overlap_days
    # default FS
    return duration_a + lag_days - overlap_days


def _sequential_schedule(tasks: list[dict], start_date: date) -> list[dict]:
    cursor = start_date
    result: list[dict] = []

    for index, task in enumerate(tasks, start=1):
        days = _duration_days(task)
        duration_weeks = max(1, days // 7)
        if _has_manual_start_week(task):
            task_start = start_date + timedelta(days=_manual_start_offset_days(task))
        else:
            task_start = cursor
        end = task_start + timedelta(days=days)
        scheduled_task = dict(task)
        scheduled_task.update(
            {
                "task_id": _task_id(task, index),
                "task": task.get("task") or task.get("task_name") or task.get("name") or f"Task {index}",
                "start": task_start,
                "end": end,
                "duration_days": days,
                "duration_weeks": duration_weeks,
                "duration": duration_weeks,
                "owner": task.get("owner") or DEFAULT_OWNERS[(index - 1) % len(DEFAULT_OWNERS)],
                "status": task.get("status") or DEFAULT_STATUS,
                "progress": task.get("progress", 0),
            }
        )
        result.append(scheduled_task)
        cursor = max(cursor, end)

    return result


def _dependency_schedule(tasks: list[dict], dependencies: list[dict], start_date: date) -> list[dict]:
    task_ids = [_task_id(task, i) for i, task in enumerate(tasks, start=1)]
    id_to_index = {tid: idx for idx, tid in enumerate(task_ids)}
    durations = {tid: _duration_days(tasks[idx]) for tid, idx in id_to_index.items()}

    # Difference constraints form: start[to] >= start[from] + weight
    constraints: list[tuple[str, str, int]] = []
    for dep in dependencies:
        if not isinstance(dep, dict):
            continue
        source = str(dep.get("from") or "").strip()
        target = str(dep.get("to") or "").strip()
        if source not in id_to_index or target not in id_to_index:
            continue
        if source == target:
            continue

        weight = _constraint_weight(dep, durations[source], durations[target])
        constraints.append((source, target, weight))

    pinned_task_ids = {
        tid for tid in task_ids
        if _has_manual_start_week(tasks[id_to_index[tid]])
    }
    starts = {
        tid: _manual_start_offset_days(tasks[id_to_index[tid]]) if tid in pinned_task_ids else 0
        for tid in task_ids
    }
    task_count = len(task_ids)
    updated = False

    for _ in range(max(1, task_count - 1)):
        updated = False
        for source, target, weight in constraints:
            candidate = starts[source] + weight
            if target in pinned_task_ids:
                continue
            if starts[target] < candidate:
                starts[target] = candidate
                updated = True
        if not updated:
            break

    # Positive-cycle-like updates imply conflicting constraints.
    if updated:
        return _sequential_schedule(tasks, start_date)

    min_start = min(starts.values()) if starts else 0
    if min_start < 0:
        for tid in starts:
            starts[tid] -= min_start

    scheduled_tasks: list[dict] = []
    for index, task in enumerate(tasks, start=1):
        tid = task_ids[index - 1]
        days = durations[tid]
        duration_weeks = max(1, days // 7)
        start = start_date + timedelta(days=starts[tid])
        end = start + timedelta(days=days)
        scheduled_task = dict(task)
        scheduled_task.update(
            {
                "task_id": tid,
                "task": task.get("task") or task.get("task_name") or task.get("name") or f"Task {index}",
                "start": start,
                "end": end,
                "duration_days": days,
                "duration_weeks": duration_weeks,
                "duration": duration_weeks,
                "owner": task.get("owner") or DEFAULT_OWNERS[(index - 1) % len(DEFAULT_OWNERS)],
                "status": task.get("status") or DEFAULT_STATUS,
                "progress": task.get("progress", 0),
            }
        )
        scheduled_tasks.append(scheduled_task)

    return scheduled_tasks


def _align_schedule_to_total_days(scheduled_tasks: list[dict], target_total_days: int | None) -> list[dict]:
    target_days = _normalize_target_total_days(target_total_days)
    if target_days is None or not scheduled_tasks:
        return scheduled_tasks

    starts = [task.get("start") for task in scheduled_tasks if isinstance(task.get("start"), date)]
    ends = [task.get("end") for task in scheduled_tasks if isinstance(task.get("end"), date)]
    if not starts or not ends:
        return scheduled_tasks

    min_start = min(starts)
    max_end = max(ends)
    current_total_days = max(1, (max_end - min_start).days)
    if current_total_days == target_days:
        return scheduled_tasks

    # Keep timeline math on whole-week boundaries to avoid mid-week drift.
    current_total_weeks = max(1, (current_total_days + 6) // 7)
    target_total_weeks = max(1, (target_days + 6) // 7)
    if current_total_weeks == target_total_weeks:
        return scheduled_tasks

    aligned_tasks: list[dict] = []
    for task in scheduled_tasks:
        task_start = task.get("start")
        task_end = task.get("end")
        if not isinstance(task_start, date) or not isinstance(task_end, date):
            aligned_tasks.append(task)
            continue

        start_offset_days = max(0, (task_start - min_start).days)
        end_offset_days = max(start_offset_days + 1, (task_end - min_start).days)

        start_offset_weeks = max(0, start_offset_days // 7)
        end_offset_weeks = max(start_offset_weeks + 1, (end_offset_days + 6) // 7)

        new_start_week_offset = (start_offset_weeks * target_total_weeks) // current_total_weeks
        new_end_week_offset = (end_offset_weeks * target_total_weeks + current_total_weeks - 1) // current_total_weeks

        new_start_week_offset = min(max(0, new_start_week_offset), target_total_weeks)
        new_end_week_offset = min(max(0, new_end_week_offset), target_total_weeks)
        if new_end_week_offset <= new_start_week_offset:
            if new_start_week_offset >= target_total_weeks:
                new_start_week_offset = max(0, target_total_weeks - 1)
                new_end_week_offset = target_total_weeks
            else:
                new_end_week_offset = min(target_total_weeks, new_start_week_offset + 1)

        new_start = min_start + timedelta(days=new_start_week_offset * 7)
        new_end = min_start + timedelta(days=new_end_week_offset * 7)
        new_duration_days = max(7, (new_end - new_start).days)
        new_duration_weeks = max(1, new_duration_days // 7)

        aligned_task = dict(task)
        aligned_task.update(
            {
                "start": new_start,
                "end": new_end,
                "duration_days": new_duration_days,
                "duration_weeks": new_duration_weeks,
                "duration": new_duration_weeks,
            }
        )
        if "start_week" in aligned_task:
            aligned_task["start_week"] = max(1, new_start_week_offset + 1)
        if "end_week" in aligned_task:
            aligned_task["end_week"] = max(aligned_task.get("start_week", 1), new_end_week_offset)
        aligned_tasks.append(aligned_task)

    return aligned_tasks


def schedule_tasks(tasks, dependencies=None, target_total_days=None):

    start_date = date.today()
    deps = dependencies or []
    if deps:
        scheduled = _dependency_schedule(tasks, deps, start_date)
    else:
        scheduled = _sequential_schedule(tasks, start_date)
    return _align_schedule_to_total_days(scheduled, target_total_days)