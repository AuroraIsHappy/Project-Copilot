
from planner.agents import plan_builder_agent
from planner.agents import review_refine_agent
from planner.agents import strategy_agent
from planner.agents.agent_logger import log_agent_step
from planning.task_graph import build_task_graph


def _emit_progress(progress_callback, **payload) -> None:
    if not callable(progress_callback):
        return
    progress_callback(payload)


def _normalize_revised_tasks(tasks: list[dict], fallback_tasks: list[dict]) -> list[dict]:
    if not tasks:
        return fallback_tasks

    normalized: list[dict] = []
    for index, task in enumerate(tasks, start=1):
        if not isinstance(task, dict):
            continue

        task_id = str(task.get("task_id") or f"T{index}").strip() or f"T{index}"
        task_name = str(task.get("task_name") or task.get("task") or task.get("name") or "").strip()
        if not task_name:
            continue

        raw_duration = task.get("duration_weeks")
        if raw_duration is None:
            raw_duration = task.get("duration_week")
        if raw_duration is None:
            raw_duration = task.get("duration_days")
            try:
                raw_duration = max(1, (int(raw_duration) + 6) // 7)
            except (TypeError, ValueError):
                raw_duration = None
        if raw_duration is None:
            raw_duration = task.get("duration")
        try:
            duration_weeks = max(1, int(raw_duration))
        except (TypeError, ValueError):
            duration_weeks = 2

        normalized.append(
            {
                "task_id": task_id,
                "task_name": task_name,
                "duration_weeks": duration_weeks,
                "duration": duration_weeks,
                "duration_days": duration_weeks * 7,
            }
        )

    return normalized or fallback_tasks


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


def _normalize_revised_dependencies(dependencies: list[dict], valid_task_ids: set[str]) -> list[dict]:
    if not dependencies:
        return []

    normalized: list[dict] = []
    seen: set[tuple] = set()
    for item in dependencies:
        if not isinstance(item, dict):
            continue

        from_id = str(item.get("from") or item.get("source") or "").strip()
        to_id = str(item.get("to") or item.get("target") or "").strip()
        if not from_id or not to_id or from_id == to_id:
            continue
        if from_id not in valid_task_ids or to_id not in valid_task_ids:
            continue

        dep_type = str(item.get("type") or "FS").upper().strip()
        if dep_type not in {"FS", "SS", "FF", "SF"}:
            dep_type = "FS"

        raw_lag_weeks = item.get("lag_weeks")
        if raw_lag_weeks is None:
            raw_lag_weeks = item.get("lag")
        if raw_lag_weeks is None:
            raw_lag_weeks = _days_to_weeks(item.get("lag_days", 0), allow_negative=True)
        try:
            lag_weeks = int(raw_lag_weeks)
        except (TypeError, ValueError):
            lag_weeks = 0

        raw_overlap_weeks = item.get("overlap_weeks")
        if raw_overlap_weeks is None:
            raw_overlap_weeks = item.get("overlap")
        if raw_overlap_weeks is None:
            raw_overlap_weeks = _days_to_weeks(item.get("overlap_days", 0), allow_negative=False)
        try:
            overlap_weeks = int(raw_overlap_weeks)
        except (TypeError, ValueError):
            overlap_weeks = 0
        overlap_weeks = max(0, overlap_weeks)

        dep = {
            "from": from_id,
            "to": to_id,
            "type": dep_type,
            "lag_weeks": lag_weeks,
            "overlap_weeks": overlap_weeks,
        }
        key = (from_id, to_id, dep_type, lag_weeks, overlap_weeks)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(dep)

    return normalized


def multi_agent_plan(okr: str, progress_callback=None) -> dict:
    total_steps = 3
    log_agent_step("pipeline", "start", {"okr": okr})

    _emit_progress(
        progress_callback,
        stage="strategy_agent",
        label="Strategy Agent",
        phase="working",
        completed_steps=0,
        total_steps=total_steps,
        message="strategy agent working...",
    )
    strategies = strategy_agent.run(okr)
    log_agent_step("strategy_agent", "output", strategies)
    _emit_progress(
        progress_callback,
        stage="strategy_agent",
        label="Strategy Agent",
        phase="done",
        completed_steps=1,
        total_steps=total_steps,
        message="strategy agent completed.",
    )

    _emit_progress(
        progress_callback,
        stage="plan_builder_agent",
        label="Plan-Builder Agent",
        phase="working",
        completed_steps=1,
        total_steps=total_steps,
        message="plan-builder agent working...",
    )
    plan_payload = plan_builder_agent.run(strategies)
    if not isinstance(plan_payload, dict):
        plan_payload = {}

    tasks = _normalize_revised_tasks(plan_payload.get("tasks", []), [])
    dependencies = _normalize_revised_dependencies(
        plan_payload.get("dependencies", []),
        {t["task_id"] for t in tasks},
    )
    log_agent_step("plan_builder_agent", "output", {"tasks": tasks, "dependencies": dependencies})
    _emit_progress(
        progress_callback,
        stage="plan_builder_agent",
        label="Plan-Builder Agent",
        phase="done",
        completed_steps=2,
        total_steps=total_steps,
        message="plan-builder agent completed.",
    )

    _emit_progress(
        progress_callback,
        stage="review_refine_agent",
        label="Review & Refine Agent",
        phase="working",
        completed_steps=2,
        total_steps=total_steps,
        message="review & refine agent working...",
    )
    review_payload = review_refine_agent.run(tasks, dependencies)
    if not isinstance(review_payload, dict):
        review_payload = {}
    log_agent_step("review_refine_agent", "output", review_payload)

    raw_issues = review_payload.get("issues", [])
    raw_suggestions = review_payload.get("suggestions", [])
    issues = [str(item).strip() for item in raw_issues if str(item).strip()] if isinstance(raw_issues, list) else []
    suggestions = [str(item).strip() for item in raw_suggestions if str(item).strip()] if isinstance(raw_suggestions, list) else []
    critique = {
        "issues": issues,
        "suggestions": suggestions,
    }

    revised_tasks = _normalize_revised_tasks(review_payload.get("tasks", []), tasks)
    revised_dependencies_raw = review_payload.get("dependencies", [])
    if revised_dependencies_raw:
        revised_dependencies = _normalize_revised_dependencies(
            revised_dependencies_raw,
            {t["task_id"] for t in revised_tasks},
        )
    else:
        revised_dependencies = dependencies

    _emit_progress(
        progress_callback,
        stage="review_refine_agent",
        label="Review & Refine Agent",
        phase="done",
        completed_steps=3,
        total_steps=total_steps,
        message="review & refine agent completed.",
    )

    final_graph = build_task_graph(revised_tasks, revised_dependencies)
    result = {
        "tasks": revised_tasks,
        "dependencies": revised_dependencies,
        "graph": final_graph,
        "critique": critique,
    }
    log_agent_step("pipeline", "end", result)
    _emit_progress(
        progress_callback,
        stage="pipeline",
        label="Pipeline",
        phase="done",
        completed_steps=3,
        total_steps=total_steps,
        message="multi-agent planning completed.",
    )
    return result