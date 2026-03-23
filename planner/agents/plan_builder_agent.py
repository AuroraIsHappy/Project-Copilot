from utils.llm_client import call_llm
from utils.json_utils import extract_json

_ALLOWED_DEP_TYPES = {"FS", "SS", "FF", "SF"}


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


def _normalize_task(item: dict, index: int) -> dict | None:
    task_id = str(item.get("task_id") or f"T{index}").strip() or f"T{index}"
    task_name = str(item.get("task_name") or item.get("task") or item.get("name") or "").strip()
    if not task_name:
        return None

    raw_duration = item.get("duration_weeks")
    if raw_duration is None:
        raw_duration = item.get("duration_week")
    if raw_duration is None:
        raw_duration = item.get("estimated_duration_weeks")
    if raw_duration is None:
        raw_duration = item.get("duration")
    if raw_duration is None:
        raw_duration = _days_to_weeks(item.get("duration_days"), allow_negative=False)

    try:
        duration_weeks = int(raw_duration)
    except (TypeError, ValueError):
        duration_weeks = 2
    duration_weeks = max(1, min(12, duration_weeks))

    return {
        "task_id": task_id,
        "task_name": task_name,
        "duration_weeks": duration_weeks,
    }


def _normalize_dependency(item: dict, valid_task_ids: set[str]) -> dict | None:
    from_id = str(item.get("from") or item.get("source") or "").strip()
    to_id = str(item.get("to") or item.get("target") or "").strip()
    if not from_id or not to_id or from_id == to_id:
        return None
    if from_id not in valid_task_ids or to_id not in valid_task_ids:
        return None

    dep_type = str(item.get("type") or "FS").upper().strip()
    if dep_type not in _ALLOWED_DEP_TYPES:
        dep_type = "FS"

    raw_lag_weeks = item.get("lag_weeks")
    if raw_lag_weeks is None:
        raw_lag_weeks = item.get("lag")
    if raw_lag_weeks is None:
        raw_lag_weeks = _days_to_weeks(item.get("lag_days"), allow_negative=True)

    raw_overlap_weeks = item.get("overlap_weeks")
    if raw_overlap_weeks is None:
        raw_overlap_weeks = item.get("overlap")
    if raw_overlap_weeks is None:
        raw_overlap_weeks = _days_to_weeks(item.get("overlap_days"), allow_negative=False)

    try:
        lag_weeks = int(raw_lag_weeks)
    except (TypeError, ValueError):
        lag_weeks = 0

    try:
        overlap_weeks = int(raw_overlap_weeks)
    except (TypeError, ValueError):
        overlap_weeks = 0
    overlap_weeks = max(0, overlap_weeks)

    return {
        "from": from_id,
        "to": to_id,
        "type": dep_type,
        "lag_weeks": lag_weeks,
        "overlap_weeks": overlap_weeks,
    }


def _parse(response: str) -> dict:
    payload = extract_json(response)

    if isinstance(payload, dict):
        tasks_payload = payload.get("tasks", [])
        dependencies_payload = payload.get("dependencies", [])
    elif isinstance(payload, list):
        tasks_payload = payload
        dependencies_payload = []
    else:
        tasks_payload = []
        dependencies_payload = []

    if not isinstance(tasks_payload, list):
        tasks_payload = []
    if not isinstance(dependencies_payload, list):
        dependencies_payload = []

    tasks: list[dict] = []
    for index, item in enumerate(tasks_payload, start=1):
        if not isinstance(item, dict):
            continue
        normalized = _normalize_task(item, index)
        if normalized:
            tasks.append(normalized)

    used: set[str] = set()
    for index, task in enumerate(tasks, start=1):
        tid = task.get("task_id", "")
        if not tid or tid in used:
            tid = f"T{index}"
            task["task_id"] = tid
        used.add(tid)

    valid_task_ids = {task["task_id"] for task in tasks}
    dependencies: list[dict] = []
    for item in dependencies_payload:
        if not isinstance(item, dict):
            continue
        normalized = _normalize_dependency(item, valid_task_ids)
        if normalized:
            dependencies.append(normalized)

    unique_dependencies: list[dict] = []
    seen: set[tuple] = set()
    for dep in dependencies:
        key = (dep["from"], dep["to"], dep["type"], dep["lag_weeks"], dep["overlap_weeks"])
        if key in seen:
            continue
        seen.add(key)
        unique_dependencies.append(dep)

    return {
        "tasks": tasks,
        "dependencies": unique_dependencies,
    }


def plan_builder_prompt(strategies: list[dict], target_total_weeks: int | None = None) -> str:
    horizon_rule = ""
    if target_total_weeks is not None:
        try:
            normalized_weeks = max(1, int(target_total_weeks))
        except (TypeError, ValueError):
            normalized_weeks = None
        if normalized_weeks is not None:
            horizon_rule = (
                f"- The total project timeline from first task start to final task end should be close to {normalized_weeks} weeks.\n"
                "- Tune durations and dependency overlap/lag to make the initial plan fit that horizon.\n"
                "- Close enough is sufficient; do not search for an exact optimum.\n"
            )

    prompt = f"""
You are an experienced technical project planner.

Your task is to convert high-level strategies into a complete executable project plan in one pass.

Use fast, practical judgment and produce a good-enough feasible plan.
Do not search for the globally optimal schedule.

You must output:
1) a task list
2) a dependency list

Be concise. Do not overthink.
Do reasoning internally and output only JSON.

TASK RULES

- Each task must be concrete and implementable.
- Prefer 8-12 tasks.
- Do not exceed 12 tasks unless the strategies clearly represent multiple independent workstreams.
- Use integer duration_weeks only.
- duration_weeks must be between 1 and 12.
- Keep task IDs stable and unique as T1, T2, ...
- Keep each task_name short and direct.

DEPENDENCY RULES

- Use only FS/SS/FF/SF dependency types.
- Use only task IDs that exist in tasks.
- Avoid redundant links.
- Avoid circular dependencies.
- overlap_weeks must be >= 0.
- lag_weeks can be negative, zero, or positive.
- Keep dependencies sparse and include only the critical sequencing links.
{horizon_rule}

FLOW GUIDANCE

Typical software project sequence often resembles:
research -> design -> implementation -> testing -> release

LANGUAGE RULE

- Determine the dominant language from the full strategy text, not from isolated technical terms, acronyms, benchmark names, or product names.
- If the strategies are mainly Chinese, or Chinese with some English technical terms or acronyms, output task_name in Chinese.
- Treat terms such as SOTA, Attention, Tool-use, Video-MME, LibriTTS, VCTK and similar technical names as terminology, not as evidence that the strategies are English.
- Only output all-English task_name values when the strategies are predominantly written in English sentences.
- You may keep well-known English acronyms or benchmark names inside otherwise Chinese task_name values when necessary.

Strategies:
{strategies}

Return ONLY JSON object in this exact schema:

{{
  "tasks": [
    {{
      "task_id": "T1",
      "task_name": "task description",
      "duration_weeks": 2
    }}
  ],
  "dependencies": [
    {{
      "from": "T1",
      "to": "T2",
      "type": "FS",
      "lag_weeks": 0,
      "overlap_weeks": 0
    }}
  ]
}}

STRICT OUTPUT RULES

- Output ONLY JSON
- No explanations
- No markdown
- No reasoning text
- Keep the JSON compact and do not add extra fields
"""
    return prompt


def run(strategies: list[dict], target_total_weeks: int | None = None) -> dict:
    prompt = plan_builder_prompt(strategies, target_total_weeks=target_total_weeks)
    response = call_llm(
        prompt,
        trace_label="plan_builder_agent",
    )
    return _parse(response)


def plan_builder_agent(strategies, target_total_weeks: int | None = None):
    return run(strategies, target_total_weeks=target_total_weeks)
