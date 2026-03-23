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


def _parse_text_list(raw_value) -> list[str]:
    if not isinstance(raw_value, list):
        return []
    normalized: list[str] = []
    for item in raw_value:
        text = str(item).strip()
        if text:
            normalized.append(text)
    return normalized


def _parse(response: str) -> dict:
    payload = extract_json(response)
    if not isinstance(payload, dict):
        return {
            "issues": [],
            "suggestions": [],
            "tasks": [],
            "dependencies": [],
        }

    raw_issues = payload.get("issues", [])
    raw_suggestions = payload.get("suggestions", [])

    if (not isinstance(raw_issues, list) or not isinstance(raw_suggestions, list)) and isinstance(payload.get("critique"), dict):
        critique = payload.get("critique", {})
        if not isinstance(raw_issues, list):
            raw_issues = critique.get("issues", [])
        if not isinstance(raw_suggestions, list):
            raw_suggestions = critique.get("suggestions", [])

    tasks_payload = payload.get("tasks", [])
    dependencies_payload = payload.get("dependencies", [])
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
        "issues": _parse_text_list(raw_issues),
        "suggestions": _parse_text_list(raw_suggestions),
        "tasks": tasks,
        "dependencies": unique_dependencies,
    }


def review_refine_prompt(tasks: list[dict], dependencies: list[dict], target_total_weeks: int | None = None) -> str:
    horizon_rule = ""
    if target_total_weeks is not None:
        try:
            normalized_weeks = max(1, int(target_total_weeks))
        except (TypeError, ValueError):
            normalized_weeks = None
        if normalized_weeks is not None:
            horizon_rule = (
                f"- The revised plan should keep the total project timeline close to {normalized_weeks} weeks.\n"
                "- If the current plan violates this horizon, adjust durations and dependency overlap/lag to move it closer.\n"
            )

    prompt = f"""
You are a senior project planning reviewer and reviser.

You will receive a project plan with tasks and dependencies.
In one pass, you must:
1) critique the plan
2) suggest improvements
3) return an improved plan

Do reasoning internally and output only JSON.

CRITIQUE RULES

- Identify missing phases, risky sequencing, and unrealistic flow.
- Keep issues concise and specific.
- Keep suggestions actionable.
- Only list issues that you will actually fix in this same output.

- Improve the plan without excessive rewriting.
- Keep task IDs stable whenever possible.
- Preserve reasonable task names and intent.
- Keep dependency logic valid and acyclic.
- Use integer week-based durations and constraints only.

ISSUE-COVERAGE RULES (MANDATORY)

- Treat the issues list as a required checklist for revision.
- Every issue must be resolved by at least one concrete change in output tasks and/or dependencies.

DEPENDENCY RULES

- Allowed dependency types: FS, SS, FF, SF.
- overlap_weeks must be >= 0.
- lag_weeks can be negative, zero, or positive.
- Use only task IDs that exist in output tasks.
{horizon_rule}

LANGUAGE RULE

- Detect language from input task_name values.
- If input tasks are Chinese, output task_name/issues/suggestions in Chinese.
- If input tasks are English, output task_name/issues/suggestions in English.
- Do not mix Chinese and English.

Tasks:
{tasks}

Dependencies:
{dependencies}

Return ONLY JSON object in this exact schema:

{{
  "issues": [
    "issue description"
  ],
  "suggestions": [
    "suggestion description"
  ],
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
"""
    return prompt


def run(tasks: list[dict], dependencies: list[dict], target_total_weeks: int | None = None) -> dict:
    prompt = review_refine_prompt(tasks, dependencies, target_total_weeks=target_total_weeks)
    response = call_llm(
        prompt,
        trace_label="review_refine_agent",
    )
    return _parse(response)


def review_refine_agent(tasks, dependencies, target_total_weeks: int | None = None):
    return run(tasks, dependencies, target_total_weeks=target_total_weeks)
