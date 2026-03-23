from datetime import datetime
from pathlib import Path

from utils.llm_client import call_llm
from utils.json_utils import extract_json


DEBUG_LOG_FILE = Path(__file__).resolve().parents[1] / "data" / "debug" / "llm_debug_log.txt"
MAX_PARSE_RETRIES = 1


def _append_debug_log(
    okr_text: str,
    prompt: str,
    response: str,
    tasks: list[dict],
    dependencies: list[dict],
    error: str = "",
) -> None:
    DEBUG_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log_lines = [
        "=" * 88,
        f"timestamp: {stamp}",
        f"tasks_count: {len(tasks)}",
        f"dependencies_count: {len(dependencies)}",
    ]
    if error:
        log_lines.append(f"error: {error}")

    log_lines.extend(
        [
            "[okr_text]",
            okr_text,
            "[prompt]",
            prompt,
            "[llm_raw_response]",
            response,
            "[parsed_tasks]",
            str(tasks),
            "[parsed_dependencies]",
            str(dependencies),
            "",
        ]
    )

    with DEBUG_LOG_FILE.open("a", encoding="utf-8") as f:
        f.write("\n".join(log_lines))


def _is_valid_task_item(item: dict) -> bool:
    if not isinstance(item, dict):
        return False

    # Accept a few common key aliases from different models.
    task_name = item.get("task_name") or item.get("task") or item.get("name")
    duration = item.get("duration_weeks")
    if duration is None:
        duration = item.get("duration_days")
    if duration is None:
        duration = item.get("duration")

    if not str(task_name or "").strip():
        return False

    try:
        int(duration if duration is not None else 3)
    except (TypeError, ValueError):
        return False

    return True


def _normalize_task(task: dict, index: int) -> dict | None:
    if not _is_valid_task_item(task):
        return None

    task_name = str(task.get("task_name") or task.get("task") or task.get("name") or "").strip()
    if not task_name:
        return None

    task_id = str(task.get("task_id") or f"T{index}").strip() or f"T{index}"

    duration_weeks = task.get("duration_weeks")
    if duration_weeks is None:
        duration_weeks = task.get("duration_days")
        try:
            duration_weeks = max(1, (int(duration_weeks) + 6) // 7)
        except (TypeError, ValueError):
            duration_weeks = None
    if duration_weeks is None:
        duration_weeks = task.get("duration", 1)

    try:
        duration = max(1, int(duration_weeks))
    except (TypeError, ValueError):
        duration = 1

    return {
        "task_id": task_id,
        "task": task_name,
        "duration": duration,
        "duration_weeks": duration,
        "duration_days": duration * 7,
    }


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


def _normalize_dependencies(raw_dependencies: list[dict], valid_task_ids: set[str]) -> list[dict]:
    normalized: list[dict] = []
    seen: set[tuple] = set()

    for dep in raw_dependencies:
        if not isinstance(dep, dict):
            continue

        source = str(dep.get("from") or dep.get("source") or "").strip()
        target = str(dep.get("to") or dep.get("target") or "").strip()
        if not source or not target or source == target:
            continue
        if source not in valid_task_ids or target not in valid_task_ids:
            continue

        dep_type = str(dep.get("type") or "FS").upper().strip()
        if dep_type not in {"FS", "SS", "FF", "SF"}:
            dep_type = "FS"

        raw_lag_weeks = dep.get("lag_weeks")
        if raw_lag_weeks is None:
            raw_lag_weeks = dep.get("lag")
        if raw_lag_weeks is None:
            raw_lag_weeks = _days_to_weeks(dep.get("lag_days", 0), allow_negative=True)
        try:
            lag_weeks = int(raw_lag_weeks)
        except (TypeError, ValueError):
            lag_weeks = 0

        raw_overlap_weeks = dep.get("overlap_weeks")
        if raw_overlap_weeks is None:
            raw_overlap_weeks = dep.get("overlap")
        if raw_overlap_weeks is None:
            raw_overlap_weeks = _days_to_weeks(dep.get("overlap_days", 0), allow_negative=False)
        try:
            overlap_weeks = max(0, int(raw_overlap_weeks))
        except (TypeError, ValueError):
            overlap_weeks = 0

        item = {
            "from": source,
            "to": target,
            "type": dep_type,
            "lag_weeks": lag_weeks,
            "overlap_weeks": overlap_weeks,
        }
        key = (source, target, dep_type, lag_weeks, overlap_weeks)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(item)

    return normalized


def _needs_repair(raw_response: str, tasks: list[dict], dependencies: list[dict]) -> bool:
    if tasks and dependencies is not None:
        return False
    return bool(str(raw_response or "").strip())


def _build_repair_prompt(original_prompt: str, bad_response: str) -> str:
    return f"""
Your previous output could not be parsed into the required JSON schema.

Fix the output and return ONLY valid JSON object.

Schema:
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

Rules:
- No markdown
- No explanations
- No extra text
- At least 5 tasks

Original instruction:
{original_prompt}

Previous invalid output:
{bad_response}
"""

def build_prompt(okr, target_total_weeks: int | None = None):

    horizon_rule = ""
    if target_total_weeks is not None:
        try:
            normalized_weeks = max(1, int(target_total_weeks))
        except (TypeError, ValueError):
            normalized_weeks = None
        if normalized_weeks is not None:
            horizon_rule = (
                f"- The full project timeline from the first task start to the final task end should be close to {normalized_weeks} weeks.\n"
                "- Choose durations and dependency overlap/lag so the overall schedule is realistic within that horizon.\n"
                "- Close enough is sufficient; do not spend effort searching for an exact optimum.\n"
            )

    prompt = f"""
You are a senior technical project planner.

Your task is to convert an OKR into a realistic plan with actionable project tasks and task dependencies.

Use fast, practical judgment and return a good-enough feasible plan in one pass.
Do not search for the perfect schedule.

Use the following internal reasoning process:

1. Understand the Objective.
2. Identify the Key Results.
3. Determine strategies that achieve those Key Results.
4. Convert the strategies into concrete project tasks.

Do NOT output the reasoning steps.

PLANNING GUIDELINES:

- Tasks must be concrete and executable.
- Avoid vague tasks.
- Each task should represent a meaningful unit of work.
- Tasks should follow a logical project workflow.
- Typical stages include:
  research → design → implementation → testing → launch
- Prefer 8-12 tasks.
- Do not exceed 12 tasks unless the OKR clearly requires multiple independent workstreams.
- Each task should take between 1 and 12 weeks.
- Keep each task_name concise and direct.
- Ensure tasks collectively support achieving the Key Results.
- Ensure tasks cover the full project lifecycle.
- Add realistic dependencies.
- Allow overlap when reasonable by using overlap_weeks > 0.
- Keep dependencies sparse and include only sequencing links that are truly needed.
- Prefer the simplest feasible plan over the most optimized plan.
{horizon_rule}

Language rule:
- Determine the dominant language from the full OKR sentence structure, not from isolated technical terms, acronyms, benchmark names, or product names.
- If the OKR is mainly Chinese, or Chinese with some English technical terms or acronyms, output task_name in Chinese.
- Treat terms such as SOTA, Attention, Tool-use, Video-MME, LibriTTS, VCTK and similar technical names as terminology, not as evidence that the OKR language is English.
- Only output all-English task_name values when the OKR is predominantly written in English sentences.
- You may keep well-known English acronyms or benchmark names inside otherwise Chinese task_name values when necessary.

OKR:
{okr}

Return ONLY valid JSON object in this format:

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

STRICT OUTPUT RULES:

- Output ONLY JSON.
- No explanations.
- No markdown.
- No text outside the JSON.
- No reasoning text.
- Keep the JSON compact and do not add extra fields.
"""
    return prompt



def parse_plan(response: str) -> dict:
    json_data = extract_json(response)

    raw_tasks: list[dict] = []
    raw_dependencies: list[dict] = []

    if isinstance(json_data, dict):
        maybe_tasks = json_data.get("tasks", [])
        maybe_dependencies = json_data.get("dependencies", [])
        if isinstance(maybe_tasks, list):
            raw_tasks = maybe_tasks
        if isinstance(maybe_dependencies, list):
            raw_dependencies = maybe_dependencies
    elif isinstance(json_data, list):
        # Backward compatibility: old schema returns only task list.
        raw_tasks = json_data

    tasks: list[dict] = []
    for index, item in enumerate(raw_tasks, start=1):
        if not isinstance(item, dict):
            continue
        normalized = _normalize_task(item, index)
        if normalized is None:
            continue
        tasks.append(normalized)

    # Ensure task IDs are unique and deterministic.
    used_ids: set[str] = set()
    for index, task in enumerate(tasks, start=1):
        tid = task.get("task_id", "")
        if not tid or tid in used_ids:
            tid = f"T{index}"
            task["task_id"] = tid
        used_ids.add(tid)

    dependencies = _normalize_dependencies(raw_dependencies, used_ids)

    return {"tasks": tasks, "dependencies": dependencies}


def parse_tasks(response):
    return parse_plan(response).get("tasks", [])


def generate_tasks_with_dependencies(okr_text: str, target_total_weeks: int | None = None) -> dict:

    prompt = build_prompt(okr_text, target_total_weeks=target_total_weeks)

    response = ""
    tasks: list[dict] = []
    dependencies: list[dict] = []
    used_prompt = prompt

    try:
        for retry_idx in range(MAX_PARSE_RETRIES + 1):
            response = call_llm(used_prompt)
            parsed = parse_plan(response)
            tasks = parsed.get("tasks", [])
            dependencies = parsed.get("dependencies", [])

            if not _needs_repair(response, tasks, dependencies):
                break

            if retry_idx < MAX_PARSE_RETRIES:
                used_prompt = _build_repair_prompt(prompt, response)

        if not tasks:
            raise ValueError("LLM returned an invalid task plan. Please check the debug log.")

        _append_debug_log(okr_text, used_prompt, response, tasks, dependencies)
    except Exception as exc:
        _append_debug_log(okr_text, used_prompt, response, tasks, dependencies, error=str(exc))
        raise

    return {"tasks": tasks, "dependencies": dependencies}


def generate_tasks(okr_text, target_total_weeks: int | None = None):
    return generate_tasks_with_dependencies(okr_text, target_total_weeks=target_total_weeks).get("tasks", [])