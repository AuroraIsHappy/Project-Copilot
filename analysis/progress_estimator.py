from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
import json
import time

from utils.llm_client import call_llm_messages
from utils.llm_client import load_llm_config
from utils.json_utils import extract_json


_MAX_PROGRESS_WORKERS = 3
_PROGRESS_LLM_MAX_OUTPUT_TOKENS = 5000
_PROGRESS_LLM_MAX_OUTPUT_TOKENS_HARD_CAP = 8000
_MAX_TASK_TITLE_CHARS = 120
_MAX_TASK_OWNER_CHARS = 40
_MAX_SUMMARY_PROMPT_CHARS = 4000
_MAX_SUMMARY_PROMPT_LINES = 120
_PROGRESS_TIMING_LOG_FILE = Path(__file__).resolve().parents[1] / "data" / "debug" / "progress_timing_log.txt"


def _append_timing_log(event: str, payload: dict) -> None:
  record = {
    "timestamp": datetime.now().isoformat(timespec="seconds"),
    "event": event,
    "payload": payload,
  }
  try:
    _PROGRESS_TIMING_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with _PROGRESS_TIMING_LOG_FILE.open("a", encoding="utf-8") as f:
      f.write(json.dumps(record, ensure_ascii=False) + "\n")
  except OSError:
    # Logging should never interrupt main progress estimation flow.
    return


def _to_float(value, default: float = 0.0) -> float:
  try:
    return float(value)
  except (TypeError, ValueError):
    return default


def _normalize_single_update(update: dict) -> dict | None:
  if not isinstance(update, dict):
    return None

  task_id = str(update.get("task_id") or "").strip()
  if not task_id:
    return None

  progress_delta = _to_float(update.get("progress_delta"), 0.0)
  progress_delta = max(-0.30, min(1.00, progress_delta))

  confidence = _to_float(update.get("confidence"), 0.7)
  confidence = max(0.0, min(1.0, confidence))

  evidence = str(update.get("evidence") or "").strip()
  reasoning = str(update.get("reasoning") or "").strip()

  return {
    "task_id": task_id,
    "progress_delta": progress_delta,
    "confidence": confidence,
    "evidence": evidence,
    "reasoning": reasoning,
  }


def _ensure_per_summary_updates(summary_updates) -> list[list[dict]]:
  """
  Normalize inputs into: [summary1_updates, summary2_updates, ...]

  Supported input shapes:
  - list[dict]                        (single summary)
  - list[list[dict]]                  (multiple summaries)
  - tuple/list mixed                  (will be normalized)
  """
  if summary_updates is None:
    return []

  if isinstance(summary_updates, dict):
    normalized = _normalize_single_update(summary_updates)
    return [[normalized]] if normalized else []

  if not isinstance(summary_updates, list):
    return []

  if not summary_updates:
    return []

  first = summary_updates[0]
  if isinstance(first, dict):
    one_summary = []
    for item in summary_updates:
      normalized = _normalize_single_update(item)
      if normalized:
        one_summary.append(normalized)
    return [one_summary] if one_summary else []

  per_summary: list[list[dict]] = []
  for bucket in summary_updates:
    if isinstance(bucket, dict):
      normalized = _normalize_single_update(bucket)
      if normalized:
        per_summary.append([normalized])
      continue
    if not isinstance(bucket, list):
      continue
    one_summary = []
    for item in bucket:
      normalized = _normalize_single_update(item)
      if normalized:
        one_summary.append(normalized)
    if one_summary:
      per_summary.append(one_summary)

  return per_summary


def _clip_text(value: str, max_chars: int) -> str:
  text = str(value or "").strip()
  if max_chars <= 0:
    return ""
  if len(text) <= max_chars:
    return text

  marker = "\n...[truncated]...\n"
  if max_chars <= len(marker):
    return text[:max_chars]

  head_chars = int(max_chars * 0.7)
  tail_chars = max_chars - head_chars - len(marker)
  if tail_chars <= 0:
    return text[:max_chars]

  return text[:head_chars].rstrip() + marker + text[-tail_chars:].lstrip()


def _compact_tasks_for_prompt(tasks) -> list[dict]:
  if not isinstance(tasks, list):
    return []

  compact_tasks: list[dict] = []
  for task in tasks:
    if not isinstance(task, dict):
      continue

    task_id = str(task.get("task_id") or "").strip()
    task_name = _clip_text(str(task.get("task") or ""), _MAX_TASK_TITLE_CHARS)
    if not task_id or not task_name:
      continue

    compact = {
      "task_id": task_id,
      "task": task_name,
    }

    owner = _clip_text(str(task.get("owner") or ""), _MAX_TASK_OWNER_CHARS)
    if owner:
      compact["owner"] = owner

    compact_tasks.append(compact)

  return compact_tasks


def _prepare_tasks_prompt(tasks) -> tuple[str, int]:
  compact_tasks = _compact_tasks_for_prompt(tasks)
  return json.dumps(compact_tasks, ensure_ascii=False, separators=(",", ":")), len(compact_tasks)


def _prepare_summary_prompt(summary: str) -> str:
  raw = str(summary or "")
  cleaned_lines = [line.strip() for line in raw.splitlines() if line.strip()]

  if len(cleaned_lines) > _MAX_SUMMARY_PROMPT_LINES:
    head_count = _MAX_SUMMARY_PROMPT_LINES // 2
    tail_count = _MAX_SUMMARY_PROMPT_LINES - head_count
    cleaned_lines = [
      *cleaned_lines[:head_count],
      "...[truncated lines]...",
      *cleaned_lines[-tail_count:],
    ]

  merged = "\n".join(cleaned_lines)
  return _clip_text(merged, _MAX_SUMMARY_PROMPT_CHARS)


def build_progress_prompt(tasks_json: str, summary: str):

    prompt = f"""
You are a project progress delta extractor.

Task list (JSON array):
{tasks_json}

Work summary:
{summary}

Goal:
- Infer which existing tasks were affected by this summary.
- Output progress_delta for this summary only (not absolute progress).

Rules:
1. Only use task_id values from task list.
2. Only output clearly mentioned or strongly implied tasks.
3. Do not invent tasks and do not update unrelated tasks.
4. Typical affected tasks per summary: 1-3.
5. progress_delta range: -0.30 to 1.00, step 0.05.
6. confidence range: 0 to 1. Skip uncertain items (confidence < 0.40).
7. evidence must be a short supporting snippet from summary.
8. Match task-list language.

Return ONLY valid JSON array, no markdown and no extra text:
[
  {{
    "task_id": "T2",
    "progress_delta": 0.10,
    "confidence": 0.82,
    "evidence": "..."
  }}
]
"""

    return prompt


def extract_progress(
  summary: str,
  tasks_json: str,
  max_output_tokens: int,
  progress_model: str | None = None,
):
    prepared_summary = _prepare_summary_prompt(summary)
    if not prepared_summary:
      return []

    prompt = build_progress_prompt(tasks_json, prepared_summary)

    response = call_llm_messages(
      [{"role": "user", "content": prompt}],
      inject_system_memory=True,
      max_tokens_override=max_output_tokens,
      model_override=progress_model,
    )

    updates = parse_json(response)

    return updates


def aggregate_updates(summary_updates, summary_names: list[str] | None = None):
    """
    Aggregate updates from multiple summaries.

    Key rule:
    If one task is mentioned in multiple summaries, its final delta is
    the arithmetic mean of per-summary deltas.

    Example:
    summary A -> T1 +0.30
    summary B -> T1 +0.20
    final T1 delta -> +0.25
    """
    per_summary = _ensure_per_summary_updates(summary_updates)
    if not per_summary:
      return []

    task_stats: dict[str, dict] = {}

    for summary_idx, updates_in_one_summary in enumerate(per_summary):
      summary_name = ""
      if isinstance(summary_names, list) and summary_idx < len(summary_names):
        summary_name = str(summary_names[summary_idx] or "").strip()

      # Prevent duplicate counting if model emits same task multiple times in one summary.
      one_task_delta: dict[str, list[float]] = {}
      one_task_conf: dict[str, list[float]] = {}
      one_task_evidence: dict[str, list[str]] = {}
      one_task_reasoning: dict[str, list[str]] = {}
      one_task_files: dict[str, set[str]] = {}

      for update in updates_in_one_summary:
        task_id = update["task_id"]
        one_task_delta.setdefault(task_id, []).append(update["progress_delta"])
        one_task_conf.setdefault(task_id, []).append(update["confidence"])

        evidence = update.get("evidence", "")
        if evidence:
          one_task_evidence.setdefault(task_id, []).append(evidence)

        reasoning = update.get("reasoning", "")
        if reasoning:
          one_task_reasoning.setdefault(task_id, []).append(reasoning)

        if summary_name:
          one_task_files.setdefault(task_id, set()).add(summary_name)

      for task_id, delta_list in one_task_delta.items():
        per_summary_avg_delta = sum(delta_list) / len(delta_list)
        per_summary_avg_conf = sum(one_task_conf.get(task_id, [0.7])) / max(len(one_task_conf.get(task_id, [])), 1)

        if task_id not in task_stats:
          task_stats[task_id] = {
            "delta_sum": 0.0,
            "confidence_sum": 0.0,
            "summary_count": 0,
            "evidence": [],
            "reasoning": [],
            "evidence_files": [],
          }

        task_stats[task_id]["delta_sum"] += per_summary_avg_delta
        task_stats[task_id]["confidence_sum"] += per_summary_avg_conf
        task_stats[task_id]["summary_count"] += 1
        task_stats[task_id]["evidence"].extend(one_task_evidence.get(task_id, []))
        task_stats[task_id]["reasoning"].extend(one_task_reasoning.get(task_id, []))
        task_stats[task_id]["evidence_files"].extend(sorted(one_task_files.get(task_id, set())))

    aggregated = []
    for task_id, stats in task_stats.items():
      count = max(1, stats["summary_count"])
      avg_delta = stats["delta_sum"] / count
      avg_confidence = stats["confidence_sum"] / count

      unique_evidence = []
      for item in stats["evidence"]:
        if item not in unique_evidence:
          unique_evidence.append(item)

      unique_reasoning = []
      for item in stats["reasoning"]:
        if item not in unique_reasoning:
          unique_reasoning.append(item)

      unique_files = []
      for item in stats["evidence_files"]:
        if item not in unique_files:
          unique_files.append(item)

      aggregated.append(
        {
          "task_id": task_id,
          "progress_delta": round(avg_delta, 4),
          "confidence": round(avg_confidence, 4),
          "evidence": " | ".join(unique_evidence[:3]),
          "evidence_files": " | ".join(unique_files[:5]),
          "reasoning": " | ".join(unique_reasoning[:2]),
          "summary_count": stats["summary_count"],
        }
      )

    aggregated.sort(key=lambda item: item["task_id"])
    return aggregated


def apply_progress_updates(tasks, aggregated_updates):
    """
    Apply aggregated deltas to task progress.

    Persist progress as integer percentage (0-100), which is the canonical
    storage format used by state/state_manager.py.
    """
    if not isinstance(tasks, list):
        return []

    updates_by_task = {
        str(item.get("task_id")): item
        for item in (aggregated_updates or [])
        if isinstance(item, dict) and str(item.get("task_id") or "")
    }

    updated_tasks: list[dict] = []
    for task in tasks:
        task_copy = dict(task)
        task_id = str(task_copy.get("task_id") or "")
        update = updates_by_task.get(task_id)
        if not update:
            updated_tasks.append(task_copy)
            continue

        raw_progress = _to_float(task_copy.get("progress"), 0.0)
        # Backward-compatible read: if caller passes ratio in (0,1], convert to percent first.
        if 0 < raw_progress <= 1:
            current_percent = raw_progress * 100.0
        else:
            current_percent = raw_progress
        current_percent = max(0.0, min(100.0, current_percent))

        delta = _to_float(update.get("progress_delta"), 0.0)
        delta_percent = delta * 100.0
        new_percent = max(0.0, min(100.0, current_percent + delta_percent))

        task_copy["progress"] = int(round(new_percent))
        task_copy["progress_delta_applied"] = round(delta, 4)
        task_copy["progress_update_confidence"] = _to_float(update.get("confidence"), 0.0)

        evidence = str(update.get("evidence") or "").strip()
        if evidence:
            task_copy["progress_update_evidence"] = evidence

        updated_tasks.append(task_copy)

    return updated_tasks


def parse_json(response):
    """
    Parse LLM response and return normalized update list.
    """
    parsed = extract_json(response)

    if isinstance(parsed, dict):
      parsed = parsed.get("updates", [])

    if not isinstance(parsed, list):
      return []

    normalized_updates = []
    for item in parsed:
        normalized = _normalize_single_update(item)
        if normalized:
            normalized_updates.append(normalized)

    return normalized_updates


def _resolve_progress_workers() -> int:
  try:
    cfg = load_llm_config()
  except Exception:
    return _MAX_PROGRESS_WORKERS

  raw_value = cfg.get("progress_max_workers", _MAX_PROGRESS_WORKERS)
  try:
    workers = int(raw_value)
  except (TypeError, ValueError):
    return _MAX_PROGRESS_WORKERS

  return max(1, min(16, workers))


def _resolve_progress_llm_max_output_tokens() -> int:
  try:
    cfg = load_llm_config()
  except Exception:
    return _PROGRESS_LLM_MAX_OUTPUT_TOKENS

  raw_value = cfg.get("progress_llm_max_output_tokens", _PROGRESS_LLM_MAX_OUTPUT_TOKENS)
  try:
    max_tokens = int(raw_value)
  except (TypeError, ValueError):
    return _PROGRESS_LLM_MAX_OUTPUT_TOKENS

  return max(100, min(_PROGRESS_LLM_MAX_OUTPUT_TOKENS_HARD_CAP, max_tokens))


def _resolve_progress_llm_model() -> str:
  try:
    cfg = load_llm_config()
  except Exception:
    return ""

  progress_model = str(cfg.get("progress_model") or "").strip()
  if progress_model:
    return progress_model
  return str(cfg.get("model") or "").strip()


def _extract_progress_parallel(
  tasks_json: str,
  summaries: list[str],
  progress_llm_max_output_tokens: int,
  progress_model: str,
) -> list[list[dict]]:
  if not summaries:
    return []

  if len(summaries) == 1:
    single_started = time.perf_counter()
    updates = extract_progress(summaries[0], tasks_json, progress_llm_max_output_tokens, progress_model)
    _append_timing_log(
      "summary_done",
      {
        "summary_index": 0,
        "duration_ms": round((time.perf_counter() - single_started) * 1000, 2),
        "updates_count": len(updates),
        "workers": 1,
      },
    )
    _append_timing_log(
      "batch_done",
      {
        "summaries_count": 1,
        "workers": 1,
        "duration_ms": round((time.perf_counter() - single_started) * 1000, 2),
        "total_updates": len(updates),
      },
    )
    return [updates]

  max_workers = _resolve_progress_workers()
  worker_count = min(len(summaries), max_workers)
  ordered_updates: list[list[dict]] = [[] for _ in summaries]
  batch_started = time.perf_counter()
  _append_timing_log(
    "batch_start",
    {
      "summaries_count": len(summaries),
      "workers": worker_count,
    },
  )

  with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="progress-estimator") as pool:
    future_to_meta = {
      pool.submit(extract_progress, summary, tasks_json, progress_llm_max_output_tokens, progress_model): (idx, time.perf_counter())
      for idx, summary in enumerate(summaries)
    }

    for future in as_completed(future_to_meta):
      idx, started_at = future_to_meta[future]
      try:
        updates = future.result()
        ordered_updates[idx] = updates
        _append_timing_log(
          "summary_done",
          {
            "summary_index": idx,
            "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "updates_count": len(updates),
            "workers": worker_count,
          },
        )
      except Exception as exc:
        _append_timing_log(
          "summary_error",
          {
            "summary_index": idx,
            "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "error": str(exc),
            "workers": worker_count,
          },
        )
        raise RuntimeError(f"Failed to estimate progress for summary index {idx}") from exc

  _append_timing_log(
    "batch_done",
    {
      "summaries_count": len(summaries),
      "workers": worker_count,
      "duration_ms": round((time.perf_counter() - batch_started) * 1000, 2),
      "total_updates": sum(len(item) for item in ordered_updates),
    },
  )

  return ordered_updates


def estimate_progress_from_summaries(tasks, summaries, summary_names: list[str] | None = None):
    """
    End-to-end helper:
    1. Extract per-summary updates
    2. Aggregate by task with per-summary average delta
    3. Apply deltas to task list
    """
    estimate_started = time.perf_counter()

    if not summaries:
      _append_timing_log(
        "estimate_done",
        {
          "summaries_count": 0,
          "duration_ms": round((time.perf_counter() - estimate_started) * 1000, 2),
          "aggregated_updates": 0,
          "non_empty_summary_updates": 0,
        },
      )
      return {
        "per_summary_updates": [],
        "aggregated_updates": [],
        "updated_tasks": list(tasks or []),
      }

    tasks_json, prompt_tasks_count = _prepare_tasks_prompt(tasks)
    progress_llm_max_output_tokens = _resolve_progress_llm_max_output_tokens()
    progress_model = _resolve_progress_llm_model()
    per_summary_updates = _extract_progress_parallel(
      tasks_json,
      summaries,
      progress_llm_max_output_tokens,
      progress_model,
    )

    aggregated = aggregate_updates(per_summary_updates, summary_names=summary_names)
    updated_tasks = apply_progress_updates(tasks, aggregated)

    _append_timing_log(
      "estimate_done",
      {
        "summaries_count": len(summaries),
        "duration_ms": round((time.perf_counter() - estimate_started) * 1000, 2),
        "prompt_tasks_count": prompt_tasks_count,
        "prompt_tasks_chars": len(tasks_json),
        "progress_model": progress_model,
        "progress_llm_max_output_tokens": progress_llm_max_output_tokens,
        "aggregated_updates": len(aggregated),
        "non_empty_summary_updates": sum(1 for updates in per_summary_updates if updates),
      },
    )

    return {
      "per_summary_updates": per_summary_updates,
      "aggregated_updates": aggregated,
      "updated_tasks": updated_tasks,
    }