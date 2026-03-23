import uuid
from datetime import date
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

from utils.json_utils import write_json, read_json

_STATE_DIR = Path(__file__).resolve().parent
_REGISTRY_FILE = _STATE_DIR / "projects_registry.json"
_PROJECTS_DIR = _STATE_DIR / "projects"
_INSIGHT_URL_HISTORY_FILE = _STATE_DIR / "insight_url_history.json"
_WORKSPACE_INSIGHT_STATE_FILE = _STATE_DIR / "workspace_insight_state.json"
_WORKSPACE_INSIGHT_SETTINGS_FILE = _STATE_DIR / "workspace_insight_settings.json"

# Legacy single-project file (kept for migration)
_LEGACY_STATE_FILE = _STATE_DIR / "project_state.json"
_ALLOWED_GANTT_THEMES = {"default", "minimal", "retro"}
_ALLOWED_INSIGHT_FEEDBACK_ACTIONS = {"useful", "not_relevant", "save", "unsave", "deep_dive"}
_MAX_INSIGHT_URL_HISTORY = 5000
_INSIGHT_SOURCE_KEYS = ("arxiv", "github", "blog", "reddit")


def _normalize_risk_lag_threshold(value, default: int = 30) -> int:
    try:
        return max(0, min(100, int(value)))
    except (TypeError, ValueError):
        return max(0, min(100, int(default)))


def _normalize_gantt_theme(value, default: str = "default") -> str:
    normalized = str(value or "").strip().lower()
    if normalized in _ALLOWED_GANTT_THEMES:
        return normalized

    fallback = str(default or "default").strip().lower()
    if fallback in _ALLOWED_GANTT_THEMES:
        return fallback
    return "default"


def _normalize_plan_total_weeks(value, default: int = 12) -> int:
    try:
        return max(1, min(520, int(value)))
    except (TypeError, ValueError):
        return max(1, min(520, int(default)))


def _normalize_seen_summary_files(value) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        return {}

    normalized: dict[str, list[str]] = {}
    for raw_folder, raw_names in value.items():
        folder = str(raw_folder or "").strip()
        if not folder:
            continue

        if isinstance(raw_names, list):
            iterable = raw_names
        elif isinstance(raw_names, (set, tuple)):
            iterable = list(raw_names)
        else:
            continue

        names: list[str] = []
        seen_names: set[str] = set()
        for raw_name in iterable:
            name = str(raw_name or "").replace("\\", "/").strip()
            if not name or name in seen_names:
                continue
            seen_names.add(name)
            names.append(name)

        if names:
            normalized[folder] = names

    return normalized


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _canonical_insight_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""

    parsed = urlsplit(raw)
    netloc = str(parsed.netloc or "").lower().strip()
    path = str(parsed.path or "").strip()

    # Handle URLs without scheme, e.g. "github.com/org/repo".
    if not netloc and raw and "/" in raw:
        parsed = urlsplit(f"https://{raw}")
        netloc = str(parsed.netloc or "").lower().strip()
        path = str(parsed.path or "").strip()

    if netloc.startswith("www."):
        netloc = netloc[4:]

    path = path.rstrip("/")
    if path.endswith(".git"):
        path = path[:-4]

    return f"{netloc}{path}"


def _normalize_iso_date(raw_value: str) -> str:
    raw = str(raw_value or "").strip()
    if not raw:
        return ""
    if len(raw) >= 10:
        raw = raw[:10]
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError:
        return ""


def _normalize_insight_url_history(value) -> dict:
    state = value if isinstance(value, dict) else {}
    urls_raw = state.get("urls", [])
    if not isinstance(urls_raw, list):
        urls_raw = []

    normalized_urls: list[dict] = []
    seen_keys: set[str] = set()

    for item in urls_raw:
        if isinstance(item, dict):
            raw_url = str(item.get("url") or "").strip()
            url_key = _canonical_insight_url(item.get("url_key") or raw_url)
            first_seen_on = _normalize_iso_date(item.get("first_seen_on"))
            last_seen_on = _normalize_iso_date(item.get("last_seen_on"))
            show_count = max(1, _safe_int(item.get("show_count", 1), default=1))
            last_project_id = str(item.get("last_project_id") or "").strip()
            last_feed_id = str(item.get("last_feed_id") or "").strip()
        else:
            raw_url = str(item or "").strip()
            url_key = _canonical_insight_url(raw_url)
            first_seen_on = ""
            last_seen_on = ""
            show_count = 1
            last_project_id = ""
            last_feed_id = ""

        if not url_key or url_key in seen_keys:
            continue

        if not first_seen_on and last_seen_on:
            first_seen_on = last_seen_on
        if not last_seen_on and first_seen_on:
            last_seen_on = first_seen_on

        normalized_urls.append(
            {
                "url_key": url_key,
                "url": raw_url,
                "first_seen_on": first_seen_on,
                "last_seen_on": last_seen_on,
                "show_count": show_count,
                "last_project_id": last_project_id,
                "last_feed_id": last_feed_id,
            }
        )
        seen_keys.add(url_key)

        if len(normalized_urls) >= _MAX_INSIGHT_URL_HISTORY:
            break

    return {
        "urls": normalized_urls,
        "updated_at": str(state.get("updated_at") or "").strip(),
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


def _normalize_summary_update_result(value, project_id: str) -> dict | None:
    if not isinstance(value, dict):
        return None

    status = str(value.get("status", "")).strip().lower()
    if status != "ok":
        return None

    aggregated_updates = value.get("aggregated_updates", [])
    if not isinstance(aggregated_updates, list):
        aggregated_updates = []

    risk_items = value.get("risk_items", [])
    if not isinstance(risk_items, list):
        risk_items = []

    new_filenames_raw = value.get("new_filenames", [])
    if not isinstance(new_filenames_raw, list):
        new_filenames_raw = []
    new_filenames = [
        str(name or "").replace("\\", "/").strip()
        for name in new_filenames_raw
        if str(name or "").strip()
    ]

    skipped_filenames_raw = value.get("skipped_filenames", [])
    if not isinstance(skipped_filenames_raw, list):
        skipped_filenames_raw = []
    skipped_filenames = [
        str(name or "").replace("\\", "/").strip()
        for name in skipped_filenames_raw
        if str(name or "").strip()
    ]
    read_at = str(value.get("read_at") or "").strip()

    return {
        "project_id": project_id,
        "status": "ok",
        "message": str(value.get("message") or "已根据团队总结更新任务状态。"),
        "read_at": read_at,
        "summaries_count": max(0, _safe_int(value.get("summaries_count", 0), default=0)),
        "updates_count": max(0, _safe_int(value.get("updates_count", 0), default=0)),
        "changed_tasks_count": max(0, _safe_int(value.get("changed_tasks_count", 0), default=0)),
        "aggregated_updates": aggregated_updates,
        "risk_count": max(0, _safe_int(value.get("risk_count", 0), default=0)),
        "risk_items": risk_items,
        "risk_lag_threshold": _normalize_risk_lag_threshold(value.get("risk_lag_threshold", 30), default=30),
        "risk_checked_on": str(value.get("risk_checked_on") or ""),
        "new_filenames": new_filenames,
        "skipped_filenames": skipped_filenames,
    }


def _normalize_string_list(value, *, limit: int = 200) -> list[str]:
    if not isinstance(value, list):
        return []

    normalized: list[str] = []
    seen: set[str] = set()
    for raw in value:
        item = str(raw or "").strip()
        if not item:
            continue
        if item in seen:
            continue
        seen.add(item)
        normalized.append(item)
        if len(normalized) >= limit:
            break

    return normalized


def _normalize_insight_source_enabled(value) -> dict[str, bool]:
    normalized = {key: True for key in _INSIGHT_SOURCE_KEYS}
    raw = value if isinstance(value, dict) else {}

    for key in _INSIGHT_SOURCE_KEYS:
        if key in raw:
            normalized[key] = bool(raw.get(key))

    return normalized


def _normalize_insight_settings(value) -> dict:
    state = value if isinstance(value, dict) else {}
    raw_enabled = state.get("enabled")
    enabled = True if raw_enabled is None else bool(raw_enabled)
    source_enabled = _normalize_insight_source_enabled(
        state.get("source_enabled") if "source_enabled" in state else state.get("sources")
    )

    return {
        "enabled": enabled,
        "source_enabled": source_enabled,
    }


def _normalize_insight_card(value: dict, index: int) -> dict | None:
    if not isinstance(value, dict):
        return None

    card_id = str(value.get("card_id") or f"card_{index}").strip()
    title = str(value.get("title") or "").strip()
    url = str(value.get("url") or "").strip()

    if not card_id or not title or not url:
        return None

    source_type = str(value.get("source_type") or "paper").strip().lower() or "paper"

    score = value.get("relevance_score", 0.0)
    try:
        relevance_score = max(0.0, min(1.0, float(score)))
    except (TypeError, ValueError):
        relevance_score = 0.0

    return {
        "card_id": card_id,
        "source": str(value.get("source") or "").strip(),
        "source_type": source_type,
        "title": title,
        "url": url,
        "project_id": str(value.get("project_id") or "").strip(),
        "project_name": str(value.get("project_name") or "").strip(),
        "core_insight": str(value.get("core_insight") or "").strip(),
        "risk_alert": str(value.get("risk_alert") or "").strip(),
        "alternative": str(value.get("alternative") or "").strip(),
        "relevance_reason": str(value.get("relevance_reason") or "").strip(),
        "evidence_snippet": str(value.get("evidence_snippet") or "").strip(),
        "relevance_score": round(relevance_score, 4),
    }


def _normalize_insight_feed(value, project_id: str) -> dict | None:
    if not isinstance(value, dict):
        return None

    feed_id = str(value.get("feed_id") or "").strip()
    raw_date = str(value.get("date") or "").strip()
    generated_at = str(value.get("generated_at") or "").strip()

    if raw_date:
        try:
            date.fromisoformat(raw_date)
        except ValueError:
            raw_date = ""

    cards_raw = value.get("cards", [])
    cards: list[dict] = []
    seen_card_ids: set[str] = set()
    if isinstance(cards_raw, list):
        for idx, item in enumerate(cards_raw, start=1):
            normalized = _normalize_insight_card(item, idx)
            if not normalized:
                continue
            card_id = normalized["card_id"]
            if card_id in seen_card_ids:
                continue
            seen_card_ids.add(card_id)
            cards.append(normalized)

    if not cards:
        return None

    retrieval = value.get("retrieval", {})
    if not isinstance(retrieval, dict):
        retrieval = {}

    return {
        "project_id": project_id,
        "feed_id": feed_id or f"feed_{raw_date or datetime.now().date().isoformat()}",
        "date": raw_date or datetime.now().date().isoformat(),
        "generated_at": generated_at,
        "cards": cards,
        "keywords": _normalize_string_list(value.get("keywords", []), limit=20),
        "retrieval": {
            "source": str(retrieval.get("source") or "").strip(),
            "query": str(retrieval.get("query") or "").strip(),
            "candidate_count": max(0, _safe_int(retrieval.get("candidate_count", 0))),
        },
    }


def _normalize_insight_feedback_events(value) -> list[dict]:
    if not isinstance(value, list):
        return []

    normalized: list[dict] = []
    for item in value:
        if not isinstance(item, dict):
            continue

        action = str(item.get("action") or "").strip().lower()
        card_id = str(item.get("card_id") or "").strip()
        if action not in _ALLOWED_INSIGHT_FEEDBACK_ACTIONS:
            continue
        if not card_id:
            continue

        normalized.append(
            {
                "action": action,
                "card_id": card_id,
                "project_id": str(item.get("project_id") or "").strip(),
                "feed_id": str(item.get("feed_id") or "").strip(),
                "ts": str(item.get("ts") or "").strip(),
            }
        )

    return normalized[-500:]


def _normalize_saved_insight_card(value: dict, index: int, project_id: str) -> dict | None:
    if not isinstance(value, dict):
        return None

    card_id = str(value.get("card_id") or "").strip()
    if not card_id:
        return None

    title = str(value.get("title") or "").strip() or f"Insight Card {index}"
    source_type = str(value.get("source_type") or "paper").strip().lower() or "paper"

    return {
        "card_id": card_id,
        "feed_id": str(value.get("feed_id") or "").strip(),
        "project_id": str(value.get("project_id") or "").strip() or project_id,
        "project_name": str(value.get("project_name") or "").strip(),
        "title": title,
        "url": str(value.get("url") or "").strip(),
        "source": str(value.get("source") or "").strip(),
        "source_type": source_type,
        "core_insight": str(value.get("core_insight") or "").strip(),
        "risk_alert": str(value.get("risk_alert") or "").strip(),
        "alternative": str(value.get("alternative") or "").strip(),
        "relevance_reason": str(value.get("relevance_reason") or "").strip(),
        "evidence_snippet": str(value.get("evidence_snippet") or "").strip(),
        "annotation": str(value.get("annotation") or "").strip(),
        "saved_at": str(value.get("saved_at") or "").strip(),
    }


def _normalize_saved_insight_cards(
    value,
    project_id: str,
    *,
    allowed_card_ids: set[str] | None = None,
) -> list[dict]:
    if not isinstance(value, list):
        return []

    normalized: list[dict] = []
    seen_card_ids: set[str] = set()
    for idx, item in enumerate(value, start=1):
        card = _normalize_saved_insight_card(item, idx, project_id)
        if not card:
            continue

        card_id = str(card.get("card_id") or "").strip()
        if not card_id:
            continue
        if allowed_card_ids is not None and card_id not in allowed_card_ids:
            continue
        if card_id in seen_card_ids:
            continue

        seen_card_ids.add(card_id)
        normalized.append(card)

        if len(normalized) >= 300:
            break

    normalized.sort(key=lambda item: str(item.get("saved_at") or ""), reverse=True)
    return normalized


def _normalize_insight_state(value, project_id: str) -> dict:
    state = value if isinstance(value, dict) else {}

    latest_feed = _normalize_insight_feed(state.get("latest_feed"), project_id)

    feeds_raw = state.get("feeds", [])
    feeds: list[dict] = []
    seen_feed_ids: set[str] = set()
    if isinstance(feeds_raw, list):
        for item in feeds_raw:
            normalized = _normalize_insight_feed(item, project_id)
            if not normalized:
                continue
            feed_id = normalized.get("feed_id", "")
            if feed_id and feed_id in seen_feed_ids:
                continue
            if feed_id:
                seen_feed_ids.add(feed_id)
            feeds.append(normalized)

    if latest_feed:
        latest_feed_id = latest_feed.get("feed_id", "")
        in_history = any(str(feed.get("feed_id") or "") == latest_feed_id for feed in feeds)
        if not in_history:
            feeds.insert(0, latest_feed)

    feeds = feeds[:30]

    raw_last_generated_on = str(state.get("last_generated_on") or "").strip()
    if raw_last_generated_on:
        try:
            date.fromisoformat(raw_last_generated_on)
        except ValueError:
            raw_last_generated_on = ""
    if not raw_last_generated_on and latest_feed:
        raw_last_generated_on = str(latest_feed.get("date") or "").strip()

    saved_card_ids = _normalize_string_list(state.get("saved_card_ids", []), limit=300)
    saved_card_id_set = {item for item in saved_card_ids if item}
    saved_cards = _normalize_saved_insight_cards(
        state.get("saved_cards", []),
        project_id,
        allowed_card_ids=saved_card_id_set if saved_card_id_set else None,
    )
    if not saved_card_id_set and saved_cards:
        saved_card_ids = [
            str(item.get("card_id") or "").strip()
            for item in saved_cards
            if str(item.get("card_id") or "").strip()
        ]

    feedback_events = _normalize_insight_feedback_events(state.get("feedback_events", []))
    pending_notification = bool(state.get("pending_notification", False)) and bool(latest_feed)

    return {
        "project_id": project_id,
        "last_generated_on": raw_last_generated_on,
        "latest_feed": latest_feed,
        "feeds": feeds,
        "saved_card_ids": saved_card_ids,
        "saved_cards": saved_cards,
        "feedback_events": feedback_events,
        "pending_notification": pending_notification,
    }


def _projects_dir() -> Path:
    _PROJECTS_DIR.mkdir(parents=True, exist_ok=True)
    return _PROJECTS_DIR


def _project_file(project_id: str) -> Path:
    return _projects_dir() / f"{project_id}.json"


def _load_registry() -> list[dict]:
    data = read_json(str(_REGISTRY_FILE), default={"projects": []})
    return data.get("projects", [])


def _save_registry(projects: list[dict]) -> None:
    write_json(str(_REGISTRY_FILE), {"projects": projects})


def _touch_project_updated_at(project_id: str, updated_at: str | None = None) -> str:
    normalized_project_id = str(project_id or "").strip()
    if not normalized_project_id:
        return ""

    touched_at = str(updated_at or "").strip() or datetime.now().isoformat(timespec="seconds")
    projects = _load_registry()
    changed = False
    for project in projects:
        if str(project.get("id") or "").strip() != normalized_project_id:
            continue
        project["updated_at"] = touched_at
        changed = True
        break

    if changed:
        _save_registry(projects)

    return touched_at


# ---------------------------------------------------------------------------
# Project CRUD
# ---------------------------------------------------------------------------

def list_projects() -> list[dict]:
    """Return all projects sorted by created_at descending."""
    projects = _load_registry()
    # Auto-migrate legacy state on first run
    if not projects:
        legacy = read_json(str(_LEGACY_STATE_FILE), default={"tasks": []})
        legacy_tasks = legacy.get("tasks", [])
        if legacy_tasks:
            pid = _create_project_record("My First Project")
            write_json(str(_project_file(pid["id"])), {"tasks": legacy_tasks})
            projects = [pid]

    normalized_projects: list[dict] = []
    changed = False
    for project in projects:
        if not isinstance(project, dict):
            continue

        project_id = str(project.get("id") or "").strip()
        if not project_id:
            continue

        created_at = str(project.get("created_at") or "").strip()
        if not created_at:
            created_at = datetime.now().isoformat(timespec="seconds")
            project["created_at"] = created_at
            changed = True

        updated_at = str(project.get("updated_at") or "").strip()
        if not updated_at:
            updated_at = created_at
            project["updated_at"] = updated_at
            changed = True

        normalized = dict(project)
        normalized["id"] = project_id
        normalized["name"] = str(project.get("name") or "").strip() or "Untitled Project"
        normalized["created_at"] = created_at
        normalized["updated_at"] = updated_at
        normalized_projects.append(normalized)

    if changed:
        _save_registry(projects)

    return sorted(normalized_projects, key=lambda p: p.get("created_at", ""), reverse=True)


def create_project(name: str) -> dict:
    """Create a new project and return the project record."""
    projects = _load_registry()
    record = _create_project_record(name)
    write_json(str(_project_file(record["id"])), {"tasks": [], "dependencies": []})
    projects.append(record)
    _save_registry(projects)
    return record


def _create_project_record(name: str) -> dict:
    now = datetime.now().isoformat(timespec="seconds")
    return {
        "id": uuid.uuid4().hex,
        "name": name.strip() or "Untitled Project",
        "created_at": now,
        "updated_at": now,
    }


def delete_project(project_id: str) -> None:
    """Delete a project and its task data."""
    projects = [p for p in _load_registry() if p["id"] != project_id]
    _save_registry(projects)
    f = _project_file(project_id)
    if f.exists():
        f.unlink()


def rename_project(project_id: str, new_name: str) -> None:
    projects = _load_registry()
    for p in projects:
        if p["id"] == project_id:
            p["name"] = new_name.strip() or p["name"]
            p["updated_at"] = datetime.now().isoformat(timespec="seconds")
            break
    _save_registry(projects)


# ---------------------------------------------------------------------------
# Task normalization
# ---------------------------------------------------------------------------

def _normalize_task(task: dict, index: int) -> dict:
    name = task.get("task", task.get("name", f"Task {index}"))
    task_id = str(task.get("task_id") or f"T{index}").strip() or f"T{index}"
    duration = task.get("duration", 1)
    try:
        duration = max(1, int(duration))
    except (TypeError, ValueError):
        duration = 1

    duration_days = task.get("duration_days")
    if duration_days is None:
        duration_days = duration * 7
    try:
        duration_days = max(1, int(duration_days))
    except (TypeError, ValueError):
        duration_days = duration * 7

    progress = task.get("progress", 0)
    try:
        progress = max(0, min(100, int(progress)))
    except (TypeError, ValueError):
        progress = 0

    kr_index = task.get("kr_index")
    try:
        kr_index = int(kr_index) if kr_index is not None else None
    except (TypeError, ValueError):
        kr_index = None

    subtask_index = task.get("subtask_index")
    try:
        subtask_index = int(subtask_index) if subtask_index is not None else None
    except (TypeError, ValueError):
        subtask_index = None

    start_week = task.get("start_week")
    try:
        start_week = max(1, int(start_week)) if start_week is not None and str(start_week).strip() != "" else None
    except (TypeError, ValueError):
        start_week = None

    return {
        "task_id": task_id,
        "task": name,
        "start_week": start_week,
        "start": task.get("start", ""),
        "end": task.get("end", ""),
        "duration": duration,
        "duration_days": duration_days,
        "owner": task.get("owner", "Unassigned"),
        "status": task.get("status", "Planned"),
        "progress": progress,
        "kr": task.get("kr", "KR1: Execution"),
        "objective": task.get("objective", ""),
        "kr_index": kr_index,
        "subtask_index": subtask_index,
    }


def _normalize_dependency(dep: dict) -> dict | None:
    if not isinstance(dep, dict):
        return None

    source = str(dep.get("from") or dep.get("source") or "").strip()
    target = str(dep.get("to") or dep.get("target") or "").strip()
    if not source or not target or source == target:
        return None

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

    return {
        "from": source,
        "to": target,
        "type": dep_type,
        "lag_weeks": lag_weeks,
        "overlap_weeks": overlap_weeks,
    }


# ---------------------------------------------------------------------------
# Task I/O (project-scoped)
# ---------------------------------------------------------------------------

def save_tasks(tasks: list[dict], project_id: str, dependencies: list[dict] | None = None) -> None:
    normalized_tasks = [_normalize_task(task, index) for index, task in enumerate(tasks, start=1)]
    normalized_dependencies = []
    if dependencies:
        seen: set[tuple] = set()
        for dep in dependencies:
            normalized = _normalize_dependency(dep)
            if not normalized:
                continue
            key = (
                normalized["from"],
                normalized["to"],
                normalized["type"],
                normalized["lag_weeks"],
                normalized["overlap_weeks"],
            )
            if key in seen:
                continue
            seen.add(key)
            normalized_dependencies.append(normalized)

    file_path = str(_project_file(project_id))
    existing_state = read_json(file_path, default={})
    merged_state = dict(existing_state)
    merged_state["tasks"] = normalized_tasks
    merged_state["dependencies"] = normalized_dependencies
    write_json(file_path, merged_state)
    _touch_project_updated_at(project_id)


def load_tasks(project_id: str) -> list[dict]:
    state = read_json(str(_project_file(project_id)), default={"tasks": []})
    tasks = state.get("tasks", [])
    if not isinstance(tasks, list):
        return []
    return [_normalize_task(task, index) for index, task in enumerate(tasks, start=1)]


def load_dependencies(project_id: str) -> list[dict]:
    state = read_json(str(_project_file(project_id)), default={"dependencies": []})
    dependencies = state.get("dependencies", [])
    if not isinstance(dependencies, list):
        return []

    normalized_deps: list[dict] = []
    seen: set[tuple] = set()
    for dep in dependencies:
        normalized = _normalize_dependency(dep)
        if not normalized:
            continue
        key = (
            normalized["from"],
            normalized["to"],
            normalized["type"],
            normalized["lag_weeks"],
            normalized["overlap_weeks"],
        )
        if key in seen:
            continue
        seen.add(key)
        normalized_deps.append(normalized)

    return normalized_deps


def get_project_risk_lag_threshold(project_id: str, default: int = 30) -> tuple[int, bool]:
    state = read_json(str(_project_file(project_id)), default={})
    has_saved_value = "risk_lag_threshold" in state
    raw_value = state.get("risk_lag_threshold", default)
    threshold = _normalize_risk_lag_threshold(raw_value, default=default)
    return threshold, has_saved_value


def save_project_risk_lag_threshold(project_id: str, threshold: int) -> int:
    file_path = str(_project_file(project_id))
    state = read_json(file_path, default={})
    normalized_threshold = _normalize_risk_lag_threshold(threshold)
    state["risk_lag_threshold"] = normalized_threshold
    write_json(file_path, state)
    return normalized_threshold


def get_project_gantt_theme(project_id: str, default: str = "default") -> tuple[str, bool]:
    state = read_json(str(_project_file(project_id)), default={})
    has_saved_value = "gantt_theme" in state
    raw_value = state.get("gantt_theme", default)
    theme = _normalize_gantt_theme(raw_value, default=default)
    return theme, has_saved_value


def save_project_gantt_theme(project_id: str, theme: str) -> str:
    file_path = str(_project_file(project_id))
    state = read_json(file_path, default={})
    normalized_theme = _normalize_gantt_theme(theme)
    state["gantt_theme"] = normalized_theme
    write_json(file_path, state)
    return normalized_theme


def get_project_plan_total_weeks(project_id: str, default: int = 12) -> tuple[int, bool]:
    state = read_json(str(_project_file(project_id)), default={})
    has_saved_value = "plan_total_weeks" in state
    raw_value = state.get("plan_total_weeks", default)
    total_weeks = _normalize_plan_total_weeks(raw_value, default=default)
    return total_weeks, has_saved_value


def save_project_plan_total_weeks(project_id: str, total_weeks: int) -> int:
    file_path = str(_project_file(project_id))
    state = read_json(file_path, default={})
    normalized_weeks = _normalize_plan_total_weeks(total_weeks)
    state["plan_total_weeks"] = normalized_weeks
    write_json(file_path, state)
    return normalized_weeks


def load_project_summary_state(project_id: str) -> dict:
    """Load project-scoped summary update state.

    Shape:
            {
                "saved_summary_folder": str,
                "seen_summary_files": {folder_path: [relative_filename, ...]},
                "latest_summary_update_result": dict | None,
            }
    """
    state = read_json(str(_project_file(project_id)), default={})

    saved_summary_folder = state.get("saved_summary_folder", "")
    if not isinstance(saved_summary_folder, str):
        saved_summary_folder = ""
    saved_summary_folder = saved_summary_folder.strip()

    seen_summary_files = _normalize_seen_summary_files(state.get("seen_summary_files", {}))
    latest_summary_update_result = _normalize_summary_update_result(
        state.get("latest_summary_update_result"),
        project_id,
    )

    return {
        "saved_summary_folder": saved_summary_folder,
        "seen_summary_files": seen_summary_files,
        "latest_summary_update_result": latest_summary_update_result,
    }


def save_project_summary_state(
    project_id: str,
    *,
    saved_summary_folder: str | None = None,
    seen_summary_files: dict | None = None,
    latest_summary_update_result: dict | None = None,
    clear_latest_summary_update_result: bool = False,
) -> dict:
    """Persist summary update state and return the normalized merged state."""
    file_path = str(_project_file(project_id))
    state = read_json(file_path, default={})

    if saved_summary_folder is not None:
        state["saved_summary_folder"] = str(saved_summary_folder).strip()

    if seen_summary_files is not None:
        state["seen_summary_files"] = _normalize_seen_summary_files(seen_summary_files)

    if clear_latest_summary_update_result:
        state.pop("latest_summary_update_result", None)
    elif latest_summary_update_result is not None:
        normalized_result = _normalize_summary_update_result(latest_summary_update_result, project_id)
        if normalized_result is None:
            state.pop("latest_summary_update_result", None)
        else:
            state["latest_summary_update_result"] = normalized_result

    write_json(file_path, state)
    _touch_project_updated_at(project_id)
    return load_project_summary_state(project_id)


# ---------------------------------------------------------------------------
# Assistant state (project-scoped)
# ---------------------------------------------------------------------------

def _normalize_assistant_chat_history(history: object) -> list[dict]:
    if not isinstance(history, list):
        return []

    normalized: list[dict] = []
    for item in history:
        if isinstance(item, dict):
            normalized.append(dict(item))
    return normalized


def _merge_assistant_chat_history(existing_history: object, incoming_history: object) -> list[dict]:
    """Preserve older chat entries when callers write back a trimmed suffix."""
    merged_incoming = _normalize_assistant_chat_history(incoming_history)
    if not merged_incoming:
        return merged_incoming

    merged_existing = _normalize_assistant_chat_history(existing_history)
    if not merged_existing or len(merged_incoming) >= len(merged_existing):
        return merged_incoming
    if merged_incoming == merged_existing:
        return merged_incoming

    max_overlap = min(len(merged_existing), len(merged_incoming))
    for overlap in range(max_overlap, 0, -1):
        if merged_existing[-overlap:] == merged_incoming[:overlap]:
            return merged_existing + merged_incoming[overlap:]

    return merged_incoming

def load_project_assistant_state(project_id: str) -> dict:
    """Load project-scoped assistant state.

    Shape:
      {
        "chat_history": [{"role": "user|assistant", "content": str, "ts": str}],
        "assistant_memory": str,
        "last_actions": [{"summary": str, "ts": str}],
        "pending_change": {...} | None,
      }
    """
    state = read_json(str(_project_file(project_id)), default={})
    chat_history = _normalize_assistant_chat_history(state.get("chat_history", []))

    assistant_memory = state.get("assistant_memory", "")
    if not isinstance(assistant_memory, str):
        assistant_memory = ""

    last_actions = state.get("last_actions", [])
    if not isinstance(last_actions, list):
        last_actions = []

    pending_change = state.get("pending_change")
    if pending_change is not None and not isinstance(pending_change, dict):
        pending_change = None

    return {
        "chat_history": chat_history,
        "assistant_memory": assistant_memory,
        "last_actions": last_actions,
        "pending_change": pending_change,
    }


def save_project_assistant_state(
    project_id: str,
    *,
    chat_history: list[dict] | None = None,
    assistant_memory: str | None = None,
    last_actions: list[dict] | None = None,
    pending_change: dict | None = None,
    clear_pending_change: bool = False,
) -> dict:
    """Persist project-scoped assistant state and return merged state."""
    file_path = str(_project_file(project_id))
    state = read_json(file_path, default={})

    if chat_history is not None:
        state["chat_history"] = _merge_assistant_chat_history(state.get("chat_history", []), chat_history)
    if assistant_memory is not None:
        state["assistant_memory"] = assistant_memory
    if last_actions is not None:
        state["last_actions"] = last_actions
    if clear_pending_change:
        state.pop("pending_change", None)
    elif pending_change is not None:
        state["pending_change"] = pending_change

    write_json(file_path, state)
    return load_project_assistant_state(project_id)


# ---------------------------------------------------------------------------
# Insight state (project-scoped)
# ---------------------------------------------------------------------------

def load_project_insight_state(project_id: str) -> dict:
    state = read_json(str(_project_file(project_id)), default={})
    insight_state = state.get("insight_state", {})
    return _normalize_insight_state(insight_state, project_id)


def save_project_insight_state(project_id: str, insight_state: dict) -> dict:
    file_path = str(_project_file(project_id))
    state = read_json(file_path, default={})
    state["insight_state"] = _normalize_insight_state(insight_state, project_id)
    write_json(file_path, state)
    return load_project_insight_state(project_id)


def load_workspace_insight_state() -> dict:
    raw_state = read_json(str(_WORKSPACE_INSIGHT_STATE_FILE), default={})
    insight_state = raw_state.get("insight_state", raw_state) if raw_state else {}
    normalized = _normalize_insight_state(insight_state, "__workspace__")

    if raw_state.get("insight_state") != normalized:
        write_json(str(_WORKSPACE_INSIGHT_STATE_FILE), {"insight_state": normalized})

    return normalized


def save_workspace_insight_state(insight_state: dict) -> dict:
    normalized = _normalize_insight_state(insight_state, "__workspace__")
    write_json(str(_WORKSPACE_INSIGHT_STATE_FILE), {"insight_state": normalized})
    return load_workspace_insight_state()


def _derive_workspace_insight_settings_from_projects() -> dict:
    best_settings: dict | None = None
    best_sort_key = ("", "", "")

    for project in list_projects():
        project_id = str(project.get("id") or "").strip()
        if not project_id:
            continue

        raw_state = read_json(str(_project_file(project_id)), default={})
        if "insight_settings" not in raw_state:
            continue

        candidate = _normalize_insight_settings(raw_state.get("insight_settings", {}))
        sort_key = (
            str(project.get("updated_at") or "").strip(),
            str(project.get("created_at") or "").strip(),
            project_id,
        )
        if best_settings is None or sort_key > best_sort_key:
            best_settings = candidate
            best_sort_key = sort_key

    return best_settings if isinstance(best_settings, dict) else _normalize_insight_settings({})


def load_workspace_insight_settings() -> dict:
    raw_state = read_json(str(_WORKSPACE_INSIGHT_SETTINGS_FILE), default={})
    if raw_state:
        settings = raw_state.get("insight_settings", raw_state)
        return _normalize_insight_settings(settings)

    migrated = _derive_workspace_insight_settings_from_projects()
    normalized = _normalize_insight_settings(migrated)
    write_json(str(_WORKSPACE_INSIGHT_SETTINGS_FILE), {"insight_settings": normalized})
    return normalized


def save_workspace_insight_settings(
    *,
    enabled: bool | None = None,
    source_enabled: dict | None = None,
) -> dict:
    current = load_workspace_insight_settings()

    if enabled is not None:
        current["enabled"] = bool(enabled)
    if source_enabled is not None:
        current["source_enabled"] = _normalize_insight_source_enabled(source_enabled)

    normalized = _normalize_insight_settings(current)
    write_json(str(_WORKSPACE_INSIGHT_SETTINGS_FILE), {"insight_settings": normalized})
    return load_workspace_insight_settings()


def load_project_insight_settings(project_id: str) -> dict:
    state = read_json(str(_project_file(project_id)), default={})
    insight_settings = state.get("insight_settings", {})
    return _normalize_insight_settings(insight_settings)


def save_project_insight_settings(
    project_id: str,
    *,
    enabled: bool | None = None,
    source_enabled: dict | None = None,
) -> dict:
    file_path = str(_project_file(project_id))
    state = read_json(file_path, default={})
    current = _normalize_insight_settings(state.get("insight_settings", {}))

    if enabled is not None:
        current["enabled"] = bool(enabled)
    if source_enabled is not None:
        current["source_enabled"] = _normalize_insight_source_enabled(source_enabled)

    state["insight_settings"] = _normalize_insight_settings(current)
    write_json(file_path, state)
    _touch_project_updated_at(project_id)
    return load_project_insight_settings(project_id)


def load_insight_url_history() -> dict:
    raw = read_json(str(_INSIGHT_URL_HISTORY_FILE), default={})
    normalized = _normalize_insight_url_history(raw)

    if raw != normalized:
        write_json(str(_INSIGHT_URL_HISTORY_FILE), normalized)

    return normalized


def get_seen_insight_url_keys(*, before_date: str = "") -> set[str]:
    history = load_insight_url_history()
    urls = history.get("urls", []) if isinstance(history.get("urls"), list) else []

    cutoff_date = _normalize_iso_date(before_date)
    dedup_keys: set[str] = set()

    for item in urls:
        if not isinstance(item, dict):
            continue

        url_key = str(item.get("url_key") or "").strip()
        if not url_key:
            continue

        if cutoff_date:
            seen_date = _normalize_iso_date(item.get("last_seen_on") or item.get("first_seen_on") or "")
            if seen_date and seen_date >= cutoff_date:
                continue

        dedup_keys.add(url_key)

    return dedup_keys


def record_shown_insight_urls(
    urls: list[str],
    *,
    seen_on: str = "",
    project_id: str = "",
    feed_id: str = "",
) -> dict:
    history = load_insight_url_history()
    current_items = history.get("urls", []) if isinstance(history.get("urls"), list) else []

    by_key: dict[str, dict] = {}
    for item in current_items:
        if not isinstance(item, dict):
            continue
        key = str(item.get("url_key") or "").strip()
        if not key:
            continue
        by_key[key] = dict(item)

    normalized_seen_on = _normalize_iso_date(seen_on) or date.today().isoformat()
    normalized_project_id = str(project_id or "").strip()
    normalized_feed_id = str(feed_id or "").strip()

    for raw_url in urls:
        url_text = str(raw_url or "").strip()
        url_key = _canonical_insight_url(url_text)
        if not url_key:
            continue

        existing = by_key.get(url_key)
        if existing is None:
            by_key[url_key] = {
                "url_key": url_key,
                "url": url_text,
                "first_seen_on": normalized_seen_on,
                "last_seen_on": normalized_seen_on,
                "show_count": 1,
                "last_project_id": normalized_project_id,
                "last_feed_id": normalized_feed_id,
            }
            continue

        if not str(existing.get("url") or "").strip() and url_text:
            existing["url"] = url_text
        if not str(existing.get("first_seen_on") or "").strip():
            existing["first_seen_on"] = normalized_seen_on
        existing["last_seen_on"] = normalized_seen_on
        existing["show_count"] = max(1, _safe_int(existing.get("show_count", 1), default=1) + 1)
        if normalized_project_id:
            existing["last_project_id"] = normalized_project_id
        if normalized_feed_id:
            existing["last_feed_id"] = normalized_feed_id
        by_key[url_key] = existing

    merged_urls = sorted(
        by_key.values(),
        key=lambda item: (
            str(item.get("last_seen_on") or ""),
            _safe_int(item.get("show_count", 0), default=0),
        ),
        reverse=True,
    )[:_MAX_INSIGHT_URL_HISTORY]

    normalized_history = {
        "urls": merged_urls,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    write_json(str(_INSIGHT_URL_HISTORY_FILE), normalized_history)
    return normalized_history