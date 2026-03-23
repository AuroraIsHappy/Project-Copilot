from planner.okr_planner import generate_tasks
from planner.okr_planner import generate_tasks_with_dependencies
from planner.multi_agent_planner import multi_agent_plan
from planning.scheduler import schedule_tasks
from analysis.summary_parser import find_summary_snippets
from analysis.summary_parser import load_summaries_with_names
from analysis.summary_parser import normalize_summary_folder_path
from analysis.progress_estimator import estimate_progress_from_summaries
from analysis.risk_predictor import predict_risk
from insight.engine import generate_insight_feed
from visualization.gantt_renderer import create_gantt
from visualization.gantt_renderer import list_gantt_themes
from visualization.graph_renderer import create_dependency_graph
from utils.llm_client import call_llm
from utils.llm_client import call_llm_messages
from utils.llm_client import load_llm_config
from state.state_manager import save_tasks
from state.state_manager import load_tasks
from state.state_manager import load_dependencies
from state.state_manager import get_project_risk_lag_threshold
from state.state_manager import get_project_gantt_theme
from state.state_manager import save_project_risk_lag_threshold
from state.state_manager import save_project_gantt_theme
from state.state_manager import get_project_plan_total_weeks
from state.state_manager import save_project_plan_total_weeks
from state.state_manager import list_projects
from state.state_manager import create_project
from state.state_manager import delete_project
from state.state_manager import load_project_assistant_state
from state.state_manager import save_project_assistant_state
from state.state_manager import load_project_insight_state
from state.state_manager import save_project_insight_state
from state.state_manager import load_workspace_insight_state
from state.state_manager import save_workspace_insight_state
from state.state_manager import load_workspace_insight_settings
from state.state_manager import save_workspace_insight_settings
from state.state_manager import load_project_insight_settings
from state.state_manager import save_project_insight_settings
from state.state_manager import load_project_summary_state
from state.state_manager import get_seen_insight_url_keys
from state.state_manager import record_shown_insight_urls
from state.assistant_memory import read_project_memory
from state.assistant_memory import read_system_memory
from state.assistant_memory import upsert_system_memory
from state.assistant_memory import upsert_project_memory
from state.assistant_memory import reclassify_memory_item as reclassify_memory_item_store

import json
import math
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from pathlib import Path
import re
import threading
import uuid
from datetime import date
from datetime import datetime
from urllib.parse import urlsplit


_INSIGHT_CARD_COUNT = 3
_INSIGHT_WEIGHT_DECAY_DAYS = 7.0
_INSIGHT_MIN_CANDIDATE_LIMIT = 4
_INSIGHT_MAX_CANDIDATE_LIMIT = 12
_GLOBAL_INSIGHT_RULE_FILE = Path(__file__).resolve().parents[1] / "insight_recommendation_rule.md"
_GLOBAL_INSIGHT_RULE_COMPRESS_TRIGGER_CHARS = 12000
_GLOBAL_INSIGHT_RULE_TARGET_CHARS = 8000
_GLOBAL_INSIGHT_RULE_PROMPT_MAX_CHARS = 6000
_GLOBAL_INSIGHT_RULE_EXAMPLE_LIMIT = 36
_GLOBAL_INSIGHT_RULE_UPDATE_LOCK = threading.Lock()
_INSIGHT_SOURCE_ORDER = ("arxiv", "github", "blog", "reddit")
_WORKSPACE_INSIGHT_PROJECT_ID = "__workspace__"


def _parse_okr_context(okr_text: str) -> tuple[str, list[str]]:
    lines = [line.strip() for line in okr_text.splitlines() if line.strip()]
    objective = ""
    krs: list[str] = []

    for line in lines:
        low = line.lower()
        if low.startswith("o:") or low.startswith("objective:"):
            objective = line.split(":", 1)[1].strip() if ":" in line else line
        elif low.startswith("kr"):
            krs.append(line)

    if not objective and lines:
        objective = lines[0]

    if not krs:
        krs = ["KR1: Execution"]

    return objective, krs


def _group_tasks_into_kr_blocks(tasks: list[dict], objective: str, krs: list[str]) -> list[dict]:
    if not tasks:
        return []

    if not krs:
        krs = ["KR1: Execution"]

    ordered_tasks: list[dict] = []
    base_count, remainder = divmod(len(tasks), len(krs))
    cursor = 0

    for kr_index, kr in enumerate(krs, start=1):
        block_size = base_count + (1 if kr_index <= remainder else 0)
        if block_size <= 0:
            continue

        for subtask_index, task in enumerate(tasks[cursor:cursor + block_size], start=1):
            ordered_task = dict(task)
            ordered_task["objective"] = objective
            ordered_task["kr"] = kr
            ordered_task["kr_index"] = kr_index
            ordered_task["subtask_index"] = subtask_index
            ordered_tasks.append(ordered_task)

        cursor += block_size

    if cursor < len(tasks):
        fallback_kr = krs[-1]
        fallback_kr_index = len(krs)
        extra_start = sum(1 for task in ordered_tasks if task.get("kr") == fallback_kr) + 1
        for offset, task in enumerate(tasks[cursor:], start=extra_start):
            ordered_task = dict(task)
            ordered_task["objective"] = objective
            ordered_task["kr"] = fallback_kr
            ordered_task["kr_index"] = fallback_kr_index
            ordered_task["subtask_index"] = offset
            ordered_tasks.append(ordered_task)

    return ordered_tasks


def generate_plan(okr_text: str, project_id: str):

    return generate_plan_by_mode(okr_text, project_id, mode="single")


def _to_internal_task_shape(tasks: list[dict]) -> list[dict]:
    normalized: list[dict] = []
    for index, task in enumerate(tasks, start=1):
        if not isinstance(task, dict):
            continue

        task_id = str(task.get("task_id") or f"T{index}").strip() or f"T{index}"
        task_name = str(task.get("task_name") or task.get("task") or task.get("name") or "").strip()
        if not task_name:
            continue

        raw_weeks = task.get("duration_weeks")
        if raw_weeks is None:
            raw_weeks = task.get("duration_week")
        if raw_weeks is None:
            raw_weeks = task.get("duration")
        if raw_weeks is None:
            raw_days = task.get("duration_days")
            try:
                raw_weeks = max(1, int(raw_days) // 7)
            except (TypeError, ValueError):
                raw_weeks = 1

        try:
            duration_weeks = max(1, int(raw_weeks))
        except (TypeError, ValueError):
            duration_weeks = 1
        duration_days = duration_weeks * 7

        normalized.append(
            {
                "task_id": task_id,
                "task": task_name,
                "duration_days": duration_days,
                "duration_weeks": duration_weeks,
                "duration": duration_weeks,
            }
        )

    return normalized


def generate_plan_by_mode(
    okr_text: str,
    project_id: str,
    mode: str = "single",
    progress_callback=None,
    target_total_weeks: int | None = None,
):

    # Step1 任务生成
    planning_mode = (mode or "single").strip().lower()
    normalized_total_weeks: int | None = None
    if target_total_weeks is not None:
        try:
            normalized_total_weeks = max(1, int(target_total_weeks))
        except (TypeError, ValueError):
            normalized_total_weeks = None

    dependencies: list[dict] = []
    if planning_mode == "multi":
        multi_result = multi_agent_plan(
            okr_text,
            target_total_weeks=normalized_total_weeks,
            progress_callback=progress_callback,
        )
        tasks = _to_internal_task_shape(multi_result.get("tasks", []))
        dependencies = multi_result.get("dependencies", []) or []
    else:
        single_result = generate_tasks_with_dependencies(
            okr_text,
            target_total_weeks=normalized_total_weeks,
        )
        tasks = single_result.get("tasks", []) or generate_tasks(
            okr_text,
            target_total_weeks=normalized_total_weeks,
        )
        dependencies = single_result.get("dependencies", []) or []

    objective, krs = _parse_okr_context(okr_text)
    ordered_tasks = _group_tasks_into_kr_blocks(tasks, objective, krs)

    # Step2 自动排期
    target_total_days = normalized_total_weeks * 7 if normalized_total_weeks is not None else None
    scheduled_tasks = schedule_tasks(
        ordered_tasks,
        dependencies=dependencies,
        target_total_days=target_total_days,
    )

    # Step3 保存状态
    save_tasks(scheduled_tasks, project_id, dependencies=dependencies)
    if normalized_total_weeks is not None:
        save_project_plan_total_weeks(project_id, normalized_total_weeks)

    return scheduled_tasks


def load_tasks_from_state(project_id: str):
    return load_tasks(project_id)


def load_plan_from_state(project_id: str) -> dict:
    return {
        "tasks": load_tasks(project_id),
        "dependencies": load_dependencies(project_id),
    }


def _today_iso() -> str:
    return date.today().isoformat()


def _resolve_project_name(project_id: str) -> str:
    for project in list_projects():
        if str(project.get("id") or "").strip() == project_id:
            return str(project.get("name") or "").strip() or "当前项目"
    return "当前项目"


def _parse_iso_datetime(raw_value: str) -> datetime | None:
    raw = str(raw_value or "").strip()
    if not raw:
        return None

    try:
        if len(raw) == 10:
            return datetime.fromisoformat(raw)
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _project_last_updated_at(project: dict) -> datetime:
    updated = _parse_iso_datetime(project.get("updated_at", ""))
    if updated is not None:
        return updated

    created = _parse_iso_datetime(project.get("created_at", ""))
    if created is not None:
        return created

    return datetime.now()


def _canonical_url_key(raw_url: str) -> str:
    text = str(raw_url or "").strip()
    if not text:
        return ""

    parsed = urlsplit(text)
    netloc = str(parsed.netloc or "").lower().strip()
    path = str(parsed.path or "").strip().rstrip("/")

    if not netloc and "/" in text:
        fallback = urlsplit(f"https://{text}")
        netloc = str(fallback.netloc or "").lower().strip()
        path = str(fallback.path or "").strip().rstrip("/")

    if netloc.startswith("www."):
        netloc = netloc[4:]
    if path.endswith(".git"):
        path = path[:-4]

    return f"{netloc}{path}"


def _safe_markdown_text(value: str, *, max_chars: int = 240) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if not text:
        return "N/A"
    if len(text) > max_chars:
        return f"{text[:max_chars - 1].rstrip()}..."
    return text


def _compact_optional_text(value: str, *, max_chars: int = 240) -> str:
    compacted = _safe_markdown_text(value, max_chars=max_chars)
    return "" if compacted == "N/A" else compacted


def _format_insight_card_memory_lines(card_snapshot: dict) -> list[str]:
    lines: list[str] = []
    field_specs = (
        ("核心洞察", str(card_snapshot.get("core_insight") or ""), 220),
        ("项目关联", str(card_snapshot.get("relevance_reason") or ""), 220),
        ("证据摘录", str(card_snapshot.get("evidence_snippet") or ""), 220),
        ("风险提醒", str(card_snapshot.get("risk_alert") or ""), 220),
        ("替代方向", str(card_snapshot.get("alternative") or ""), 220),
    )

    for label, value, max_chars in field_specs:
        compacted = _compact_optional_text(value, max_chars=max_chars)
        if compacted:
            lines.append(f"- {label}: {compacted}")

    return lines


def _strip_markdown_fence(text: str) -> str:
    raw = str(text or "").strip()
    if not raw.startswith("```"):
        return raw

    lines = raw.splitlines()
    if lines and lines[0].strip().startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _truncate_for_prompt(text: str, max_chars: int) -> str:
    cleaned = str(text or "").strip()
    max_chars = max(200, int(max_chars or 0))
    if len(cleaned) <= max_chars:
        return cleaned

    marker = "\n\n[...省略部分历史规则...]\n\n"
    remaining = max_chars - len(marker)
    if remaining <= 0:
        return cleaned[:max_chars]

    head_chars = max(120, remaining // 2)
    tail_chars = max(120, remaining - head_chars)
    head = cleaned[:head_chars].rstrip()
    tail = cleaned[-tail_chars:].lstrip()
    return f"{head}{marker}{tail}"


def _iter_insight_feeds(insight_state: dict) -> list[dict]:
    feeds: list[dict] = []

    latest_feed = insight_state.get("latest_feed") if isinstance(insight_state, dict) else None
    if isinstance(latest_feed, dict):
        feeds.append(latest_feed)

    feeds_raw = insight_state.get("feeds", []) if isinstance(insight_state, dict) else []
    if isinstance(feeds_raw, list):
        for item in feeds_raw:
            if isinstance(item, dict):
                feeds.append(item)

    return feeds


def _extract_card_snapshot_from_state(
    insight_state: dict,
    project_id: str,
    card_id: str,
    feed_id: str = "",
) -> dict:
    normalized_card_id = str(card_id or "").strip()
    normalized_feed_id = str(feed_id or "").strip()
    project_name = _resolve_project_name(project_id)

    if not normalized_card_id:
        return {
            "card_id": "",
            "feed_id": normalized_feed_id,
            "project_id": project_id,
            "project_name": project_name,
            "title": "",
            "url": "",
            "source": "",
            "source_type": "",
            "core_insight": "",
            "risk_alert": "",
            "alternative": "",
            "relevance_reason": "",
            "evidence_snippet": "",
        }

    feeds = _iter_insight_feeds(insight_state)
    if normalized_feed_id:
        feeds = sorted(
            feeds,
            key=lambda item: 0 if str(item.get("feed_id") or "").strip() == normalized_feed_id else 1,
        )

    for feed in feeds:
        current_feed_id = str(feed.get("feed_id") or "").strip()
        cards_raw = feed.get("cards", []) if isinstance(feed.get("cards"), list) else []
        for card in cards_raw:
            if not isinstance(card, dict):
                continue

            current_card_id = str(card.get("card_id") or "").strip()
            if current_card_id != normalized_card_id:
                continue

            card_project_id = str(card.get("project_id") or "").strip() or project_id
            card_project_name = str(card.get("project_name") or "").strip()
            if not card_project_name:
                card_project_name = _resolve_project_name(card_project_id)

            return {
                "card_id": normalized_card_id,
                "feed_id": current_feed_id or normalized_feed_id,
                "project_id": card_project_id,
                "project_name": card_project_name or project_name,
                "title": str(card.get("title") or "").strip(),
                "url": str(card.get("url") or "").strip(),
                "source": str(card.get("source") or "").strip(),
                "source_type": str(card.get("source_type") or "").strip(),
                "core_insight": str(card.get("core_insight") or "").strip(),
                "risk_alert": str(card.get("risk_alert") or "").strip(),
                "alternative": str(card.get("alternative") or "").strip(),
                "relevance_reason": str(card.get("relevance_reason") or "").strip(),
                "evidence_snippet": str(card.get("evidence_snippet") or "").strip(),
            }

    return {
        "card_id": normalized_card_id,
        "feed_id": normalized_feed_id,
        "project_id": project_id,
        "project_name": project_name,
        "title": "",
        "url": "",
        "source": "",
        "source_type": "",
        "core_insight": "",
        "risk_alert": "",
        "alternative": "",
        "relevance_reason": "",
        "evidence_snippet": "",
    }


def _normalize_saved_card_snapshot(value: dict, fallback_project_id: str) -> dict | None:
    if not isinstance(value, dict):
        return None

    card_id = str(value.get("card_id") or "").strip()
    if not card_id:
        return None

    project_id = str(value.get("project_id") or "").strip() or str(fallback_project_id or "").strip()
    project_name = str(value.get("project_name") or "").strip() or _resolve_project_name(project_id)

    return {
        "card_id": card_id,
        "feed_id": str(value.get("feed_id") or "").strip(),
        "project_id": project_id,
        "project_name": project_name,
        "title": str(value.get("title") or "").strip(),
        "url": str(value.get("url") or "").strip(),
        "source": str(value.get("source") or "").strip(),
        "source_type": str(value.get("source_type") or "").strip(),
        "core_insight": str(value.get("core_insight") or "").strip(),
        "risk_alert": str(value.get("risk_alert") or "").strip(),
        "alternative": str(value.get("alternative") or "").strip(),
        "relevance_reason": str(value.get("relevance_reason") or "").strip(),
        "evidence_snippet": str(value.get("evidence_snippet") or "").strip(),
        "annotation": str(value.get("annotation") or "").strip(),
        "saved_at": str(value.get("saved_at") or "").strip(),
    }


def _enabled_insight_sources(insight_settings: dict) -> list[str]:
    source_enabled = (
        insight_settings.get("source_enabled", {})
        if isinstance(insight_settings, dict)
        else {}
    )
    return [
        source_name
        for source_name in _INSIGHT_SOURCE_ORDER
        if bool(source_enabled.get(source_name, True))
    ]


def _collect_recent_insight_feedback_examples(limit: int = _GLOBAL_INSIGHT_RULE_EXAMPLE_LIMIT) -> list[dict]:
    examples: list[dict] = []
    projects = list_projects()

    for project in projects:
        project_id = str(project.get("id") or "").strip()
        if not project_id:
            continue

        project_name = str(project.get("name") or "").strip() or "当前项目"
        try:
            insight_state = load_project_insight_state(project_id)
        except Exception:
            continue

        events_raw = insight_state.get("feedback_events", [])
        events = events_raw if isinstance(events_raw, list) else []

        for event in events:
            if not isinstance(event, dict):
                continue

            action = str(event.get("action") or "").strip().lower()
            if action not in {"useful", "not_relevant"}:
                continue

            card_id = str(event.get("card_id") or "").strip()
            if not card_id:
                continue

            event_feed_id = str(event.get("feed_id") or "").strip()
            card_snapshot = _extract_card_snapshot_from_state(
                insight_state,
                project_id,
                card_id,
                event_feed_id,
            )
            example_project_id = str(card_snapshot.get("project_id") or "").strip() or project_id
            example_project_name = str(card_snapshot.get("project_name") or "").strip() or _resolve_project_name(example_project_id)
            examples.append(
                {
                    "ts": str(event.get("ts") or "").strip(),
                    "action": action,
                    "project_id": example_project_id,
                    "project_name": example_project_name,
                    "feed_id": event_feed_id,
                    "card_id": card_id,
                    "title": _compact_optional_text(str(card_snapshot.get("title") or ""), max_chars=180),
                    "url": _compact_optional_text(str(card_snapshot.get("url") or ""), max_chars=240),
                    "source": _compact_optional_text(
                        str(card_snapshot.get("source") or card_snapshot.get("source_type") or ""),
                        max_chars=80,
                    ),
                    "source_type": _compact_optional_text(str(card_snapshot.get("source_type") or ""), max_chars=40),
                    "core_insight": _compact_optional_text(str(card_snapshot.get("core_insight") or ""), max_chars=220),
                    "risk_alert": _compact_optional_text(str(card_snapshot.get("risk_alert") or ""), max_chars=220),
                    "alternative": _compact_optional_text(str(card_snapshot.get("alternative") or ""), max_chars=220),
                    "relevance_reason": _compact_optional_text(
                        str(card_snapshot.get("relevance_reason") or ""),
                        max_chars=220,
                    ),
                    "evidence_snippet": _compact_optional_text(
                        str(card_snapshot.get("evidence_snippet") or ""),
                        max_chars=220,
                    ),
                }
            )

    examples.sort(key=lambda item: str(item.get("ts") or ""), reverse=True)
    safe_limit = max(1, int(limit or _GLOBAL_INSIGHT_RULE_EXAMPLE_LIMIT))
    return examples[:safe_limit]


def _default_global_insight_rule_template() -> str:
    return (
        "# Insight Recommendation Rule Memory\n\n"
        f"> Global preference memory for insight push recommendation. Auto-updated at {_now_iso()}.\n\n"
        "## 用户偏好规律\n"
        "- 暂无，等待用户反馈。\n\n"
        "## 喜欢的推送实例\n"
        "- 暂无。\n\n"
        "## 否定的推送实例\n"
        "- 暂无。\n\n"
        "## 最近反馈分析\n"
    )


def _read_global_insight_rule_markdown() -> str:
    try:
        if not _GLOBAL_INSIGHT_RULE_FILE.exists():
            return ""
        return _GLOBAL_INSIGHT_RULE_FILE.read_text(encoding="utf-8")
    except Exception:
        return ""


def _write_global_insight_rule_markdown(content: str) -> None:
    _GLOBAL_INSIGHT_RULE_FILE.parent.mkdir(parents=True, exist_ok=True)
    _GLOBAL_INSIGHT_RULE_FILE.write_text(str(content or "").rstrip() + "\n", encoding="utf-8")


def _load_global_insight_rules_for_prompt() -> str:
    rules_markdown = _read_global_insight_rule_markdown()
    if not rules_markdown.strip():
        return ""
    return _truncate_for_prompt(rules_markdown, _GLOBAL_INSIGHT_RULE_PROMPT_MAX_CHARS)


def _fallback_feedback_analysis_markdown(current_event: dict) -> str:
    action = str(current_event.get("action") or "").strip().lower()
    card = current_event.get("card", {}) if isinstance(current_event.get("card"), dict) else {}

    title = _safe_markdown_text(str(card.get("title") or current_event.get("card_id") or "N/A"), max_chars=180)
    source = _safe_markdown_text(str(card.get("source") or card.get("source_type") or "unknown"), max_chars=80)
    url = _safe_markdown_text(str(card.get("url") or "N/A"), max_chars=240)
    core_insight = _compact_optional_text(str(card.get("core_insight") or ""), max_chars=220)
    relevance_reason = _compact_optional_text(str(card.get("relevance_reason") or ""), max_chars=220)
    evidence_snippet = _compact_optional_text(str(card.get("evidence_snippet") or ""), max_chars=220)

    positive_reason = relevance_reason or core_insight or "与当前项目目标/任务的连接足够明确，且具备可执行价值。"
    negative_reason = relevance_reason or "卡片没有把来源内容映射到当前项目目标，相关性说明不足。"

    if action == "useful":
        preference_hint = (
            "用户更倾向于保留能明确说明“为什么与当前项目相关”的推送，"
            f"尤其是能把外部内容映射到当前任务/目标的卡片。本次高信号点：{positive_reason}"
        )
        liked_example = f"- {title} | 来源: {source} | 匹配原因: {positive_reason} | 链接: {url}"
        disliked_example = "- 暂无新增（待后续负反馈补充）。"
        reason_hint = "- 本次标记为“有用”，说明这张卡片不仅主题相近，还给出了足够具体的项目关联说明。"
    else:
        preference_hint = (
            "用户倾向于过滤没有说清楚与项目哪一环相关、或者只有表面关键词相似的推送。"
            f"本次负信号点：{negative_reason}"
        )
        liked_example = "- 暂无新增（待后续正反馈补充）。"
        disliked_example = f"- {title} | 来源: {source} | 不匹配原因: {negative_reason} | 链接: {url}"
        reason_hint = "- 本次标记为“不相关”，说明这张卡片缺少足够可信的项目关联信号。"

    if evidence_snippet:
        reason_hint += f" 证据摘录：{evidence_snippet}"

    return (
        "### 偏好规律增量\n"
        f"- {preference_hint}\n\n"
        "### 喜欢的推送实例增量\n"
        f"{liked_example}\n\n"
        "### 否定的推送实例增量\n"
        f"{disliked_example}\n\n"
        "### 本次反馈原因分析\n"
        f"{reason_hint}"
    )


def _analyze_feedback_rule_entry_with_llm(
    current_event: dict,
    recent_examples: list[dict],
    existing_rules_markdown: str,
) -> str:
    compact_rules = _truncate_for_prompt(existing_rules_markdown, 2800)
    examples_payload = recent_examples[:_GLOBAL_INSIGHT_RULE_EXAMPLE_LIMIT]

    prompt = (
        "你是 Insight 推送偏好分析器。请根据用户反馈生成可追加到 markdown 的增量分析。\n"
        "你的核心任务不是复述事件，而是从成功/失败案例里提炼会直接影响后续推送筛选和排序的用户偏好信号。\n\n"
        "优先关注这些维度：\n"
        "- 用户更喜欢/反感的主题或问题类型。\n"
        "- 内容是否明确说明了与当前项目、目标或任务的关系（relevance_reason）。\n"
        "- 内容是否提供了具体证据、执行启发、风险提醒，而不是泛泛描述。\n"
        "- 用户对 source/source_type、方法形态、信息密度、可落地性的隐含偏好。\n\n"
        "输出要求：\n"
        "1) 只输出 markdown，不要代码块。\n"
        "2) 必须包含且仅包含以下四个三级标题（每节至少 1 条 bullet）：\n"
        "### 偏好规律增量\n"
        "### 喜欢的推送实例增量\n"
        "### 否定的推送实例增量\n"
        "### 本次反馈原因分析\n"
        "3) “偏好规律增量”是最重要部分：优先输出 2-4 条高信号规律，每条都要落到可观察特征，明确写出后续筛选应该优先、降权还是过滤什么。\n"
        "4) 不要写空话，例如“更相关”“更高质量”“更有帮助”。必须说明具体是哪些信号导致用户喜欢或反感。\n"
        "5) 如果某条规律只有单个案例支撑，请标注为“待继续验证”；只有正负案例共同支持或多次重复出现时，才能把它写成稳定偏好。\n"
        "6) “喜欢/否定的推送实例增量”优先使用近期真实反馈样本，并尽量保留 title/source/url，再补一句“为何匹配/为何不匹配”，优先使用 relevance_reason、core_insight、evidence_snippet 中最具体的句子。\n"
        "7) “本次反馈原因分析”要指出当前卡片被判为有用/不相关的直接触发因素，点出最关键的 1-2 个信号。\n"
        "8) 若某类样本不足，请输出“暂无新增”。\n\n"
        f"当前反馈事件：{json.dumps(current_event, ensure_ascii=False)}\n"
        f"近期反馈样本：{json.dumps(examples_payload, ensure_ascii=False)}\n"
        f"已有规则摘要：{json.dumps(compact_rules, ensure_ascii=False)}"
    )

    response = call_llm_messages(
        [
            {
                "role": "system",
                "content": "你只输出 markdown 增量，不要解释，不要代码块。重点提炼可用于后续 insight 排序的用户偏好规律，避免空泛复述。",
            },
            {"role": "user", "content": prompt},
        ],
        inject_system_memory=False,
        trace_label="insight_feedback_rule",
    )
    return _strip_markdown_fence(response)


def _fallback_compact_global_rule_markdown(raw_markdown: str) -> str:
    cleaned = str(raw_markdown or "").strip()
    if not cleaned:
        return _default_global_insight_rule_template()
    if len(cleaned) <= _GLOBAL_INSIGHT_RULE_TARGET_CHARS:
        return cleaned

    marker = "\n\n## 历史内容（fallback 压缩）\n\n"
    remaining = max(400, _GLOBAL_INSIGHT_RULE_TARGET_CHARS - len(marker))
    head_chars = max(200, remaining // 2)
    tail_chars = max(200, remaining - head_chars)
    head = cleaned[:head_chars].rstrip()
    tail = cleaned[-tail_chars:].lstrip()

    return (
        "# Insight Recommendation Rule Memory\n\n"
        "> Fallback compaction applied because LLM compaction was unavailable.\n"
        f"\n{marker}{head}\n\n---\n\n{tail}"
    )


def _compress_global_insight_rule_markdown(raw_markdown: str) -> str:
    cleaned = str(raw_markdown or "").strip()
    if not cleaned:
        return _default_global_insight_rule_template()

    structured_examples = _collect_recent_insight_feedback_examples(
        limit=min(_GLOBAL_INSIGHT_RULE_EXAMPLE_LIMIT, 24)
    )

    prompt = (
        "你是 Insight 推荐规则压缩器。你的目标不是简单删字，而是把长文档重写成一个更短、但更强的“用户推送偏好模型”。\n"
        "这个结果会被直接注入后续 insight 筛选/排序 prompt，因此优先级如下：\n"
        "1) 优先提炼稳定的用户偏好规律，回答“用户到底喜欢什么样的推送、反感什么样的推送”。\n"
        "2) 其次保留最有判别力的正负案例，并写清它们为什么匹配/不匹配项目。\n"
        "3) 最后才是压缩字数和删除流水账。\n\n"
        "压缩要求：\n"
        "1) 仅输出 markdown，不要代码块。\n"
        f"2) 尽量控制在 {_GLOBAL_INSIGHT_RULE_TARGET_CHARS} 字符以内；如果信息过多，优先保留高价值规律和带具体理由的代表案例。\n"
        "3) 必须保留且仅保留这四个二级标题：\n"
        "## 用户偏好规律\n"
        "## 喜欢的推送实例\n"
        "## 否定的推送实例\n"
        "## 最近反馈分析（精简）\n"
        "4) “## 用户偏好规律”是最重要部分，至少输出 6 条高质量 bullet。每条尽量包含：\n"
        "   - 结论：用户偏好/反感/仍待观察的具体内容。\n"
        "   - 触发信号：主题、方法、source/source_type、relevance_reason、evidence_snippet、可执行性、风险提醒风格等可观察特征。\n"
        "   - 筛选动作：后续应优先推、降权还是过滤。\n"
        "   - 证据：引用最能支持该规律的案例标题或关键原因。\n"
        "5) 规律禁止空泛，例如“更相关”“更有价值”“更高质量”。必须说明具体喜欢/不喜欢什么。\n"
        "6) 只有当多个案例重复出现，或正负案例形成明显对照时，才写成稳定规律；单个案例只写成“待验证”观察。\n"
        "7) “## 喜欢的推送实例”和“## 否定的推送实例”各保留最有判别力的 3-6 条案例。每条必须包含 title/source/url，并尽量补一句“为何匹配/为何不匹配”，优先保留 relevance_reason、core_insight、evidence_snippet 中最具体的一句。\n"
        "8) “## 最近反馈分析（精简）”只保留最近真正影响排序策略的变化：新增强信号、冲突信号、待验证假设。不要逐条复述所有历史反馈。\n"
        "9) 去重时不要只按字面重复度裁剪，而要按“哪些信号更容易触发 useful / not_relevant”来归纳。\n"
        "10) 请把最可执行、最稳定的规律排在最前面，因为后续提示注入可能会截断。\n\n"
        f"近期结构化反馈样本：{json.dumps(structured_examples, ensure_ascii=False)}\n\n"
        f"原始文档：\n{cleaned}"
    )

    response = call_llm_messages(
        [
            {
                "role": "system",
                "content": "你是文档压缩助手，只输出 markdown。优先产出可用于后续 insight 排序的用户偏好规律，不要保留流水账式复述。",
            },
            {"role": "user", "content": prompt},
        ],
        inject_system_memory=False,
        trace_label="insight_rule_compact",
    )

    compacted = _strip_markdown_fence(response)
    if not compacted:
        return _fallback_compact_global_rule_markdown(cleaned)
    if len(compacted) > _GLOBAL_INSIGHT_RULE_TARGET_CHARS:
        compacted = _truncate_for_prompt(compacted, _GLOBAL_INSIGHT_RULE_TARGET_CHARS)
    return compacted


def _update_global_insight_rule_memory(
    project_id: str,
    card_id: str,
    action: str,
    feed_id: str,
    insight_state: dict,
) -> None:
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"useful", "not_relevant"}:
        return

    existing_markdown = _read_global_insight_rule_markdown().strip()
    if not existing_markdown:
        existing_markdown = _default_global_insight_rule_template().strip()

    if len(existing_markdown) >= _GLOBAL_INSIGHT_RULE_COMPRESS_TRIGGER_CHARS:
        try:
            existing_markdown = _compress_global_insight_rule_markdown(existing_markdown)
        except Exception:
            existing_markdown = _fallback_compact_global_rule_markdown(existing_markdown)

    card_snapshot = _extract_card_snapshot_from_state(insight_state, project_id, card_id, feed_id)
    event_project_id = str(card_snapshot.get("project_id") or "").strip() or str(project_id or "").strip()
    event_project_name = str(card_snapshot.get("project_name") or "").strip() or _resolve_project_name(event_project_id)
    action_label = "有用" if normalized_action == "useful" else "不相关"
    current_event = {
        "ts": _now_iso(),
        "action": normalized_action,
        "action_label": action_label,
        "project_id": event_project_id,
        "project_name": event_project_name,
        "feed_id": str(feed_id or "").strip(),
        "card_id": str(card_id or "").strip(),
        "card": card_snapshot,
    }

    recent_examples = _collect_recent_insight_feedback_examples(limit=_GLOBAL_INSIGHT_RULE_EXAMPLE_LIMIT)

    try:
        analysis_markdown = _analyze_feedback_rule_entry_with_llm(
            current_event,
            recent_examples,
            existing_markdown,
        )
    except Exception:
        analysis_markdown = ""

    if not analysis_markdown:
        analysis_markdown = _fallback_feedback_analysis_markdown(current_event)

    source_text = _safe_markdown_text(
        str(card_snapshot.get("source") or card_snapshot.get("source_type") or "unknown"),
        max_chars=80,
    )
    title_text = _safe_markdown_text(str(card_snapshot.get("title") or card_id), max_chars=180)
    url_text = _safe_markdown_text(str(card_snapshot.get("url") or "N/A"), max_chars=240)
    card_detail_lines = _format_insight_card_memory_lines(card_snapshot)
    card_detail_block = ""
    if card_detail_lines:
        card_detail_block = "\n" + "\n".join(card_detail_lines)

    entry = (
        f"\n\n### {current_event['ts']} | 项目：{current_event['project_name']} | 反馈：{action_label}\n"
        f"- card_id: {current_event['card_id']}\n"
        f"- 标题: {title_text}\n"
        f"- 来源: {source_text}\n"
        f"- 链接: {url_text}{card_detail_block}\n\n"
        f"{analysis_markdown.strip()}\n"
    )

    updated_markdown = existing_markdown.rstrip() + entry
    if len(updated_markdown) >= _GLOBAL_INSIGHT_RULE_COMPRESS_TRIGGER_CHARS:
        try:
            updated_markdown = _compress_global_insight_rule_markdown(updated_markdown)
        except Exception:
            updated_markdown = _fallback_compact_global_rule_markdown(updated_markdown)

    _write_global_insight_rule_markdown(updated_markdown)


def _update_global_insight_rule_memory_async(
    project_id: str,
    card_id: str,
    action: str,
    feed_id: str,
    insight_state: dict,
) -> None:
    normalized_project_id = str(project_id or "").strip()
    normalized_card_id = str(card_id or "").strip()
    normalized_action = str(action or "").strip().lower()
    normalized_feed_id = str(feed_id or "").strip()
    state_snapshot = dict(insight_state) if isinstance(insight_state, dict) else {}

    if not normalized_project_id or not normalized_card_id:
        return
    if normalized_action not in {"useful", "not_relevant"}:
        return

    def _worker() -> None:
        with _GLOBAL_INSIGHT_RULE_UPDATE_LOCK:
            _update_global_insight_rule_memory(
                normalized_project_id,
                normalized_card_id,
                normalized_action,
                normalized_feed_id,
                state_snapshot,
            )

    threading.Thread(target=_worker, daemon=True).start()


def _merge_keywords(keyword_groups: list[list[str]], limit: int = 20) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for group in keyword_groups:
        for keyword in group:
            normalized = str(keyword or "").strip()
            if not normalized:
                continue
            lowered = normalized.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            merged.append(normalized)
            if len(merged) >= limit:
                return merged
    return merged


def _build_weighted_project_contexts() -> list[dict]:
    now = datetime.now()
    contexts: list[dict] = []

    for project in list_projects():
        project_id = str(project.get("id") or "").strip()
        project_name = str(project.get("name") or "").strip() or "未命名项目"
        if not project_id:
            continue

        project_tasks = load_tasks(project_id)
        last_updated_at = _project_last_updated_at(project)
        days_since_update = max(0, (now.date() - last_updated_at.date()).days)
        recency_weight = math.exp(-float(days_since_update) / _INSIGHT_WEIGHT_DECAY_DAYS)

        contexts.append(
            {
                "project_id": project_id,
                "project_name": project_name,
                "tasks": project_tasks,
                "last_updated_at": last_updated_at,
                "days_since_update": days_since_update,
                "raw_weight": recency_weight,
            }
        )

    if not contexts:
        return []

    total_weight = sum(float(item.get("raw_weight", 0.0) or 0.0) for item in contexts)
    if total_weight <= 0:
        total_weight = float(len(contexts))

    for item in contexts:
        normalized_weight = float(item.get("raw_weight", 0.0) or 0.0) / total_weight
        candidate_limit = int(round(_INSIGHT_MIN_CANDIDATE_LIMIT + (normalized_weight * 12.0)))
        candidate_limit = max(_INSIGHT_MIN_CANDIDATE_LIMIT, min(_INSIGHT_MAX_CANDIDATE_LIMIT, candidate_limit))

        card_target = int(round(1 + normalized_weight * 2.0))
        card_target = max(1, min(3, card_target))

        item["weight"] = round(normalized_weight, 6)
        item["candidate_limit"] = candidate_limit
        item["card_target"] = card_target

    contexts.sort(
        key=lambda item: (
            float(item.get("weight", 0.0) or 0.0),
            -int(item.get("days_since_update", 0) or 0),
        ),
        reverse=True,
    )
    return contexts


def _collect_project_summary_snippets(project_id: str, project_name: str, tasks: list[dict]) -> list[dict]:
    try:
        summary_state = load_project_summary_state(project_id)
    except Exception:
        return []

    folder = str(summary_state.get("saved_summary_folder") or "").strip()
    if not folder:
        return []

    task_terms = [
        str(task.get("task") or "").strip()
        for task in tasks[:6]
        if isinstance(task, dict) and str(task.get("task") or "").strip()
    ]
    query = f"{project_name} 风险 失败 条件 技术 路线 进展 {' '.join(task_terms)}".strip()

    try:
        snippets = _pick_summary_snippets(query, folder, max_files=3, max_chars=600)
    except Exception:
        return []

    return snippets if isinstance(snippets, list) else []


def _source_breakdown_from_cards(cards: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for card in cards:
        if not isinstance(card, dict):
            continue
        source = str(card.get("source") or card.get("source_type") or "unknown").strip().lower() or "unknown"
        counts[source] = counts.get(source, 0) + 1
    return counts


def _clone_insight_feed_snapshot(feed: dict) -> dict | None:
    if not isinstance(feed, dict):
        return None

    raw_cards = feed.get("cards", [])
    cards = [dict(card) for card in raw_cards if isinstance(card, dict)]
    if not cards:
        return None

    raw_keywords = feed.get("keywords", [])
    keywords = [
        str(item or "").strip()
        for item in (raw_keywords if isinstance(raw_keywords, list) else [])
        if str(item or "").strip()
    ]
    keyword_extraction = (
        dict(feed.get("keyword_extraction", {}))
        if isinstance(feed.get("keyword_extraction"), dict)
        else {}
    )
    retrieval = feed.get("retrieval", {}) if isinstance(feed.get("retrieval"), dict) else {}

    return {
        "feed_id": str(feed.get("feed_id") or "").strip(),
        "date": str(feed.get("date") or "").strip(),
        "generated_at": str(feed.get("generated_at") or "").strip(),
        "cards": cards,
        "keywords": keywords,
        "keyword_extraction": keyword_extraction,
        "retrieval": dict(retrieval),
    }


def _insight_feed_sort_key(feed: dict | None) -> tuple[str, str, str]:
    if not isinstance(feed, dict):
        return ("", "", "")

    return (
        str(feed.get("date") or "").strip(),
        str(feed.get("generated_at") or "").strip(),
        str(feed.get("feed_id") or "").strip(),
    )


def _find_latest_workspace_insight_feed(*, target_date: str | None = None) -> dict | None:
    normalized_target_date = str(target_date or "").strip()
    best_feed: dict | None = None
    best_sort_key = ("", "")

    for project in list_projects():
        project_id = str(project.get("id") or "").strip()
        if not project_id:
            continue

        insight_state = load_project_insight_state(project_id)
        latest_feed = insight_state.get("latest_feed")
        if not isinstance(latest_feed, dict):
            continue

        feed_date = str(latest_feed.get("date") or "").strip()
        if normalized_target_date and feed_date != normalized_target_date:
            continue

        snapshot = _clone_insight_feed_snapshot(latest_feed)
        if not snapshot:
            continue

        sort_key = (
            str(snapshot.get("generated_at") or "").strip(),
            str(snapshot.get("feed_id") or "").strip(),
        )
        if best_feed is None or sort_key > best_sort_key:
            best_feed = snapshot
            best_sort_key = sort_key

    return best_feed


def _merge_saved_card_snapshot(saved_cards_by_id: dict[str, dict], snapshot: dict | None) -> bool:
    normalized = _normalize_saved_card_snapshot(snapshot or {}, str((snapshot or {}).get("project_id") or "").strip())
    if not normalized:
        return False

    card_id = normalized["card_id"]
    existing = saved_cards_by_id.get(card_id)
    if existing is None:
        saved_cards_by_id[card_id] = normalized
        return True

    existing_saved_at = str(existing.get("saved_at") or "").strip()
    candidate_saved_at = str(normalized.get("saved_at") or "").strip()
    if candidate_saved_at > existing_saved_at:
        merged = dict(normalized)
        for key, value in existing.items():
            if merged.get(key):
                continue
            if value:
                merged[key] = value
    else:
        merged = dict(existing)
        for key, value in normalized.items():
            if merged.get(key):
                continue
            if value:
                merged[key] = value

    changed = merged != existing
    if changed:
        saved_cards_by_id[card_id] = merged
    return changed


def _collect_saved_card_snapshots_from_state(insight_state: dict, fallback_project_id: str) -> list[dict]:
    saved_card_ids_raw = insight_state.get("saved_card_ids", []) if isinstance(insight_state, dict) else []
    saved_card_ids = [
        str(item).strip()
        for item in (saved_card_ids_raw if isinstance(saved_card_ids_raw, list) else [])
        if str(item).strip()
    ]
    saved_card_id_set = set(saved_card_ids)

    saved_cards_raw = insight_state.get("saved_cards", []) if isinstance(insight_state, dict) else []
    saved_cards_input = saved_cards_raw if isinstance(saved_cards_raw, list) else []
    saved_cards: list[dict] = []
    seen_card_ids: set[str] = set()

    for item in saved_cards_input:
        normalized = _normalize_saved_card_snapshot(item, fallback_project_id)
        if not normalized:
            continue

        card_id = normalized["card_id"]
        if card_id in seen_card_ids:
            continue
        if saved_card_id_set and card_id not in saved_card_id_set:
            continue

        seen_card_ids.add(card_id)
        saved_cards.append(normalized)

    for card_id in saved_card_ids:
        if card_id in seen_card_ids:
            continue

        snapshot = _extract_card_snapshot_from_state(insight_state, fallback_project_id, card_id)
        snapshot["saved_at"] = ""
        normalized = _normalize_saved_card_snapshot(snapshot, fallback_project_id)
        if not normalized:
            continue

        seen_card_ids.add(card_id)
        saved_cards.append(normalized)

    saved_cards.sort(key=lambda item: str(item.get("saved_at") or ""), reverse=True)
    return saved_cards[:300]


def _sync_workspace_saved_cards(workspace_state: dict | None = None) -> dict:
    current_workspace_state = (
        dict(workspace_state)
        if isinstance(workspace_state, dict)
        else load_workspace_insight_state()
    )

    saved_cards_by_id: dict[str, dict] = {}
    changed = False

    for snapshot in _collect_saved_card_snapshots_from_state(current_workspace_state, _WORKSPACE_INSIGHT_PROJECT_ID):
        _merge_saved_card_snapshot(saved_cards_by_id, snapshot)

    for project in list_projects():
        project_id = str(project.get("id") or "").strip()
        if not project_id:
            continue

        project_state = load_project_insight_state(project_id)
        local_snapshots = _collect_saved_card_snapshots_from_state(project_state, project_id)
        if not local_snapshots:
            continue

        for snapshot in local_snapshots:
            if _merge_saved_card_snapshot(saved_cards_by_id, snapshot):
                changed = True

        project_state["saved_card_ids"] = []
        project_state["saved_cards"] = []
        save_project_insight_state(project_id, project_state)
        changed = True

    merged_saved_cards = list(saved_cards_by_id.values())
    merged_saved_cards.sort(key=lambda item: str(item.get("saved_at") or ""), reverse=True)
    merged_saved_cards = merged_saved_cards[:300]
    merged_saved_card_ids = [
        str(item.get("card_id") or "").strip()
        for item in merged_saved_cards
        if str(item.get("card_id") or "").strip()
    ]

    if (
        current_workspace_state.get("saved_card_ids") != merged_saved_card_ids
        or current_workspace_state.get("saved_cards") != merged_saved_cards
    ):
        current_workspace_state["saved_card_ids"] = merged_saved_card_ids
        current_workspace_state["saved_cards"] = merged_saved_cards
        changed = True

    if changed:
        return save_workspace_insight_state(current_workspace_state)
    return current_workspace_state


def _sync_workspace_today_feed(workspace_state: dict | None = None) -> dict:
    current_workspace_state = (
        dict(workspace_state)
        if isinstance(workspace_state, dict)
        else load_workspace_insight_state()
    )

    today = _today_iso()
    best_feed = None
    best_sort_key = ("", "", "")

    workspace_latest_feed = current_workspace_state.get("latest_feed")
    if isinstance(workspace_latest_feed, dict) and str(workspace_latest_feed.get("date") or "").strip() == today:
        best_feed = _clone_insight_feed_snapshot(workspace_latest_feed)
        best_sort_key = _insight_feed_sort_key(best_feed)

    for project in list_projects():
        project_id = str(project.get("id") or "").strip()
        if not project_id:
            continue

        project_state = load_project_insight_state(project_id)
        latest_feed = project_state.get("latest_feed")
        if not isinstance(latest_feed, dict):
            continue
        if str(latest_feed.get("date") or "").strip() != today:
            continue

        snapshot = _clone_insight_feed_snapshot(latest_feed)
        if not snapshot:
            continue

        sort_key = _insight_feed_sort_key(snapshot)
        if best_feed is None or sort_key > best_sort_key:
            best_feed = snapshot
            best_sort_key = sort_key

    if not isinstance(best_feed, dict):
        return current_workspace_state

    current_sort_key = _insight_feed_sort_key(current_workspace_state.get("latest_feed"))
    current_feed_date = str((current_workspace_state.get("latest_feed") or {}).get("date") or "").strip()
    should_update = (
        current_feed_date != today
        or current_sort_key != best_sort_key
        or str(current_workspace_state.get("last_generated_on") or "").strip() != today
    )
    if not should_update:
        return current_workspace_state

    feeds_raw = current_workspace_state.get("feeds", [])
    feeds = feeds_raw if isinstance(feeds_raw, list) else []
    synced_feeds = [
        item
        for item in feeds
        if isinstance(item, dict) and str(item.get("date") or "").strip() != today
    ]
    synced_feeds.insert(0, best_feed)

    current_workspace_state["last_generated_on"] = today
    current_workspace_state["latest_feed"] = best_feed
    current_workspace_state["feeds"] = synced_feeds[:30]
    current_workspace_state["pending_notification"] = False
    return save_workspace_insight_state(current_workspace_state)


def _load_synced_workspace_insight_state() -> dict:
    workspace_state = load_workspace_insight_state()
    workspace_state = _sync_workspace_saved_cards(workspace_state)
    workspace_state = _sync_workspace_today_feed(workspace_state)
    return workspace_state


def _build_effective_insight_state(project_id: str) -> dict:
    local_state = load_project_insight_state(project_id)
    workspace_state = _load_synced_workspace_insight_state()
    effective_state = dict(local_state)

    workspace_latest_feed = workspace_state.get("latest_feed")
    if isinstance(workspace_latest_feed, dict) and str(workspace_latest_feed.get("date") or "").strip() == _today_iso():
        effective_state["last_generated_on"] = _today_iso()
        effective_state["latest_feed"] = workspace_latest_feed
        effective_state["feeds"] = workspace_state.get("feeds", [])
        effective_state["pending_notification"] = bool(workspace_state.get("pending_notification"))

    effective_state["saved_card_ids"] = workspace_state.get("saved_card_ids", [])
    effective_state["saved_cards"] = workspace_state.get("saved_cards", [])
    return effective_state


def _find_insight_card_snapshot_across_workspace(
    card_id: str,
    feed_id: str = "",
    preferred_project_id: str = "",
) -> dict:
    normalized_card_id = str(card_id or "").strip()
    normalized_feed_id = str(feed_id or "").strip()
    normalized_preferred_project_id = str(preferred_project_id or "").strip()

    search_targets: list[tuple[dict, str]] = []
    workspace_state = _load_synced_workspace_insight_state()
    search_targets.append((workspace_state, normalized_preferred_project_id or _WORKSPACE_INSIGHT_PROJECT_ID))

    if normalized_preferred_project_id:
        search_targets.append((load_project_insight_state(normalized_preferred_project_id), normalized_preferred_project_id))

    for project in list_projects():
        project_id = str(project.get("id") or "").strip()
        if not project_id or project_id == normalized_preferred_project_id:
            continue
        search_targets.append((load_project_insight_state(project_id), project_id))

    for insight_state, fallback_project_id in search_targets:
        snapshot = _extract_card_snapshot_from_state(
            insight_state,
            fallback_project_id,
            normalized_card_id,
            normalized_feed_id,
        )
        if any(
            str(snapshot.get(field) or "").strip()
            for field in ("title", "url", "source", "core_insight", "risk_alert", "alternative")
        ):
            return snapshot

    fallback_project_id = normalized_preferred_project_id or _WORKSPACE_INSIGHT_PROJECT_ID
    return _extract_card_snapshot_from_state({}, fallback_project_id, normalized_card_id, normalized_feed_id)


def load_insight_state(project_id: str) -> dict:
    return _build_effective_insight_state(project_id)


def sync_project_insight_today_state(project_id: str) -> dict:
    _load_synced_workspace_insight_state()
    return load_insight_state(project_id)


def load_insight_settings(project_id: str) -> dict:
    return load_workspace_insight_settings()


def save_insight_settings(project_id: str, *, enabled: bool, source_enabled: dict) -> dict:
    return save_workspace_insight_settings(
        enabled=enabled,
        source_enabled=source_enabled,
    )


def update_saved_insight_card_annotation(project_id: str, card_id: str, annotation: str) -> dict:
    normalized_project_id = str(project_id or "").strip()
    normalized_card_id = str(card_id or "").strip()
    if not normalized_project_id or not normalized_card_id:
        return {
            "status": "error",
            "message": "缺少项目 ID 或卡片 ID。",
            "insight_state": load_insight_state(project_id),
        }

    workspace_state = _load_synced_workspace_insight_state()
    saved_cards = _collect_saved_card_snapshots_from_state(workspace_state, _WORKSPACE_INSIGHT_PROJECT_ID)
    saved_cards_by_id: dict[str, dict] = {
        str(item.get("card_id") or "").strip(): dict(item)
        for item in saved_cards
        if str(item.get("card_id") or "").strip()
    }

    if normalized_card_id not in saved_cards_by_id:
        return {
            "status": "error",
            "message": "这张卡片尚未保存，无法编辑批注。",
            "insight_state": load_insight_state(project_id),
        }

    updated_annotation = str(annotation or "").strip()
    saved_card = dict(saved_cards_by_id[normalized_card_id])
    saved_card["annotation"] = updated_annotation
    saved_cards_by_id[normalized_card_id] = saved_card

    merged_saved_cards = list(saved_cards_by_id.values())
    merged_saved_cards.sort(key=lambda item: str(item.get("saved_at") or ""), reverse=True)
    workspace_state["saved_card_ids"] = [
        str(item.get("card_id") or "").strip()
        for item in merged_saved_cards
        if str(item.get("card_id") or "").strip()
    ][:300]
    workspace_state["saved_cards"] = merged_saved_cards[:300]
    save_workspace_insight_state(workspace_state)

    return {
        "status": "ok",
        "message": "批注已更新。" if updated_annotation else "已清空批注。",
        "insight_state": load_insight_state(project_id),
        "card_id": normalized_card_id,
        "annotation": updated_annotation,
    }


def list_saved_insight_cards(project_id: str) -> dict:
    workspace_state = _load_synced_workspace_insight_state()
    saved_cards = _collect_saved_card_snapshots_from_state(workspace_state, _WORKSPACE_INSIGHT_PROJECT_ID)

    return {
        "project_id": project_id,
        "count": len(saved_cards),
        "saved_cards": saved_cards,
        "insight_state": load_insight_state(project_id),
    }


def generate_project_insight_feed(project_id: str, *, trigger_mode: str = "manual") -> dict:
    insight_settings = load_workspace_insight_settings()
    existing_state = load_project_insight_state(project_id)
    enabled_sources = _enabled_insight_sources(insight_settings)

    if not bool(insight_settings.get("enabled", True)):
        return {
            "status": "disabled",
            "message": "Insight 功能已关闭，未生成新的 insight card。",
            "feed": existing_state.get("latest_feed"),
            "insight_state": existing_state,
            "trigger_mode": trigger_mode,
            "insight_settings": insight_settings,
        }

    if not enabled_sources:
        return {
            "status": "disabled",
            "message": "Insight 信息来源均已关闭，未生成新的 insight card。",
            "feed": existing_state.get("latest_feed"),
            "insight_state": existing_state,
            "trigger_mode": trigger_mode,
            "insight_settings": insight_settings,
        }

    weighted_projects = _build_weighted_project_contexts()
    if not weighted_projects:
        return {
            "status": "error",
            "message": "Insight 生成失败：当前没有可用于检索的项目。",
            "feed": None,
            "insight_state": load_project_insight_state(project_id),
            "trigger_mode": trigger_mode,
        }

    historical_url_keys = get_seen_insight_url_keys(before_date=_today_iso())
    dedup_url_keys: set[str] = set(historical_url_keys)
    recommendation_rules_markdown = _load_global_insight_rules_for_prompt()

    aggregated_cards: list[dict] = []
    source_errors: list[str] = []
    query_parts: list[str] = []
    keyword_groups: list[list[str]] = []

    for project_ctx in weighted_projects:
        project_summary_snippets = _collect_project_summary_snippets(
            str(project_ctx.get("project_id") or ""),
            str(project_ctx.get("project_name") or ""),
            project_ctx.get("tasks", []),
        )

        generation_result = generate_insight_feed(
            str(project_ctx.get("project_name") or ""),
            project_ctx.get("tasks", []),
            card_count=int(project_ctx.get("card_target", 1) or 1),
            candidate_limit=int(project_ctx.get("candidate_limit", _INSIGHT_MIN_CANDIDATE_LIMIT) or _INSIGHT_MIN_CANDIDATE_LIMIT),
            exclude_url_keys=dedup_url_keys,
            project_summary_snippets=project_summary_snippets,
            recommendation_rules_markdown=recommendation_rules_markdown,
            enabled_sources=enabled_sources,
        )

        if generation_result.get("status") != "ok":
            message = str(generation_result.get("message") or "insight 生成失败")
            source_errors.append(f"{project_ctx.get('project_name')}: {message}")
            continue

        feed = generation_result.get("feed")
        if not isinstance(feed, dict):
            source_errors.append(f"{project_ctx.get('project_name')}: 缺少有效 feed")
            continue

        retrieval = feed.get("retrieval", {}) if isinstance(feed.get("retrieval"), dict) else {}
        query_text = str(retrieval.get("query") or "").strip()
        if query_text:
            query_parts.append(f"{project_ctx.get('project_name')}[{int(project_ctx.get('candidate_limit', 0) or 0)}]={query_text}")

        raw_keywords = feed.get("keywords", [])
        if isinstance(raw_keywords, list):
            keyword_groups.append([str(item or "").strip() for item in raw_keywords if str(item or "").strip()])

        raw_cards = feed.get("cards", [])
        if not isinstance(raw_cards, list):
            continue

        for card in raw_cards:
            if not isinstance(card, dict):
                continue

            card_url = str(card.get("url") or "").strip()
            card_url_key = _canonical_url_key(card_url)
            if card_url_key and card_url_key in dedup_url_keys:
                continue

            if card_url_key:
                dedup_url_keys.add(card_url_key)

            enriched_card = dict(card)
            enriched_card["project_id"] = str(project_ctx.get("project_id") or "").strip()
            enriched_card["project_name"] = str(project_ctx.get("project_name") or "").strip()
            enriched_card["project_weight"] = float(project_ctx.get("weight", 0.0) or 0.0)
            aggregated_cards.append(enriched_card)

    if not aggregated_cards:
        detail = f"（{source_errors[0]}）" if source_errors else ""
        return {
            "status": "error",
            "message": f"Insight 生成失败：未获得新的去重卡片{detail}",
            "feed": None,
            "insight_state": load_project_insight_state(project_id),
            "trigger_mode": trigger_mode,
        }

    def _card_rank_score(card: dict) -> float:
        try:
            relevance = float(card.get("relevance_score", 0.0) or 0.0)
        except (TypeError, ValueError):
            relevance = 0.0
        try:
            project_weight = float(card.get("project_weight", 0.0) or 0.0)
        except (TypeError, ValueError):
            project_weight = 0.0
        return relevance * (1.0 + project_weight)

    aggregated_cards.sort(key=_card_rank_score, reverse=True)
    selected_cards = aggregated_cards[:_INSIGHT_CARD_COUNT]

    feed_date = _today_iso()
    feed_id = f"feed_{feed_date}_{uuid.uuid4().hex[:8]}"

    keywords = _merge_keywords(keyword_groups, limit=20)
    if not keywords:
        keywords = [str(item.get("project_name") or "").strip() for item in weighted_projects if str(item.get("project_name") or "").strip()][:10]

    feed = {
        "feed_id": feed_id,
        "date": feed_date,
        "generated_at": _now_iso(),
        "cards": selected_cards,
        "keywords": keywords,
        "retrieval": {
            "source": "multi_project_weighted",
            "query": " | ".join(query_parts[:12]),
            "candidate_count": len(aggregated_cards),
            "source_breakdown": _source_breakdown_from_cards(selected_cards),
        },
    }

    shown_urls = [str(card.get("url") or "").strip() for card in selected_cards if str(card.get("url") or "").strip()]
    if shown_urls:
        record_shown_insight_urls(
            shown_urls,
            seen_on=feed_date,
            project_id=project_id,
            feed_id=feed_id,
        )

    insight_state = load_project_insight_state(project_id)
    feed_date = str(feed.get("date") or _today_iso()).strip() or _today_iso()

    feeds_raw = insight_state.get("feeds", [])
    feeds = feeds_raw if isinstance(feeds_raw, list) else []
    feeds = [
        item
        for item in feeds
        if isinstance(item, dict) and str(item.get("date") or "").strip() != feed_date
    ]
    feeds.insert(0, feed)

    insight_state["last_generated_on"] = feed_date
    insight_state["latest_feed"] = feed
    insight_state["feeds"] = feeds[:30]
    insight_state["pending_notification"] = str(trigger_mode or "manual").strip().lower() == "auto_daily"

    saved_state = save_project_insight_state(project_id, insight_state)

    workspace_state = _load_synced_workspace_insight_state()
    workspace_feeds_raw = workspace_state.get("feeds", [])
    workspace_feeds = workspace_feeds_raw if isinstance(workspace_feeds_raw, list) else []
    workspace_feeds = [
        item
        for item in workspace_feeds
        if isinstance(item, dict) and str(item.get("date") or "").strip() != feed_date
    ]
    workspace_feeds.insert(0, feed)
    workspace_state["last_generated_on"] = feed_date
    workspace_state["latest_feed"] = feed
    workspace_state["feeds"] = workspace_feeds[:30]
    workspace_state["pending_notification"] = str(trigger_mode or "manual").strip().lower() == "auto_daily"
    save_workspace_insight_state(workspace_state)

    message = "今日 insight 已生成（按项目更新时间加权检索）。"
    if source_errors:
        message += "（部分项目候选获取失败，已自动跳过）"
    if len(selected_cards) < _INSIGHT_CARD_COUNT:
        message += "（可用去重候选不足，卡片数量少于 3）"

    return {
        "status": "ok",
        "message": message,
        "feed": saved_state.get("latest_feed"),
        "insight_state": saved_state,
        "trigger_mode": trigger_mode,
    }


def ensure_daily_insight_feed(project_id: str) -> dict:
    workspace_state = _load_synced_workspace_insight_state()
    today = _today_iso()
    latest_feed = workspace_state.get("latest_feed")
    if workspace_state.get("last_generated_on") == today and isinstance(latest_feed, dict):
        return {
            "status": "skipped",
            "message": "今日 insight 已存在。",
            "feed": latest_feed,
            "insight_state": load_insight_state(project_id),
            "trigger_mode": "auto_daily",
        }

    return generate_project_insight_feed(project_id, trigger_mode="auto_daily")


def mark_insight_notification_seen(project_id: str) -> dict:
    workspace_state = _load_synced_workspace_insight_state()
    if not workspace_state.get("pending_notification"):
        return load_insight_state(project_id)

    workspace_state["pending_notification"] = False
    save_workspace_insight_state(workspace_state)
    return load_insight_state(project_id)


def record_insight_feedback(
    project_id: str,
    card_id: str,
    action: str,
    feed_id: str = "",
    annotation: str = "",
) -> dict:
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"useful", "not_relevant", "save", "unsave", "deep_dive"}:
        return {
            "status": "error",
            "message": "不支持的反馈动作。",
            "insight_state": load_insight_state(project_id),
        }

    normalized_card_id = str(card_id or "").strip()
    if not normalized_card_id:
        return {
            "status": "error",
            "message": "缺少卡片 ID。",
            "insight_state": load_insight_state(project_id),
        }

    normalized_feed_id = str(feed_id or "").strip()
    normalized_annotation = ""
    event_ts = _now_iso()

    card_snapshot = _find_insight_card_snapshot_across_workspace(
        normalized_card_id,
        normalized_feed_id,
        project_id,
    )
    event_project_id = str(card_snapshot.get("project_id") or "").strip() or str(project_id or "").strip()
    if normalized_action == "save":
        normalized_annotation = str(annotation or "").strip()

    workspace_state = _load_synced_workspace_insight_state()
    workspace_saved_cards = _collect_saved_card_snapshots_from_state(workspace_state, _WORKSPACE_INSIGHT_PROJECT_ID)
    workspace_saved_cards_by_id: dict[str, dict] = {
        str(item.get("card_id") or "").strip(): dict(item)
        for item in workspace_saved_cards
        if str(item.get("card_id") or "").strip()
    }

    if normalized_action == "save":
        card_snapshot["saved_at"] = event_ts
        card_snapshot["annotation"] = normalized_annotation
        normalized_snapshot = _normalize_saved_card_snapshot(card_snapshot, event_project_id)
        if normalized_snapshot:
            if not normalized_snapshot.get("feed_id"):
                normalized_snapshot["feed_id"] = normalized_feed_id
            workspace_saved_cards_by_id[normalized_card_id] = normalized_snapshot
    elif normalized_action == "unsave":
        workspace_saved_cards_by_id.pop(normalized_card_id, None)

    merged_saved_cards = list(workspace_saved_cards_by_id.values())
    merged_saved_cards.sort(key=lambda item: str(item.get("saved_at") or ""), reverse=True)
    workspace_state["saved_card_ids"] = [
        str(item.get("card_id") or "").strip()
        for item in merged_saved_cards
        if str(item.get("card_id") or "").strip()
    ][:300]
    workspace_state["saved_cards"] = merged_saved_cards[:300]
    workspace_saved_state = save_workspace_insight_state(workspace_state)

    event_state = load_project_insight_state(event_project_id)
    feedback_events_raw = event_state.get("feedback_events", [])
    feedback_events = feedback_events_raw if isinstance(feedback_events_raw, list) else []
    feedback_events.append(
        {
            "action": normalized_action,
            "card_id": normalized_card_id,
            "feed_id": normalized_feed_id,
            "project_id": event_project_id,
            "ts": event_ts,
        }
    )
    event_state["feedback_events"] = feedback_events[-500:]
    event_saved_state = save_project_insight_state(event_project_id, event_state)

    rule_update_error = ""
    if normalized_action in {"useful", "not_relevant"}:
        try:
            _update_global_insight_rule_memory_async(
                event_project_id,
                normalized_card_id,
                normalized_action,
                str(feed_id or "").strip(),
                workspace_saved_state,
            )
        except Exception as exc:
            rule_update_error = str(exc)

    message = "反馈已记录。"
    if normalized_action in {"useful", "not_relevant"}:
        if rule_update_error:
            message += "（偏好规则更新失败，已自动跳过）"
        else:
            message += "（assistant 已收到你的意见，偏好规则将在后台更新）"

    return {
        "status": "ok",
        "message": message,
        "insight_state": load_insight_state(project_id),
        "rule_update_error": rule_update_error,
        "event_state": event_saved_state,
    }


def list_all_projects():
    return list_projects()


def create_new_project(name: str) -> dict:
    return create_project(name)


def delete_existing_project(project_id: str) -> None:
    delete_project(project_id)


def load_saved_risk_threshold(project_id: str) -> tuple[int, bool]:
    return get_project_risk_lag_threshold(project_id)


def load_saved_plan_total_weeks(project_id: str) -> tuple[int, bool]:
    return get_project_plan_total_weeks(project_id)


def save_plan_total_weeks(project_id: str, total_weeks: int) -> int:
    return save_project_plan_total_weeks(project_id, total_weeks)


def load_saved_gantt_theme(project_id: str) -> tuple[str, bool]:
    return get_project_gantt_theme(project_id)


def save_gantt_theme(project_id: str, theme: str) -> str:
    return save_project_gantt_theme(project_id, theme)


def load_gantt_theme_options() -> dict[str, dict]:
    return list_gantt_themes()


def render_gantt(tasks: list[dict], objective: str = "", theme: str = "default"):
    return create_gantt(tasks, objective=objective, theme=theme)


def render_graph(tasks: list[dict], dependencies: list[dict]):
    return create_dependency_graph(tasks, dependencies)


def update_tasks_from_summaries(
    project_id: str,
    summaries_folder: str,
    seen_filenames: set | None = None,
    risk_lag_threshold: int | None = None,
) -> dict:
    tasks = load_tasks(project_id)
    dependencies = load_dependencies(project_id)
    saved_threshold, _ = get_project_risk_lag_threshold(project_id)
    effective_threshold = saved_threshold
    if risk_lag_threshold is not None:
        effective_threshold = save_project_risk_lag_threshold(project_id, risk_lag_threshold)

    if not tasks:
        return {
            "status": "no_tasks",
            "message": "当前项目暂无任务，无法从总结更新进度。",
        }

    normalized_folder = normalize_summary_folder_path(summaries_folder)
    folder_path = Path(normalized_folder) if normalized_folder else None
    if folder_path is None or not folder_path.exists() or not folder_path.is_dir():
        return {
            "status": "invalid_summaries_folder",
            "message": "指定路径不存在，或该路径不是文件夹。",
            "summaries_count": 0,
            "skipped_filenames": [],
            "new_filenames": [],
        }

    all_pairs = load_summaries_with_names(normalized_folder)
    if not all_pairs:
        return {
            "status": "no_summaries",
            "message": "指定文件夹下没有可读取的总结文件。",
            "summaries_count": 0,
            "skipped_filenames": [],
            "new_filenames": [],
        }

    # Accept both historical Windows-style and POSIX-style relative names.
    seen: set[str] = {
        str(name).replace("\\", "/")
        for name in (seen_filenames or [])
        if name
    }
    skipped_filenames = [name for name, _ in all_pairs if name in seen]
    new_pairs = [(name, content) for name, content in all_pairs if name not in seen]
    new_filenames = [name for name, _ in new_pairs]
    summaries = [content for _, content in new_pairs]

    if not summaries:
        return {
            "status": "no_new_summaries",
            "message": "文件夹中所有文件均已在上次更新中读取过，没有新的总结文件。",
            "summaries_count": 0,
            "skipped_filenames": skipped_filenames,
            "new_filenames": [],
        }

    result = estimate_progress_from_summaries(tasks, summaries, summary_names=new_filenames)
    updated_tasks = result.get("updated_tasks", tasks)
    aggregated_updates = result.get("aggregated_updates", [])

    risk_result = predict_risk(updated_tasks, current_state={"risk_lag_threshold": effective_threshold})
    updated_tasks_with_risk = risk_result.get("tasks", updated_tasks)
    risk_items = risk_result.get("risk_items", [])

    save_tasks(updated_tasks_with_risk, project_id, dependencies=dependencies)

    old_task_by_id = {
        str(task.get("task_id") or ""): {
            "progress": task.get("progress", 0),
            "status": task.get("status", "Planned"),
        }
        for task in tasks
    }
    changed_count = 0
    for task in updated_tasks_with_risk:
        tid = str(task.get("task_id") or "")
        before = old_task_by_id.get(tid, {"progress": 0, "status": "Planned"})
        if (
            before.get("progress") != task.get("progress", 0)
            or before.get("status") != task.get("status", "Planned")
        ):
            changed_count += 1

    return {
        "status": "ok",
        "message": "已根据团队总结更新任务状态。",
        "read_at": _now_iso(),
        "summaries_count": len(summaries),
        "updates_count": len(aggregated_updates),
        "changed_tasks_count": changed_count,
        "aggregated_updates": aggregated_updates,
        "per_summary_updates": result.get("per_summary_updates", []),
        "risk_count": int(risk_result.get("risk_count", 0) or 0),
        "risk_items": risk_items,
        "risk_lag_threshold": int(risk_result.get("risk_lag_threshold", 30) or 30),
        "risk_checked_on": risk_result.get("today", ""),
        "new_filenames": new_filenames,
        "skipped_filenames": skipped_filenames,
    }


_ASSISTANT_ALLOWED_FIELDS = {"progress", "status", "owner", "duration_days", "task", "start_week", "end_week"}
_ASSISTANT_ALLOWED_STATUSES = {"Planned", "In Progress", "Done", "At Risk"}
_ALLOWED_DEP_TYPES = {"FS", "SS", "FF", "SF"}
_MEMORY_HINT_TOKENS = (
    "记住", "记一下", "记下来", "偏好", "习惯", "以后", "别名", "术语", "简称",
    "默认", "我们这里", "这个项目里", "稳定事实", "请记住","的意思是","下次",
)

_REPLAN_HINT_TOKENS = (
    "不满意", "重做", "重新规划", "重新生成", "改一下计划", "调整计划", "优化计划",
    "replan", "regenerate", "revise plan", "update plan",
)

_GLOBAL_RISK_INTENT_EXACT = "我今天最应该关注的三件事是什么"
_GLOBAL_RISK_INTENT_ALIASES = (
    "最近我有什么问题需要格外关注吗",
    "最近有什么问题需要格外关注吗",
    "最近我有什么问题需要关注吗",
    "最近有什么问题需要关注吗",
    "最近我需要格外关注什么问题",
    "最近有哪些问题需要格外关注",
    "今天最值得我关注的三件事是什么",
    "最近最值得我关注的三件事是什么",
    "最近所有项目里最需要优先处理的风险有哪些",
)
_GLOBAL_RISK_TOP_ITEMS = 3
_GLOBAL_RISK_MIN_ITEMS = 1
_GLOBAL_RISK_MAX_ITEMS = 8
_GLOBAL_RISK_RECENT_SUMMARY_FILES = 5
_GLOBAL_RISK_SUMMARY_MAX_CHARS = 2000
_GLOBAL_RISK_SUMMARY_MAX_LINES = 60
_GLOBAL_RISK_SUMMARY_LLM_MAX_OUTPUT_TOKENS = 700
_GLOBAL_RISK_SUMMARY_SCAN_DEFAULT_WORKERS = 3
_GLOBAL_RISK_SUMMARY_SCAN_WORKERS_HARD_CAP = 16
_GLOBAL_RISK_SUMMARY_MAX_ISSUES_PER_SUMMARY = 3
_GLOBAL_RISK_PROJECT_SUMMARY_ISSUE_LIMIT = 3
_SUMMARY_ISSUE_KEYWORDS = (
    "问题", "瓶颈", "阻塞", "卡住", "延期", "延误", "延迟", "风险", "失败", "故障",
    "困难", "无法", "缺少", "冲突", "报错", "返工", "backlog", "blocker", "blocked",
    "bottleneck", "delay", "risk", "issue", "error", "fail", "bug",
)
_SUMMARY_ISSUE_STRONG_KEYWORDS = (
    "瓶颈", "阻塞", "卡住", "卡点", "延期", "延误", "失败", "故障", "无法", "报错", "尝试无果",
    "blocker", "blocked", "bottleneck", "delay", "error", "fail", "bug",
)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _coerce_chat_message_ts(raw_ts: str | None) -> str:
    text = str(raw_ts or "").strip()
    return text or _now_iso()


def _resolve_assistant_llm_model() -> str:
    try:
        cfg = load_llm_config()
    except Exception:
        return ""

    assistant_model = str(cfg.get("assistant_model") or "").strip()
    if assistant_model:
        return assistant_model
    return str(cfg.get("model") or "").strip()


def _resolve_global_risk_summary_scan_workers() -> int:
    try:
        cfg = load_llm_config()
    except Exception:
        return _GLOBAL_RISK_SUMMARY_SCAN_DEFAULT_WORKERS

    raw_value = cfg.get("progress_max_workers", _GLOBAL_RISK_SUMMARY_SCAN_DEFAULT_WORKERS)
    try:
        workers = int(raw_value)
    except (TypeError, ValueError):
        return _GLOBAL_RISK_SUMMARY_SCAN_DEFAULT_WORKERS

    return max(1, min(_GLOBAL_RISK_SUMMARY_SCAN_WORKERS_HARD_CAP, workers))


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


def _compact_tasks(tasks: list[dict], limit: int = 40) -> list[dict]:
    compact: list[dict] = []
    for task in tasks[:limit]:
        compact.append(
            {
                "task_id": task.get("task_id", ""),
                "task": task.get("task", ""),
                "owner": task.get("owner", ""),
                "status": task.get("status", "Planned"),
                "progress": int(task.get("progress", 0) or 0),
                "duration_days": int(task.get("duration_days", 7) or 7),
                "start_week": task.get("start_week"),
                "start": str(task.get("start", "")),
                "end": str(task.get("end", "")),
            }
        )
    return compact


def _format_schedule_window(start_week: int | None, end_week: int | None = None, duration_days: int | None = None) -> str:
    if start_week is None:
        return ""

    try:
        normalized_start = max(1, int(start_week))
    except (TypeError, ValueError):
        return ""

    normalized_end = None
    if end_week is not None:
        try:
            normalized_end = max(normalized_start, int(end_week))
        except (TypeError, ValueError):
            normalized_end = None
    elif duration_days is not None:
        try:
            weeks = max(1, (int(duration_days) + 6) // 7)
            normalized_end = normalized_start + weeks - 1
        except (TypeError, ValueError):
            normalized_end = None

    if normalized_end is None:
        return f"W{normalized_start}"
    return f"W{normalized_start}-W{normalized_end}"


def _updates_schedule_window_label(updates: dict) -> str:
    if not isinstance(updates, dict):
        return ""
    return _format_schedule_window(
        updates.get("start_week"),
        updates.get("end_week"),
        updates.get("duration_days"),
    )


def _format_task_update_items(display_updates: dict) -> list[str]:
    if not isinstance(display_updates, dict):
        return []

    items: list[str] = []
    schedule_label = _updates_schedule_window_label(display_updates)
    if schedule_label:
        items.append(f"schedule={schedule_label}")

    for key, value in display_updates.items():
        if key in {"start_week", "end_week"}:
            continue
        items.append(f"{key}={value}")

    return items


def _has_schedule_update_fields(updates: dict) -> bool:
    if not isinstance(updates, dict):
        return False
    return any(key in updates for key in {"duration_days", "start_week", "end_week"})


def _pick_summary_snippets(question: str, summaries_folder: str, max_files: int = 10, max_chars: int = 1200) -> list[dict]:
    return find_summary_snippets(
        folder=summaries_folder,
        question=question,
        max_files=max_files,
        max_chars=min(max_chars, 700),
    )


def _normalize_intent_text(text: str) -> str:
    normalized = str(text or "").strip().lower()
    normalized = re.sub(r"[\s\u3000]+", "", normalized)
    normalized = re.sub(r"[，。！？!?；;:“”\"'`·、（）()【】\[\]<>《》]", "", normalized)
    return normalized


def _is_global_risk_focus_request(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False

    # Keep explicit task-level commands out of global risk intent.
    if re.search(r"(?:\bT\d+\b|\bKR\d+\b|\d+\.\d+)", raw, flags=re.IGNORECASE):
        return False

    normalized = _normalize_intent_text(raw)
    if not normalized:
        return False

    exact_normalized = _normalize_intent_text(_GLOBAL_RISK_INTENT_EXACT)
    if exact_normalized and exact_normalized in normalized:
        return True

    for alias in _GLOBAL_RISK_INTENT_ALIASES:
        alias_normalized = _normalize_intent_text(alias)
        if alias_normalized and alias_normalized in normalized:
            return True

    has_global_scope = any(
        token in normalized
        for token in ("今天", "今日", "最近", "全局", "跨项目", "所有项目", "整体")
    )
    has_focus = any(token in normalized for token in ("关注", "优先", "重点", "重要"))
    has_priority = any(token in normalized for token in ("最应该", "最重要", "优先级", "格外", "需要","值得"))
    has_issue = any(token in normalized for token in ("问题", "风险", "瓶颈", "阻塞", "卡点", "隐患"))
    has_question = any(token in normalized for token in ("什么", "哪些", "吗", "么"))
    has_count = bool(re.search(r"(?:top\d+|[一二两三四五六七八九十\d]+件事)", normalized, flags=re.IGNORECASE))

    if has_global_scope and has_focus and has_priority and has_count:
        return True

    if has_global_scope and has_issue and has_focus and has_question:
        return True

    if has_global_scope and has_focus and has_question and ("最应该" in normalized or "格外关注" in normalized):
        return True

    return False


def _extract_global_risk_top_items(text: str, default: int = _GLOBAL_RISK_TOP_ITEMS) -> int:
    safe_default = max(_GLOBAL_RISK_MIN_ITEMS, min(_GLOBAL_RISK_MAX_ITEMS, int(default or _GLOBAL_RISK_TOP_ITEMS)))
    raw = str(text or "").strip()
    if not raw:
        return safe_default

    top_match = re.search(r"top\s*([1-9]\d?)", raw, flags=re.IGNORECASE)
    if top_match:
        try:
            requested = int(top_match.group(1))
        except (TypeError, ValueError):
            requested = safe_default
        return max(_GLOBAL_RISK_MIN_ITEMS, min(_GLOBAL_RISK_MAX_ITEMS, requested))

    count_match = re.search(r"([一二两三四五六七八九十\d]+)\s*(?:件|个)\s*(?:事|事项)", raw)
    if count_match:
        token = str(count_match.group(1) or "").strip()
        requested = _parse_cn_number(token)
        if requested is not None:
            return max(_GLOBAL_RISK_MIN_ITEMS, min(_GLOBAL_RISK_MAX_ITEMS, requested))

    return safe_default


def _parse_iso_date(value: str) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _task_ref_label(task: dict) -> str:
    task_id = str(task.get("task_id") or "").strip() or "Unknown"
    try:
        kr_idx = int(task.get("kr_index"))
        sub_idx = int(task.get("subtask_index"))
    except (TypeError, ValueError):
        return task_id

    if kr_idx > 0 and sub_idx > 0:
        return f"{kr_idx}.{sub_idx}"
    return task_id


def _extract_summary_issue_signals(content: str, *, max_items: int = 2) -> list[str]:
    text = str(content or "").replace("\r", "\n")
    if not text.strip():
        return []

    segments = re.split(r"[\n。！？!?；;]+", text)
    signals: list[str] = []
    seen: set[str] = set()

    for segment in segments:
        line = re.sub(r"\s+", " ", str(segment or "")).strip(" -\t")
        if len(line) < 6:
            continue

        lowered = line.lower()
        if not any(keyword in line for keyword in _SUMMARY_ISSUE_KEYWORDS):
            if not any(keyword in lowered for keyword in _SUMMARY_ISSUE_KEYWORDS):
                continue

        compact_line = _safe_markdown_text(line, max_chars=120)
        if not compact_line or compact_line in seen:
            continue

        seen.add(compact_line)
        signals.append(compact_line)
        if len(signals) >= max_items:
            break

    return signals


def _compact_summary_for_llm(content: str, *, max_chars: int = _GLOBAL_RISK_SUMMARY_MAX_CHARS, max_lines: int = _GLOBAL_RISK_SUMMARY_MAX_LINES) -> str:
    raw = str(content or "")
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    if not lines:
        return ""

    if len(lines) > max_lines:
        head_count = max(6, max_lines // 2)
        tail_count = max(6, max_lines - head_count)
        lines = [
            *lines[:head_count],
            "...[truncated lines]...",
            *lines[-tail_count:],
        ]

    merged = "\n".join(lines).strip()
    if len(merged) <= max_chars:
        return merged

    marker = "\n...[truncated content]...\n"
    available = max_chars - len(marker)
    if available <= 40:
        return merged[:max_chars]

    head_chars = max(80, int(available * 0.6))
    tail_chars = max(80, available - head_chars)
    return merged[:head_chars].rstrip() + marker + merged[-tail_chars:].lstrip()


def _summary_source_hint(rel_filename: str) -> str:
    normalized = str(rel_filename or "").replace("\\", "/").strip("/")
    if not normalized:
        return "未知来源"

    parts = [part for part in normalized.split("/") if part]
    if not parts:
        return "未知来源"

    owner = Path(parts[-1]).stem
    owner = re.sub(r"[-_ ]?(?:w|W)\d+(?:-\d+)?$", "", owner).strip()
    owner = re.sub(r"第?\d+周", "", owner).strip()
    owner = owner or Path(parts[-1]).stem

    if len(parts) >= 2:
        return f"{parts[0]}/{owner}"
    return owner


def _load_recent_summary_snapshots(folder: str, *, max_files: int = _GLOBAL_RISK_RECENT_SUMMARY_FILES) -> list[dict]:
    folder_text = normalize_summary_folder_path(folder)
    if not folder_text:
        return []

    folder_path = Path(folder_text)
    if not folder_path.exists() or not folder_path.is_dir():
        return []

    summary_pairs = load_summaries_with_names(folder_text)
    if not summary_pairs:
        return []

    ranked: list[tuple[datetime | None, str, str]] = []
    for rel_filename, content in summary_pairs:
        rel_name = str(rel_filename or "").replace("\\", "/").strip()
        if not rel_name:
            continue

        file_path = folder_path / Path(rel_name)
        modified_at: datetime | None = None
        try:
            modified_at = datetime.fromtimestamp(file_path.stat().st_mtime)
        except (OSError, OverflowError, ValueError):
            modified_at = None

        ranked.append((modified_at, rel_name, content))

    ranked.sort(
        key=lambda item: (item[0] or datetime.min, item[1]),
        reverse=True,
    )

    snapshots: list[dict] = []
    for modified_at, rel_name, content in ranked[: max(1, int(max_files or 1))]:
        content_excerpt = _compact_summary_for_llm(
            content,
            max_chars=_GLOBAL_RISK_SUMMARY_MAX_CHARS,
            max_lines=_GLOBAL_RISK_SUMMARY_MAX_LINES,
        )
        snapshots.append(
            {
                "filename": rel_name,
                "source_hint": _summary_source_hint(rel_name),
                "modified_at": modified_at.isoformat(timespec="seconds") if modified_at else "",
                "content_excerpt": content_excerpt,
                "issue_signals": _extract_summary_issue_signals(
                    content,
                    max_items=_GLOBAL_RISK_SUMMARY_MAX_ISSUES_PER_SUMMARY,
                ),
            }
        )

    return snapshots


def _summary_issue_score(signal: str) -> int:
    text = str(signal or "").strip().lower()
    if not text:
        return 0

    if any(keyword in text for keyword in _SUMMARY_ISSUE_STRONG_KEYWORDS):
        return 28
    return 0


def _summary_issue_sort_key(item: dict) -> tuple[int, str, str, str]:
    try:
        score = int(item.get("score", 0) or 0)
    except (TypeError, ValueError):
        score = 0
    return (
        score,
        str(item.get("modified_at") or ""),
        str(item.get("filename") or ""),
        str(item.get("signal") or ""),
    )


def _summary_issue_dedupe_key(signal: str) -> str:
    normalized = _normalize_intent_text(signal)
    normalized = re.sub(r"\d+", "", normalized)
    return normalized[:120]


def _dedupe_summary_issue_candidates(candidates: list[dict], *, limit: int) -> list[dict]:
    ordered = sorted(candidates, key=_summary_issue_sort_key, reverse=True)
    deduped: list[dict] = []
    seen_keys: set[str] = set()

    for candidate in ordered:
        key = _summary_issue_dedupe_key(str(candidate.get("signal") or ""))
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append(candidate)
        if len(deduped) >= max(1, int(limit or 1)):
            break

    return deduped


def _pick_summary_issues_by_keywords(
    summary_snapshots: list[dict],
    *,
    max_items: int = _GLOBAL_RISK_PROJECT_SUMMARY_ISSUE_LIMIT,
) -> list[dict]:
    candidates: list[dict] = []

    for snapshot in summary_snapshots:
        if not isinstance(snapshot, dict):
            continue
        signals = snapshot.get("issue_signals", [])
        if not isinstance(signals, list):
            continue

        for signal in signals:
            issue_text = _safe_markdown_text(str(signal or ""), max_chars=120)
            if not issue_text:
                continue

            issue_score = _summary_issue_score(issue_text)
            if issue_score <= 0:
                continue

            candidates.append(
                {
                    "type": "difficulty",
                    "signal": issue_text,
                    "score": 44 + issue_score,
                    "source_hint": str(snapshot.get("source_hint") or "").strip() or "最近总结",
                    "filename": str(snapshot.get("filename") or "").strip(),
                    "modified_at": str(snapshot.get("modified_at") or "").strip(),
                    "reason": "",
                    "support_action": "",
                }
            )

    return _dedupe_summary_issue_candidates(candidates, limit=max_items)


def _llm_pick_summary_issues(
    project_name: str,
    tasks: list[dict],
    summary_snapshots: list[dict],
    *,
    max_items: int = _GLOBAL_RISK_PROJECT_SUMMARY_ISSUE_LIMIT,
) -> list[dict]:
    payload: list[dict] = []
    for snapshot in summary_snapshots:
        if not isinstance(snapshot, dict):
            continue

        excerpt = str(snapshot.get("content_excerpt") or "").strip()
        if not excerpt:
            continue

        payload.append(
            {
                "filename": str(snapshot.get("filename") or "").strip(),
                "source_hint": str(snapshot.get("source_hint") or "").strip() or "最近总结",
                "modified_at": str(snapshot.get("modified_at") or "").strip(),
                "content_excerpt": excerpt,
                "issue_signals": snapshot.get("issue_signals", []),
            }
        )

    if not payload:
        return []

    tasks_context = _compact_tasks(tasks, limit=12)
    prompt_prefix = (
        "你是资深项目风险与困难审阅助手。请阅读一份工作总结，判断是否存在\"需要今天优先关注\"的风险信号或团队遇到的具体困难。\n\n"
        "判断要求：\n"
        "1) 风险包括阻塞、延期、失败、资源冲突、关键依赖受影响等。\n"
        "2) 困难包括进展受阻、瓶颈、尝试无果、技术难题、团队成员遇到的具体问题等。\n"
        "3) 明确写出\"瓶颈\"、\"阻塞\"、\"无法推进\"、\"多次尝试无果\"、\"关键依赖失败\" 的问题，优先级高于一般性的沟通/同步/待确认事项。\n"
        "4) 普通进展更新、已解决事项、轻微待确认项，不应判定为今日高优先级困难。\n"
        f"5) 同一份总结最多返回 {_GLOBAL_RISK_SUMMARY_MAX_ISSUES_PER_SUMMARY} 条问题，按严重度从高到低排序；若没有明确风险或困难，返回空数组。\n"
        "6) 输出必须是 JSON，不要 markdown。\n\n"
        "输出格式：\n"
        "{\n"
        "  \"issues\": [\n"
        "    {\n"
        "      \"type\": \"risk|difficulty\",\n"
        "      \"severity\": 0-100,\n"
        "      \"signal\": \"一句话风险或困难描述\",\n"
        "      \"reason\": \"简短说明原因\",\n"
        "      \"support_action\": \"建议动作（简短，若为困难请加‘可以考虑及时关心或提供支持’）\",\n"
        "      \"source_filename\": \"命中的文件名\",\n"
        "      \"source_hint\": \"部门/人名提示，可空\",\n"
        "      \"owner_hint\": \"例如 算法部门/Alice，可空\"\n"
        "    }\n"
        "  ]\n"
        "}\n\n"
        f"项目名：{project_name}\n"
        f"项目任务上下文（节选）：{json.dumps(tasks_context, ensure_ascii=False)}\n\n"
    )
    assistant_model = _resolve_assistant_llm_model()

    def _scan_one_summary(summary_item: dict, scan_index: int) -> list[dict]:
        prompt = (
            prompt_prefix
            + f"工作总结（按修改时间倒序，节选）：{json.dumps(summary_item, ensure_ascii=False)}"
        )

        try:
            raw = call_llm_messages(
                [
                    {"role": "system", "content": "你只输出 JSON，不要解释。"},
                    {"role": "user", "content": prompt},
                ],
                inject_system_memory=False,
                max_tokens_override=_GLOBAL_RISK_SUMMARY_LLM_MAX_OUTPUT_TOKENS,
                model_override=assistant_model,
                trace_label=f"global_risk_summary_scan_{scan_index}",
            )
        except Exception:
            return []

        parsed = _safe_parse_json(raw)
        if not parsed:
            return []

        issues_raw = parsed.get("issues")
        if not isinstance(issues_raw, list):
            has_issue_raw = parsed.get("has_issue")
            if has_issue_raw is None:
                has_issue_raw = parsed.get("has_risk")
            if isinstance(has_issue_raw, bool):
                has_issue = has_issue_raw
            else:
                has_issue = str(has_issue_raw or "").strip().lower() in {"true", "1", "yes", "y", "是"}
            issues_raw = [parsed] if has_issue else []

        candidates: list[dict] = []
        for issue_item in issues_raw[:_GLOBAL_RISK_SUMMARY_MAX_ISSUES_PER_SUMMARY]:
            if not isinstance(issue_item, dict):
                continue

            signal = _safe_markdown_text(
                str(issue_item.get("signal") or issue_item.get("risk_signal") or ""),
                max_chars=120,
            )
            if signal == "N/A":
                continue

            try:
                severity = max(0, min(100, int(issue_item.get("severity", 60) or 60)))
            except (TypeError, ValueError):
                severity = 60

            source_filename_raw = str(issue_item.get("source_filename") or issue_item.get("filename") or "").replace("\\", "/").strip()
            source_hint_raw = _compact_optional_text(str(issue_item.get("source_hint") or ""), max_chars=60)
            owner_hint_raw = _compact_optional_text(str(issue_item.get("owner_hint") or ""), max_chars=60)
            reason = _compact_optional_text(str(issue_item.get("reason") or ""), max_chars=120)
            support_action = _compact_optional_text(str(issue_item.get("support_action") or ""), max_chars=80)
            issue_type = str(issue_item.get("type") or "difficulty").strip().lower() or "difficulty"

            selected_snapshot: dict | None = None
            if source_filename_raw:
                source_filename_lower = source_filename_raw.lower()
                for snapshot in summary_snapshots:
                    filename = str(snapshot.get("filename") or "").replace("\\", "/").strip()
                    filename_lower = filename.lower()
                    if not filename_lower:
                        continue
                    if filename_lower == source_filename_lower or filename_lower.endswith("/" + source_filename_lower):
                        selected_snapshot = snapshot
                        break

            if selected_snapshot is None:
                selected_snapshot = summary_item
            if selected_snapshot is None and summary_snapshots:
                selected_snapshot = summary_snapshots[0]

            final_source_hint = owner_hint_raw or source_hint_raw
            if not final_source_hint and isinstance(selected_snapshot, dict):
                final_source_hint = str(selected_snapshot.get("source_hint") or "").strip()
            if not final_source_hint:
                final_source_hint = "最近总结"

            final_filename = source_filename_raw
            if not final_filename and isinstance(selected_snapshot, dict):
                final_filename = str(selected_snapshot.get("filename") or "").strip()

            final_modified_at = ""
            if isinstance(selected_snapshot, dict):
                final_modified_at = str(selected_snapshot.get("modified_at") or "").strip()

            candidates.append(
                {
                    "type": issue_type,
                    "signal": signal,
                    "score": 20 + int(round(severity * 0.7)) + _summary_issue_score(signal),
                    "source_hint": final_source_hint,
                    "filename": final_filename,
                    "modified_at": final_modified_at,
                    "reason": reason,
                    "support_action": support_action,
                }
            )

        return candidates

    max_workers = _resolve_global_risk_summary_scan_workers()
    worker_count = min(len(payload), max_workers)
    candidates: list[dict] = []

    if worker_count <= 1:
        for index, summary_item in enumerate(payload):
            candidates.extend(_scan_one_summary(summary_item, index))
    else:
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="summary-risk-scan") as pool:
            futures = {
                pool.submit(_scan_one_summary, summary_item, index): index
                for index, summary_item in enumerate(payload)
            }
            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception:
                    continue
                if isinstance(result, list):
                    candidates.extend([item for item in result if isinstance(item, dict)])

    if not candidates:
        return []

    return _dedupe_summary_issue_candidates(candidates, limit=max_items)


def _format_short_time(raw_value: str) -> str:
    dt = _parse_iso_datetime(raw_value)
    if dt is None:
        return ""
    return dt.strftime("%m-%d %H:%M")


def _pick_near_due_task(tasks: list[dict]) -> dict | None:
    today = date.today()
    candidates: list[tuple[date, int, dict]] = []

    for task in tasks:
        if not isinstance(task, dict):
            continue

        status = str(task.get("status") or "").strip()
        if status == "Done":
            continue

        due_date = _parse_iso_date(str(task.get("end") or ""))
        if due_date is None:
            continue

        try:
            progress = max(0, min(100, int(task.get("progress", 0) or 0)))
        except (TypeError, ValueError):
            progress = 0

        candidates.append((due_date, progress, task))

    if not candidates:
        return None

    candidates.sort(key=lambda item: (item[0], item[1]))
    due_date, progress, task = candidates[0]
    days_left = (due_date - today).days
    return {
        "task": task,
        "progress": progress,
        "due_date": due_date.isoformat(),
        "days_left": days_left,
    }


def _build_global_risk_focus_reply(*, top_items: int = _GLOBAL_RISK_TOP_ITEMS) -> str:
    projects = list_projects()
    if not projects:
        return "当前还没有项目数据，暂时无法做全局风险排查。"

    findings: list[dict] = []
    scanned_projects = 0

    for project in projects:
        project_id = str(project.get("id") or "").strip()
        project_name = str(project.get("name") or "").strip() or "未命名项目"
        if not project_id:
            continue

        scanned_projects += 1
        tasks = load_tasks(project_id)
        dependencies = load_dependencies(project_id)
        task_by_id = {
            str(task.get("task_id") or "").strip(): task
            for task in tasks
            if str(task.get("task_id") or "").strip()
        }

        threshold, _ = get_project_risk_lag_threshold(project_id)
        risk_result = predict_risk(tasks, current_state={"risk_lag_threshold": threshold})
        risk_items_raw = risk_result.get("risk_items", [])
        risk_items = risk_items_raw if isinstance(risk_items_raw, list) else []
        risk_items.sort(key=lambda item: int(item.get("lag_percent", 0) or 0), reverse=True)
        top_risk = risk_items[0] if risk_items else None

        summary_state = load_project_summary_state(project_id)
        summary_folder = str(summary_state.get("saved_summary_folder") or "").strip()
        recent_summary_snapshots = _load_recent_summary_snapshots(
            summary_folder,
            max_files=_GLOBAL_RISK_RECENT_SUMMARY_FILES,
        )
        summary_issues = _llm_pick_summary_issues(
            project_name,
            tasks,
            recent_summary_snapshots,
            max_items=_GLOBAL_RISK_PROJECT_SUMMARY_ISSUE_LIMIT,
        )
        if not summary_issues:
            summary_issues = _pick_summary_issues_by_keywords(
                recent_summary_snapshots,
                max_items=_GLOBAL_RISK_PROJECT_SUMMARY_ISSUE_LIMIT,
            )

        detail_lines: list[str] = [f"“{project_name}”项目"]
        score = 0

        if isinstance(top_risk, dict):
            risk_task_id = str(top_risk.get("task_id") or "").strip()
            risk_task = task_by_id.get(risk_task_id, {})
            task_ref = _task_ref_label(risk_task) if risk_task else (risk_task_id or "Unknown")
            task_name = str(top_risk.get("task") or risk_task.get("task") or "未命名任务").strip()

            try:
                expected_progress = max(0, min(100, int(top_risk.get("expected_progress", 0) or 0)))
            except (TypeError, ValueError):
                expected_progress = 0
            try:
                actual_progress = max(0, min(100, int(top_risk.get("actual_progress", 0) or 0)))
            except (TypeError, ValueError):
                actual_progress = 0
            try:
                lag_percent = max(0, int(top_risk.get("lag_percent", 0) or 0))
            except (TypeError, ValueError):
                lag_percent = 0

            downstream_labels: list[str] = []
            for dep in dependencies:
                if not isinstance(dep, dict):
                    continue
                source = str(dep.get("from") or "").strip()
                target = str(dep.get("to") or "").strip()
                if source != risk_task_id or not target or target not in task_by_id:
                    continue

                target_task = task_by_id[target]
                target_ref = _task_ref_label(target_task)
                target_name = str(target_task.get("task") or target).strip()
                downstream_labels.append(f"{target_ref}（{target_name}）")
                if len(downstream_labels) >= 2:
                    break

            if downstream_labels:
                impact_sentence = (
                    "如果不尽快完成，可能影响下游依赖任务 "
                    + "、".join(downstream_labels)
                    + " 的进度。"
                )
            else:
                impact_sentence = "如果不尽快处理，可能推迟对应 KR 的完成节奏。"

            detail_lines.append(
                f"任务风险：任务 {task_ref}（{task_name}）应完成 {expected_progress}%、实际 {actual_progress}%（落后 {lag_percent}%），{impact_sentence}"
            )
            score += lag_percent + (12 if downstream_labels else 6)

        if summary_issues:
            detail_lines.append("团队遇到的困难：")
            issue_scores: list[int] = []
            for issue in summary_issues[:_GLOBAL_RISK_PROJECT_SUMMARY_ISSUE_LIMIT]:
                issue_text = _safe_markdown_text(str(issue.get("signal") or ""), max_chars=120)
                if issue_text == "N/A":
                    continue

                source_hint = str(issue.get("source_hint") or "").strip() or "最近总结"
                modified_label = _format_short_time(str(issue.get("modified_at") or ""))
                summary_ref = f"{source_hint}，{modified_label}" if modified_label else source_hint
                support_action = _compact_optional_text(str(issue.get("support_action") or ""), max_chars=80)
                reason = _compact_optional_text(str(issue.get("reason") or ""), max_chars=120)
                issue_type = str(issue.get("type") or "difficulty").strip().lower()
                if not support_action:
                    if issue_type == "risk":
                        support_action = "建议今天确认根因、责任人和缓解动作。"
                    else:
                        support_action = "可以考虑及时关心或提供支持，并明确下一步缓解动作。"

                if reason:
                    support_tail = f"{support_action}（依据：{reason}）"
                else:
                    support_tail = support_action

                detail_lines.append(
                    f"- {summary_ref}：{issue_text}。{support_tail}"
                )
                try:
                    issue_scores.append(int(issue.get("score", 0) or 0))
                except (TypeError, ValueError):
                    issue_scores.append(0)

            if issue_scores:
                score += issue_scores[0]
                for extra_score in issue_scores[1:]:
                    score += max(6, int(round(extra_score * 0.35)))

        if len(detail_lines) == 1:
            near_due = _pick_near_due_task(tasks)
            if isinstance(near_due, dict):
                focus_task = near_due.get("task", {})
                if isinstance(focus_task, dict):
                    task_ref = _task_ref_label(focus_task)
                    task_name = str(focus_task.get("task") or "未命名任务").strip()
                    progress = int(near_due.get("progress", 0) or 0)
                    due_date = str(near_due.get("due_date") or "").strip()
                    days_left = int(near_due.get("days_left", 9999) or 9999)
                    due_note = f"距截止约 {days_left} 天" if days_left >= 0 else f"已超期 {abs(days_left)} 天"
                    detail_lines.append(
                        f"临期任务：任务 {task_ref}（{task_name}）计划于 {due_date} 截止，当前进度 {progress}%（{due_note}），建议今天确认资源与依赖准备。"
                    )
                    score += max(3, 18 - min(max(days_left, -30), 30))

        if len(detail_lines) == 1:
            continue

        findings.append(
            {
                "project_id": project_id,
                "project_name": project_name,
                "score": score,
                "lines": detail_lines,
            }
        )

    if not findings:
        return (
            f"我已扫描 {scanned_projects} 个项目及其最近总结，当前没有识别到明显高风险事项。"
            "建议优先检查未来两周到期但进度低于 50% 的任务。"
        )

    findings.sort(key=lambda item: float(item.get("score", 0.0) or 0.0), reverse=True)

    selected: list[dict] = []
    selected_project_ids: set[str] = set()
    try:
        requested_count = int(top_items)
    except (TypeError, ValueError):
        requested_count = _GLOBAL_RISK_TOP_ITEMS
    target_count = max(_GLOBAL_RISK_MIN_ITEMS, min(_GLOBAL_RISK_MAX_ITEMS, requested_count))

    for item in findings:
        project_id = str(item.get("project_id") or "")
        if project_id in selected_project_ids:
            continue
        selected.append(item)
        selected_project_ids.add(project_id)
        if len(selected) >= target_count:
            break

    if len(selected) < target_count:
        for item in findings:
            if item in selected:
                continue
            selected.append(item)
            if len(selected) >= target_count:
                break

    lines = ["基于全局任务状态和各项目最近总结，今天最该优先关注的是："]
    for idx, item in enumerate(selected, start=1):
        detail_lines = item.get("lines", []) if isinstance(item, dict) else []
        if not isinstance(detail_lines, list) or not detail_lines:
            continue

        first_line = str(detail_lines[0] or "").strip()
        lines.append(f"{idx}. {first_line}")
        for detail in detail_lines[1:]:
            detail_text = str(detail or "").strip()
            if detail_text:
                lines.append(f"   {detail_text}")

    lines.append("")
    lines.append(
        f"（已扫描 {scanned_projects} 个项目；每个项目最多读取最近 {_GLOBAL_RISK_RECENT_SUMMARY_FILES} 份总结文件。）"
    )
    if len(selected) < target_count:
        lines.append(f"当前可确认的高优先关注项为 {len(selected)} 条。")

    return "\n".join(lines)

_TASK_REF_CORE_PATTERN = r"(?:T\d+|\d+\.\d+)"
_TASK_REF_TOKEN_PATTERN = r"((?:(?:第\s*)?(?:任务|task)\s*)?(?:T\d+|\d+\.\d+)(?:\s*(?:个)?(?:任务|task))?)"


def _normalize_task_ref_text(ref: str | None) -> str:
    normalized = str(ref or "").strip()
    if not normalized:
        return ""

    normalized = normalized.strip("：:，,。；;（）()[]【】")
    for _ in range(3):
        updated = normalized
        updated = re.sub(r"^(?:第\s*)?(?:任务|task)\s*", "", updated, flags=re.IGNORECASE)
        updated = re.sub(r"^第\s*", "", updated)
        updated = re.sub(r"\s*(?:个)?(?:任务|task)\s*$", "", updated, flags=re.IGNORECASE)
        updated = updated.strip().strip("：:，,。；;（）()[]【】")
        if updated == normalized:
            break
        normalized = updated

    compact = re.sub(r"\s+", "", normalized)
    if re.fullmatch(r"\d+\.\d+", compact):
        return compact
    if re.fullmatch(r"(?i)T\d+", compact):
        return compact.upper()
    return normalized


def _parse_display_ref(ref: str) -> tuple[int, int] | None:
    raw = _normalize_task_ref_text(ref)
    match = re.match(r"^(\d+)\.(\d+)$", raw)
    if not match:
        return None
    try:
        return int(match.group(1)), int(match.group(2))
    except (TypeError, ValueError):
        return None


def _task_display_ref_map(tasks: list[dict]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for task in tasks:
        task_id = str(task.get("task_id", "")).strip()
        if not task_id:
            continue
        try:
            kr_idx = int(task.get("kr_index"))
            sub_idx = int(task.get("subtask_index"))
        except (TypeError, ValueError):
            continue
        if kr_idx <= 0 or sub_idx <= 0:
            continue
        mapping[f"{kr_idx}.{sub_idx}"] = task_id
    return mapping


def _resolve_task_id_ref(tasks: list[dict], ref: str | None) -> str:
    raw = str(ref or "").strip()
    if not raw:
        return ""

    normalized_raw = _normalize_task_ref_text(raw)
    lowered_candidates = {raw.lower()}
    if normalized_raw:
        lowered_candidates.add(normalized_raw.lower())
    for task in tasks:
        task_id = str(task.get("task_id", "")).strip()
        if task_id.lower() in lowered_candidates:
            return task_id

    parsed = _parse_display_ref(raw)
    if parsed:
        mapped = _task_display_ref_map(tasks).get(f"{parsed[0]}.{parsed[1]}")
        if mapped:
            return mapped

    return normalized_raw or raw


def _find_task_index(tasks: list[dict], task_id: str | None, task_name: str | None) -> int:
    resolved_tid = _resolve_task_id_ref(tasks, task_id)
    tid = str(resolved_tid or "").strip().lower()
    name = str(task_name or "").strip().lower()

    if tid:
        for index, task in enumerate(tasks):
            if str(task.get("task_id", "")).strip().lower() == tid:
                return index

    if name:
        exact_matches = [
            i for i, task in enumerate(tasks)
            if str(task.get("task", "")).strip().lower() == name
        ]
        if len(exact_matches) == 1:
            return exact_matches[0]

        fuzzy_matches = [
            i for i, task in enumerate(tasks)
            if name in str(task.get("task", "")).strip().lower()
        ]
        if len(fuzzy_matches) == 1:
            return fuzzy_matches[0]

    return -1


def _sanitize_update_fields(fields: dict) -> tuple[dict, str | None]:
    if not isinstance(fields, dict):
        return {}, "字段格式无效。"

    cleaned: dict = {}
    for key, value in fields.items():
        normalized_key = "task" if key in {"task_name", "name", "title"} else key
        if normalized_key not in _ASSISTANT_ALLOWED_FIELDS:
            continue

        if normalized_key == "progress":
            try:
                cleaned[normalized_key] = max(0, min(100, int(value)))
            except (TypeError, ValueError):
                return {}, "progress 必须是 0-100 的整数。"
        elif normalized_key == "duration_days":
            try:
                cleaned[normalized_key] = max(1, int(value))
            except (TypeError, ValueError):
                return {}, "duration_days 必须是正整数。"
        elif normalized_key in {"start_week", "end_week"}:
            try:
                cleaned[normalized_key] = max(1, int(value))
            except (TypeError, ValueError):
                return {}, f"{normalized_key} 必须是大于等于 1 的整数。"
        elif normalized_key == "status":
            status = str(value or "").strip()
            if status not in _ASSISTANT_ALLOWED_STATUSES:
                return {}, "status 仅支持 Planned / In Progress / Done / At Risk。"
            cleaned[normalized_key] = status
        elif normalized_key == "owner":
            cleaned[normalized_key] = str(value or "").strip() or "Unassigned"
        elif normalized_key == "task":
            task_name = str(value or "").strip()
            if not task_name:
                return {}, "任务名称不能为空。"
            cleaned[normalized_key] = task_name

    if not cleaned:
        return {}, "未识别到可修改字段（支持 task/progress/status/owner/duration_days/start_week/end_week）。"

    return cleaned, None


def _apply_task_updates(task: dict, updates: dict) -> tuple[dict, str | None]:
    if not isinstance(updates, dict):
        return {}, "字段格式无效。"

    remaining_updates = dict(updates)
    duration_days_value = remaining_updates.pop("duration_days", None) if "duration_days" in remaining_updates else None
    start_week_value = remaining_updates.pop("start_week", None) if "start_week" in remaining_updates else None
    end_week_value = remaining_updates.pop("end_week", None) if "end_week" in remaining_updates else None

    if end_week_value is not None:
        if start_week_value is None:
            if duration_days_value is None:
                return {}, "end_week 需要与 start_week 一起提供，或同时给出 duration_days。"
            duration_weeks = max(1, (int(duration_days_value) + 6) // 7)
            start_week_value = max(1, int(end_week_value) - duration_weeks + 1)
        if int(end_week_value) < int(start_week_value):
            return {}, "end_week 不能早于 start_week。"
        duration_days_value = (int(end_week_value) - int(start_week_value) + 1) * 7

    task.update(remaining_updates)
    if duration_days_value is not None:
        _set_task_duration_days(task, int(duration_days_value))
    if start_week_value is not None:
        task["start_week"] = max(1, int(start_week_value))

    display_updates = dict(remaining_updates)
    if start_week_value is not None:
        display_updates["start_week"] = max(1, int(start_week_value))
    if end_week_value is not None:
        display_updates["end_week"] = max(display_updates.get("start_week", 1), int(end_week_value))
    elif duration_days_value is not None and "duration_days" in updates:
        display_updates["duration_days"] = int(duration_days_value)

    return display_updates, None


def _try_parse_direct_task_rename(text: str) -> dict | None:
    raw = (text or "").strip()
    if not raw:
        return None

    patterns = [
        rf"(?:把|将)\s*{_TASK_REF_TOKEN_PATTERN}\s*(?:改为|改成|改名为|命名为)\s*[：: ]?(.+)$",
        rf"^{_TASK_REF_TOKEN_PATTERN}\s*(?:改为|改成|改名为|命名为)\s*[：: ]?(.+)$",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if not match:
            continue
        task_id = _normalize_task_ref_text(match.group(1))
        new_name = str(match.group(2) or "").strip().strip("。；;！!")
        if task_id and new_name:
            return {"task_id": task_id, "new_name": new_name}
    return None


def _try_parse_direct_task_add(text: str) -> dict | None:
    raw = (text or "").strip()
    if not raw:
        return None

    patterns = [
        r"(?:新增|添加|增加|加一个)\s*(?:任务)?\s*[：: ]?(.+)$",
        r"(?:新增任务|添加任务|增加任务)\s*[：: ]?(.+)$",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if not match:
            continue
        task_name = str(match.group(1) or "").strip().strip("。；;！!")
        if not task_name:
            continue
        if "依赖" in task_name:
            continue
        task_name = re.sub(r"^(?:T\d+|\d+\.\d+)\s*", "", task_name, flags=re.IGNORECASE).strip()
        if not task_name:
            continue
        duration_days = 7
        duration_match = re.search(r"(\d+)\s*天", task_name)
        if duration_match:
            try:
                duration_days = max(1, int(duration_match.group(1)))
            except (TypeError, ValueError):
                duration_days = 7
            task_name = re.sub(r"[，,。；;]?\s*耗时\s*\d+\s*天", "", task_name).strip()
            task_name = re.sub(r"[，,。；;]?\s*\d+\s*天", "", task_name).strip()
        if task_name:
            return {"task": task_name, "duration_days": duration_days, "owner": "Unassigned"}
    return None


def _try_parse_direct_task_delete(text: str) -> dict | None:
    raw = (text or "").strip()
    if not raw:
        return None

    id_match = re.search(rf"(?:删除|移除|去掉)\s*{_TASK_REF_TOKEN_PATTERN}", raw, flags=re.IGNORECASE)
    if id_match:
        return {"task_id": _normalize_task_ref_text(id_match.group(1))}

    name_match = re.search(r"(?:删除|移除|去掉)\s*(?:任务)?\s*[：: ]?(.+)$", raw, flags=re.IGNORECASE)
    if name_match:
        task_name = str(name_match.group(1) or "").strip().strip("。；;！!")
        if task_name and not re.match(rf"^{_TASK_REF_TOKEN_PATTERN}$", task_name, flags=re.IGNORECASE):
            return {"task_name": task_name}
    return None


def _next_task_id(tasks: list[dict]) -> str:
    max_num = 0
    for task in tasks:
        tid = str(task.get("task_id", "")).strip().upper()
        match = re.match(r"^T(\d+)$", tid)
        if not match:
            continue
        try:
            max_num = max(max_num, int(match.group(1)))
        except (TypeError, ValueError):
            continue
    return f"T{max_num + 1}"


def _parse_cn_number(text: str) -> int | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    if raw.isdigit():
        return int(raw)
    mapping = {
        "一": 1,
        "二": 2,
        "两": 2,
        "三": 3,
        "四": 4,
        "五": 5,
        "六": 6,
        "七": 7,
        "八": 8,
        "九": 9,
        "十": 10,
    }
    if raw in mapping:
        return mapping[raw]
    return None


def _try_parse_direct_dependency_change(text: str) -> dict | None:
    raw = (text or "").strip()
    if not raw:
        return None
    match = re.search(rf"{_TASK_REF_TOKEN_PATTERN}\s*依赖\s*{_TASK_REF_TOKEN_PATTERN}", raw, flags=re.IGNORECASE)
    if not match:
        return None
    dependent = _normalize_task_ref_text(match.group(1))
    prerequisite = _normalize_task_ref_text(match.group(2))
    if not dependent or not prerequisite or dependent == prerequisite:
        return None
    return {
        "action": "add",
        "from": prerequisite,
        "to": dependent,
        "type": "FS",
        "lag_weeks": 0,
        "overlap_weeks": 0,
    }


def _try_parse_direct_duration_extend(text: str) -> dict | None:
    raw = (text or "").strip()
    if not raw:
        return None
    pattern = rf"(?:把|将)?\s*{_TASK_REF_TOKEN_PATTERN}\s*(?:的)?(?:时间|工期|时长|排期|duration)?\s*延长\s*([一二两三四五六七八九十\d]+)\s*(天|周)"
    match = re.search(pattern, raw, flags=re.IGNORECASE)
    if not match:
        return None

    task_id = _normalize_task_ref_text(match.group(1))
    amount = _parse_cn_number(str(match.group(2) or "").strip())
    unit = str(match.group(3) or "天").strip()
    if not task_id or amount is None or amount <= 0:
        return None

    delta_days = amount * (7 if unit == "周" else 1)
    return {"task_id": task_id, "delta_days": delta_days}


def _parse_week_range(text: str) -> tuple[int, int] | None:
    raw = str(text or "").strip()
    if not raw:
        return None

    patterns = [
        r"[Ww]\s*(\d+)\s*(?:-|到|至|~|～)\s*[Ww]\s*(\d+)",
        r"[Ww]\s*(\d+)\s*(?:-|到|至|~|～)\s*(\d+)",
        r"第?\s*(\d+)\s*周\s*(?:-|到|至|~|～)\s*第?\s*(\d+)\s*周",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if not match:
            continue
        try:
            start_week = max(1, int(match.group(1)))
            end_week = max(1, int(match.group(2)))
        except (TypeError, ValueError):
            continue
        if end_week < start_week:
            continue
        return start_week, end_week
    return None


def _try_parse_direct_schedule_window_change(text: str) -> dict | None:
    raw = (text or "").strip()
    if not raw:
        return None

    patterns = [
        rf"(?:把|将)?\s*{_TASK_REF_TOKEN_PATTERN}\s*(?:的)?(?:排期|时间安排|时间|起止时间|起止|计划)\s*(?:改为|改成|调整为|调整到|排到|安排到|挪到|移到)\s*(.+)$",
        rf"(?:把|将)?\s*{_TASK_REF_TOKEN_PATTERN}\s*(?:排到|安排到|挪到|移到)\s*(.+)$",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if not match:
            continue
        task_id = _normalize_task_ref_text(match.group(1))
        week_range = _parse_week_range(match.group(2))
        if task_id and week_range:
            return {
                "task_id": task_id,
                "start_week": week_range[0],
                "end_week": week_range[1],
            }
    return None


def _split_compound_clauses(text: str) -> list[str]:
    raw = (text or "").strip()
    if not raw:
        return []
    parts = re.split(r"[，,；;。]\s*(?:并且|并|然后)?\s*", raw)
    clauses = [str(p).strip() for p in parts if str(p).strip()]
    return clauses if clauses else [raw]


def _parse_compound_operations(text: str) -> list[dict]:
    operations: list[dict] = []
    for clause in _split_compound_clauses(text):
        dep = _try_parse_direct_dependency_change(clause)
        if dep:
            operations.append({"kind": "dependency_update", "dependency_change": dep})
            continue

        extend = _try_parse_direct_duration_extend(clause)
        if extend:
            operations.append({"kind": "task_extend", **extend, "need_reschedule": True})
            continue

        schedule_change = _try_parse_direct_schedule_window_change(clause)
        if schedule_change:
            operations.append(
                {
                    "kind": "task_update",
                    "task_id": schedule_change["task_id"],
                    "task_name": "",
                    "updates": {
                        "start_week": schedule_change["start_week"],
                        "end_week": schedule_change["end_week"],
                    },
                    "need_reschedule": True,
                }
            )
            continue

        rename = _try_parse_direct_task_rename(clause)
        if rename:
            operations.append(
                {
                    "kind": "task_update",
                    "task_id": rename["task_id"],
                    "task_name": "",
                    "updates": {"task": rename["new_name"]},
                    "need_reschedule": False,
                }
            )
            continue

        add = _try_parse_direct_task_add(clause)
        if add:
            operations.append(
                {
                    "kind": "task_add",
                    "task": add["task"],
                    "duration_days": int(add.get("duration_days", 7) or 7),
                    "owner": str(add.get("owner", "Unassigned") or "Unassigned"),
                    "need_reschedule": True,
                }
            )
            continue

        delete = _try_parse_direct_task_delete(clause)
        if delete:
            operations.append(
                {
                    "kind": "task_delete",
                    "task_id": str(delete.get("task_id", "")).strip(),
                    "task_name": str(delete.get("task_name", "")).strip(),
                    "need_reschedule": True,
                }
            )
            continue

    return operations


def _summarize_operations(operations: list[dict]) -> str:
    lines: list[str] = []
    for op in operations:
        kind = str(op.get("kind", "")).strip().lower()
        if kind == "task_add":
            lines.append(f"- 新增任务：{op.get('task', '')}")
        elif kind == "task_delete":
            target = str(op.get("task_id") or op.get("task_name") or "")
            lines.append(f"- 删除任务：{target}")
        elif kind == "task_update":
            target = str(op.get("task_id") or op.get("task_name") or "")
            updates = op.get("updates", {}) if isinstance(op.get("updates"), dict) else {}
            schedule_label = _updates_schedule_window_label(updates)
            if schedule_label:
                lines.append(f"- 修改任务排期：{target}，{schedule_label}")
            else:
                lines.append(f"- 修改任务：{target}，updates={updates}")
        elif kind == "task_extend":
            lines.append(f"- 延长工期：{op.get('task_id', '')}，+{op.get('delta_days', 0)}天")
        elif kind == "dependency_update":
            dep = op.get("dependency_change", {}) if isinstance(op.get("dependency_change"), dict) else {}
            lines.append(
                f"- 依赖变更：{dep.get('action', '')} {dep.get('from', '')}->{dep.get('to', '')} ({dep.get('type', 'FS')})"
            )
    return "\n".join(lines)


def _set_task_duration_days(task: dict, duration_days: int) -> None:
    days = max(1, int(duration_days))
    weeks = max(1, (days + 6) // 7)
    task["duration_days"] = days
    task["duration_weeks"] = weeks
    task["duration"] = weeks


def _apply_operation_on_snapshot(tasks: list[dict], dependencies: list[dict], operation: dict) -> tuple[list[dict], list[dict], str, bool, str | None]:
    kind = str(operation.get("kind", "")).strip().lower()
    valid_task_ids = {str(task.get("task_id", "")).strip() for task in tasks}

    if kind == "dependency_update":
        dep_change, dep_err = _sanitize_dependency_change(operation.get("dependency_change", {}))
        if dep_err:
            return tasks, dependencies, "", False, dep_err

        source = _resolve_task_id_ref(tasks, dep_change["from"])
        target = _resolve_task_id_ref(tasks, dep_change["to"])
        if source not in valid_task_ids or target not in valid_task_ids:
            return tasks, dependencies, "", False, "from/to 任务 ID 不存在，请先确认任务编号。"

        action = dep_change["action"]
        existing = [
            d for d in dependencies
            if str(d.get("from", "")).strip() == source and str(d.get("to", "")).strip() == target
        ]

        if action == "add":
            dependencies.append(
                {
                    "from": source,
                    "to": target,
                    "type": dep_change["type"],
                    "lag_weeks": dep_change["lag_weeks"],
                    "overlap_weeks": dep_change["overlap_weeks"],
                }
            )
        elif action == "remove":
            dependencies = [
                d for d in dependencies
                if not (
                    str(d.get("from", "")).strip() == source
                    and str(d.get("to", "")).strip() == target
                )
            ]
            if len(existing) == 0:
                return tasks, dependencies, "", False, "未找到要删除的依赖关系。"
        else:
            if len(existing) == 0:
                return tasks, dependencies, "", False, "未找到要更新的依赖关系，请先 add。"
            dependencies = [
                (
                    {
                        "from": source,
                        "to": target,
                        "type": dep_change["type"],
                        "lag_weeks": dep_change["lag_weeks"],
                        "overlap_weeks": dep_change["overlap_weeks"],
                    }
                    if str(d.get("from", "")).strip() == source and str(d.get("to", "")).strip() == target
                    else d
                )
                for d in dependencies
            ]

        summary = (
            f"已{ '新增' if action == 'add' else '删除' if action == 'remove' else '更新' }依赖："
            f"{source} -> {target} ({dep_change['type']}, lag={dep_change['lag_weeks']}w, overlap={dep_change['overlap_weeks']}w)"
        )
        return tasks, dependencies, summary, True, None

    if kind == "task_add":
        new_task_name = str(
            operation.get("task")
            or operation.get("new_task_name")
            or operation.get("task_name")
            or ""
        ).strip()
        if not new_task_name:
            return tasks, dependencies, "", False, "新增任务缺少任务名称。"

        try:
            duration_days = max(1, int(operation.get("duration_days", 7) or 7))
        except (TypeError, ValueError):
            return tasks, dependencies, "", False, "新增任务的 duration_days 必须是正整数。"

        owner = str(operation.get("owner", "")).strip() or "Unassigned"
        duration_weeks = max(1, (duration_days + 6) // 7)
        new_task_id = _next_task_id(tasks)
        template = tasks[-1] if tasks else {}
        tasks.append(
            {
                "task_id": new_task_id,
                "task": new_task_name,
                "duration_days": duration_days,
                "duration_weeks": duration_weeks,
                "duration": duration_weeks,
                "owner": owner,
                "status": "Planned",
                "progress": 0,
                "objective": template.get("objective", ""),
                "kr": template.get("kr", "KR1: Execution"),
                "kr_index": int(template.get("kr_index", 1) or 1),
                "subtask_index": int(template.get("subtask_index", 1) or 1) + 1,
            }
        )
        summary = f"已新增任务 {new_task_id} ({new_task_name})，duration_days={duration_days}, owner={owner}"
        return tasks, dependencies, summary, bool(operation.get("need_reschedule", True)), None

    if kind == "task_delete":
        idx = _find_task_index(tasks, operation.get("task_id"), operation.get("task_name"))
        if idx < 0:
            return tasks, dependencies, "", False, "未找到要删除的任务，请给出更明确的 task_id 或任务名。"

        removed = tasks.pop(idx)
        removed_id = str(removed.get("task_id", "")).strip()
        dependencies = [
            d for d in dependencies
            if str(d.get("from", "")).strip() != removed_id and str(d.get("to", "")).strip() != removed_id
        ]
        summary = f"已删除任务 {removed_id} ({removed.get('task', '')})，并清理相关依赖"
        return tasks, dependencies, summary, bool(operation.get("need_reschedule", True)), None

    if kind == "task_extend":
        idx = _find_task_index(tasks, operation.get("task_id"), operation.get("task_name"))
        if idx < 0:
            return tasks, dependencies, "", False, "未找到要延长工期的任务。"
        try:
            delta_days = max(1, int(operation.get("delta_days", 0) or 0))
        except (TypeError, ValueError):
            return tasks, dependencies, "", False, "延长工期的天数无效。"
        old_days = max(1, int(tasks[idx].get("duration_days", 7) or 7))
        new_days = old_days + delta_days
        _set_task_duration_days(tasks[idx], new_days)
        summary = f"已延长任务 {tasks[idx].get('task_id', '')} 工期：{old_days}天 -> {new_days}天"
        return tasks, dependencies, summary, True, None

    idx = _find_task_index(tasks, operation.get("task_id"), operation.get("task_name"))
    if idx < 0:
        return tasks, dependencies, "", False, "未找到要修改的任务，请给出更明确的 task_id 或任务名。"

    updates, err = _sanitize_update_fields(operation.get("updates", {}))
    if err:
        return tasks, dependencies, "", False, err

    old_task = dict(tasks[idx])
    display_updates, apply_err = _apply_task_updates(tasks[idx], updates)
    if apply_err:
        return tasks, dependencies, "", False, apply_err

    needs_reschedule = bool(operation.get("need_reschedule")) or _has_schedule_update_fields(updates)
    summary = (
        f"已修改任务 {old_task.get('task_id')} ({old_task.get('task')})："
        + ", ".join(_format_task_update_items(display_updates))
    )
    return tasks, dependencies, summary, needs_reschedule, None


def _sanitize_dependency_change(change: dict) -> tuple[dict, str | None]:
    if not isinstance(change, dict):
        return {}, "dependency_change 格式无效。"

    action = str(change.get("action", "")).strip().lower()
    if action not in {"add", "remove", "update"}:
        return {}, "dependency_change.action 仅支持 add/remove/update。"

    source = _normalize_task_ref_text(change.get("from"))
    target = _normalize_task_ref_text(change.get("to"))
    if not source or not target:
        return {}, "dependency_change 需要 from/to 任务 ID。"
    if source == target:
        return {}, "依赖关系的 from 和 to 不能相同。"

    dep_type = str(change.get("type", "FS")).upper().strip() or "FS"
    if dep_type not in _ALLOWED_DEP_TYPES:
        return {}, "依赖类型仅支持 FS/SS/FF/SF。"

    raw_lag_weeks = change.get("lag_weeks")
    if raw_lag_weeks is None:
        raw_lag_weeks = change.get("lag")
    if raw_lag_weeks is None:
        raw_lag_weeks = _days_to_weeks(change.get("lag_days", 0), allow_negative=True)
    try:
        lag_weeks = int(raw_lag_weeks or 0)
    except (TypeError, ValueError):
        return {}, "lag_weeks 必须是整数。"

    raw_overlap_weeks = change.get("overlap_weeks")
    if raw_overlap_weeks is None:
        raw_overlap_weeks = change.get("overlap")
    if raw_overlap_weeks is None:
        raw_overlap_weeks = _days_to_weeks(change.get("overlap_days", 0), allow_negative=False)
    try:
        overlap_weeks = max(0, int(raw_overlap_weeks or 0))
    except (TypeError, ValueError):
        return {}, "overlap_weeks 必须是整数。"

    cleaned = {
        "action": action,
        "from": source,
        "to": target,
        "type": dep_type,
        "lag_weeks": lag_weeks,
        "overlap_weeks": overlap_weeks,
    }
    return cleaned, None


def _apply_pending_change(project_id: str, pending: dict) -> dict:
    tasks = load_tasks(project_id)
    dependencies = load_dependencies(project_id)
    if not tasks:
        return {"status": "error", "message": "当前项目没有任务，无法执行修改。"}

    change_kind = str(pending.get("kind", "task_update")).strip().lower()
    valid_task_ids = {str(task.get("task_id", "")).strip() for task in tasks}

    if change_kind == "batch_update":
        operations = pending.get("operations", []) if isinstance(pending.get("operations"), list) else []
        if not operations:
            return {"status": "error", "message": "批量修改为空，未执行。"}

        work_tasks = [dict(task) for task in tasks]
        work_dependencies = [dict(dep) for dep in dependencies]
        step_summaries: list[str] = []
        need_reschedule = False

        for idx, operation in enumerate(operations, start=1):
            work_tasks, work_dependencies, step_summary, step_reschedule, err = _apply_operation_on_snapshot(
                work_tasks,
                work_dependencies,
                operation if isinstance(operation, dict) else {},
            )
            if err:
                return {"status": "error", "message": f"第 {idx} 步执行失败：{err}"}
            if step_summary:
                step_summaries.append(step_summary)
            need_reschedule = need_reschedule or bool(step_reschedule)

        final_tasks = schedule_tasks(work_tasks, dependencies=work_dependencies) if need_reschedule else work_tasks
        save_tasks(final_tasks, project_id, dependencies=work_dependencies)

        summary = "已执行批量修改：\n" + "\n".join([f"{i}. {s}" for i, s in enumerate(step_summaries, start=1)])
        if need_reschedule:
            summary += "\n并已完成局部重排期。"

        assistant_state = load_project_assistant_state(project_id)
        actions = assistant_state.get("last_actions", [])[-19:]
        actions.append({"summary": summary.replace("\n", " "), "ts": _now_iso()})
        save_project_assistant_state(project_id, last_actions=actions, clear_pending_change=True)
        return {
            "status": "ok",
            "message": summary,
            "rescheduled": need_reschedule,
            "steps": len(step_summaries),
        }

    if change_kind == "dependency_update":
        dep_change, dep_err = _sanitize_dependency_change(pending.get("dependency_change", {}))
        if dep_err:
            return {"status": "error", "message": dep_err}

        source = _resolve_task_id_ref(tasks, dep_change["from"])
        target = _resolve_task_id_ref(tasks, dep_change["to"])
        if source not in valid_task_ids or target not in valid_task_ids:
            return {"status": "error", "message": "from/to 任务 ID 不存在，请先确认任务编号。"}

        action = dep_change["action"]
        existing = [
            d for d in dependencies
            if str(d.get("from", "")).strip() == source and str(d.get("to", "")).strip() == target
        ]

        if action == "add":
            dependencies.append(
                {
                    "from": source,
                    "to": target,
                    "type": dep_change["type"],
                    "lag_weeks": dep_change["lag_weeks"],
                    "overlap_weeks": dep_change["overlap_weeks"],
                }
            )
        elif action == "remove":
            dependencies = [
                d for d in dependencies
                if not (
                    str(d.get("from", "")).strip() == source
                    and str(d.get("to", "")).strip() == target
                )
            ]
            if len(existing) == 0:
                return {"status": "error", "message": "未找到要删除的依赖关系。"}
        else:  # update
            if len(existing) == 0:
                return {"status": "error", "message": "未找到要更新的依赖关系，请先 add。"}
            dependencies = [
                (
                    {
                        "from": source,
                        "to": target,
                        "type": dep_change["type"],
                        "lag_weeks": dep_change["lag_weeks"],
                        "overlap_weeks": dep_change["overlap_weeks"],
                    }
                    if str(d.get("from", "")).strip() == source and str(d.get("to", "")).strip() == target
                    else d
                )
                for d in dependencies
            ]

        final_tasks = schedule_tasks(tasks, dependencies=dependencies)
        save_tasks(final_tasks, project_id, dependencies=dependencies)

        summary = (
            f"已{ '新增' if action == 'add' else '删除' if action == 'remove' else '更新' }依赖："
            f"{source} -> {target} ({dep_change['type']}, lag={dep_change['lag_weeks']}w, overlap={dep_change['overlap_weeks']}w)，"
            "并已重新排期。"
        )

        assistant_state = load_project_assistant_state(project_id)
        actions = assistant_state.get("last_actions", [])[-19:]
        actions.append({"summary": summary, "ts": _now_iso()})
        save_project_assistant_state(project_id, last_actions=actions, clear_pending_change=True)
        return {
            "status": "ok",
            "message": summary,
            "rescheduled": True,
        }

    if change_kind == "task_add":
        new_task_name = str(
            pending.get("task")
            or pending.get("new_task_name")
            or pending.get("task_name")
            or ""
        ).strip()
        if not new_task_name:
            return {"status": "error", "message": "新增任务缺少任务名称。"}

        try:
            duration_days = max(1, int(pending.get("duration_days", 7) or 7))
        except (TypeError, ValueError):
            return {"status": "error", "message": "新增任务的 duration_days 必须是正整数。"}

        owner = str(pending.get("owner", "")).strip() or "Unassigned"
        duration_weeks = max(1, (duration_days + 6) // 7)
        new_task_id = _next_task_id(tasks)

        template = tasks[-1] if tasks else {}
        new_task = {
            "task_id": new_task_id,
            "task": new_task_name,
            "duration_days": duration_days,
            "duration_weeks": duration_weeks,
            "duration": duration_weeks,
            "owner": owner,
            "status": "Planned",
            "progress": 0,
            "objective": template.get("objective", ""),
            "kr": template.get("kr", "KR1: Execution"),
            "kr_index": int(template.get("kr_index", 1) or 1),
            "subtask_index": int(template.get("subtask_index", 1) or 1) + 1,
        }
        tasks.append(new_task)

        needs_reschedule = bool(pending.get("need_reschedule", True))
        final_tasks = schedule_tasks(tasks, dependencies=dependencies) if needs_reschedule else tasks
        save_tasks(final_tasks, project_id, dependencies=dependencies)

        summary = (
            f"已新增任务 {new_task_id} ({new_task_name})，duration_days={duration_days}, owner={owner}"
            + ("；并已局部重排期。" if needs_reschedule else "。")
        )
        assistant_state = load_project_assistant_state(project_id)
        actions = assistant_state.get("last_actions", [])[-19:]
        actions.append({"summary": summary, "ts": _now_iso()})
        save_project_assistant_state(project_id, last_actions=actions, clear_pending_change=True)
        return {
            "status": "ok",
            "message": summary,
            "added_task_id": new_task_id,
            "rescheduled": needs_reschedule,
        }

    if change_kind == "task_delete":
        idx = _find_task_index(tasks, pending.get("task_id"), pending.get("task_name"))
        if idx < 0:
            return {"status": "error", "message": "未找到要删除的任务，请给出更明确的 task_id 或任务名。"}

        removed = tasks.pop(idx)
        removed_id = str(removed.get("task_id", "")).strip()
        dependencies = [
            d for d in dependencies
            if str(d.get("from", "")).strip() != removed_id and str(d.get("to", "")).strip() != removed_id
        ]

        needs_reschedule = bool(pending.get("need_reschedule", True))
        final_tasks = schedule_tasks(tasks, dependencies=dependencies) if needs_reschedule else tasks
        save_tasks(final_tasks, project_id, dependencies=dependencies)

        summary = (
            f"已删除任务 {removed_id} ({removed.get('task', '')})，并清理相关依赖"
            + ("；并已局部重排期。" if needs_reschedule else "。")
        )
        assistant_state = load_project_assistant_state(project_id)
        actions = assistant_state.get("last_actions", [])[-19:]
        actions.append({"summary": summary, "ts": _now_iso()})
        save_project_assistant_state(project_id, last_actions=actions, clear_pending_change=True)
        return {
            "status": "ok",
            "message": summary,
            "deleted_task_id": removed_id,
            "rescheduled": needs_reschedule,
        }

    if change_kind == "task_extend":
        idx = _find_task_index(tasks, pending.get("task_id"), pending.get("task_name"))
        if idx < 0:
            return {"status": "error", "message": "未找到要延长工期的任务。"}

        try:
            delta_days = max(1, int(pending.get("delta_days", 0) or 0))
        except (TypeError, ValueError):
            return {"status": "error", "message": "延长工期的天数无效。"}

        old_days = max(1, int(tasks[idx].get("duration_days", 7) or 7))
        new_days = old_days + delta_days
        _set_task_duration_days(tasks[idx], new_days)

        final_tasks = schedule_tasks(tasks, dependencies=dependencies)
        save_tasks(final_tasks, project_id, dependencies=dependencies)

        summary = f"已延长任务 {tasks[idx].get('task_id', '')} 工期：{old_days}天 -> {new_days}天；并已重新排期。"
        assistant_state = load_project_assistant_state(project_id)
        actions = assistant_state.get("last_actions", [])[-19:]
        actions.append({"summary": summary, "ts": _now_iso()})
        save_project_assistant_state(project_id, last_actions=actions, clear_pending_change=True)
        return {
            "status": "ok",
            "message": summary,
            "changed_task_id": tasks[idx].get("task_id", ""),
            "rescheduled": True,
        }

    idx = _find_task_index(tasks, pending.get("task_id"), pending.get("task_name"))
    if idx < 0:
        return {"status": "error", "message": "未找到要修改的任务，请给出更明确的 task_id 或任务名。"}

    updates, err = _sanitize_update_fields(pending.get("updates", {}))
    if err:
        return {"status": "error", "message": err}

    old_task = dict(tasks[idx])
    display_updates, apply_err = _apply_task_updates(tasks[idx], updates)
    if apply_err:
        return {"status": "error", "message": apply_err}

    needs_reschedule = bool(pending.get("need_reschedule")) or _has_schedule_update_fields(updates)
    final_tasks = schedule_tasks(tasks, dependencies=dependencies) if needs_reschedule else tasks

    save_tasks(final_tasks, project_id, dependencies=dependencies)

    summary = (
        f"已修改任务 {old_task.get('task_id')} ({old_task.get('task')})："
        + ", ".join(_format_task_update_items(display_updates))
        + ("；并已重新排期。" if needs_reschedule else "。")
    )

    assistant_state = load_project_assistant_state(project_id)
    actions = assistant_state.get("last_actions", [])[-19:]
    actions.append({"summary": summary, "ts": _now_iso()})
    save_project_assistant_state(project_id, last_actions=actions, clear_pending_change=True)

    return {
        "status": "ok",
        "message": summary,
        "changed_task_id": old_task.get("task_id", ""),
        "updates": display_updates,
        "rescheduled": needs_reschedule,
    }


def _should_attempt_memory_capture(text: str) -> bool:
    lowered = (text or "").strip().lower()
    if not lowered:
        return False
    return any(token in lowered for token in _MEMORY_HINT_TOKENS)


def _safe_parse_memory_json(text: str) -> dict | None:
    parsed = _safe_parse_json(text)
    if not parsed:
        return None
    result = {
        "should_write": bool(parsed.get("should_write", False)),
        "preferences": parsed.get("preferences", []),
        "aliases": parsed.get("aliases", []),
        "facts": parsed.get("facts", []),
        "project_important_information": parsed.get("project_important_information", []),
    }
    for key in ("preferences", "aliases", "facts", "project_important_information"):
        if not isinstance(result[key], list):
            result[key] = []
    return result


def _extract_long_term_memory(project_id: str, user_message: str) -> dict[str, list[str]]:
    existing_system_memory = read_system_memory()
    existing_project_memory = read_project_memory(project_id)
    assistant_model = _resolve_assistant_llm_model()
    raw = call_llm_messages([
        {
            "role": "system",
            "content": (
                "你是长期记忆提取器。"
                "只提取适合长期保留的稳定信息，不要提取临时任务、一次性请求或短期状态。"
                "输出 JSON，不要输出 markdown。"
                "格式："
                '{"should_write": true|false, "preferences": ["..."], "aliases": ["..."], "facts": ["..."], "project_important_information": ["..."]}'
                "。preferences/aliases/facts 是跨项目共享的系统级记忆；project_important_information 仅限当前项目。"
                "若没有可写入长期记忆的内容，should_write=false 且数组为空。"
            ),
        },
        {
            "role": "user",
            "content": (
                f"当前系统级 MEMORY.md：\n{existing_system_memory}\n\n"
                f"当前项目 MEMORY.md：\n{existing_project_memory}\n\n"
                f"用户新消息：\n{user_message}"
            ),
        },
    ], inject_system_memory=False, model_override=assistant_model)
    parsed = _safe_parse_memory_json(raw)
    if not parsed or not parsed.get("should_write"):
        return {"preferences": [], "aliases": [], "facts": [], "project_important_information": []}
    return {
        "preferences": [str(item).strip() for item in parsed.get("preferences", []) if str(item).strip()],
        "aliases": [str(item).strip() for item in parsed.get("aliases", []) if str(item).strip()],
        "facts": [str(item).strip() for item in parsed.get("facts", []) if str(item).strip()],
        "project_important_information": [
            str(item).strip()
            for item in parsed.get("project_important_information", [])
            if str(item).strip()
        ],
    }


def _format_memory_note(added_system_entries: dict[str, list[str]], added_project_entries: list[str]) -> str:
    items: list[str] = []
    for key in ("preferences", "aliases", "facts"):
        values = added_system_entries.get(key, [])
        items.extend(values)
    items.extend(added_project_entries)
    if not items:
        return ""
    preview = "；".join(items[:3])
    suffix = "；..." if len(items) > 3 else ""
    return f"我已写入长期记忆：{preview}{suffix}"


def _memory_updates_payload(added_system_entries: dict[str, list[str]], added_project_entries: list[str]) -> dict:
    shared_items: list[str] = []
    for key in ("preferences", "aliases", "facts"):
        shared_items.extend([str(v).strip() for v in added_system_entries.get(key, []) if str(v).strip()])
    project_items = [str(v).strip() for v in added_project_entries if str(v).strip()]
    return {
        "added_system": shared_items,
        "added_project": project_items,
    }


def reclassify_memory_item(project_id: str, item: str, target_scope: str) -> dict:
    ok = reclassify_memory_item_store(project_id, item, target_scope)
    if not ok:
        return {"status": "error", "message": "记忆迁移失败，请检查条目内容。"}
    scope_label = "系统记忆" if str(target_scope).strip().lower() == "system" else "项目记忆"
    return {"status": "ok", "message": f"已将该条目迁移到{scope_label}。"}


def _looks_like_replan_request(text: str) -> bool:
    lowered = (text or "").strip().lower()
    if not lowered:
        return False
    return any(token in lowered for token in _REPLAN_HINT_TOKENS)


def _sanitize_replan_dependencies(raw_dependencies: list[dict], valid_task_ids: set[str]) -> list[dict]:
    cleaned: list[dict] = []
    seen: set[tuple] = set()
    for item in raw_dependencies if isinstance(raw_dependencies, list) else []:
        if not isinstance(item, dict):
            continue
        source = str(item.get("from") or item.get("source") or "").strip()
        target = str(item.get("to") or item.get("target") or "").strip()
        if not source or not target or source == target:
            continue
        if source not in valid_task_ids or target not in valid_task_ids:
            continue
        dep_type = str(item.get("type") or "FS").upper().strip()
        if dep_type not in _ALLOWED_DEP_TYPES:
            dep_type = "FS"
        raw_lag_weeks = item.get("lag_weeks")
        if raw_lag_weeks is None:
            raw_lag_weeks = item.get("lag")
        if raw_lag_weeks is None:
            raw_lag_weeks = _days_to_weeks(item.get("lag_days", 0), allow_negative=True)
        try:
            lag_weeks = int(raw_lag_weeks or 0)
        except (TypeError, ValueError):
            lag_weeks = 0

        raw_overlap_weeks = item.get("overlap_weeks")
        if raw_overlap_weeks is None:
            raw_overlap_weeks = item.get("overlap")
        if raw_overlap_weeks is None:
            raw_overlap_weeks = _days_to_weeks(item.get("overlap_days", 0), allow_negative=False)
        try:
            overlap_weeks = max(0, int(raw_overlap_weeks or 0))
        except (TypeError, ValueError):
            overlap_weeks = 0

        key = (source, target, dep_type, lag_weeks, overlap_weeks)
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(
            {
                "from": source,
                "to": target,
                "type": dep_type,
                "lag_weeks": lag_weeks,
                "overlap_weeks": overlap_weeks,
            }
        )
    return cleaned


def _replan_from_feedback(project_id: str, feedback: str) -> dict:
    old_tasks = load_tasks(project_id)
    old_dependencies = load_dependencies(project_id)
    if not old_tasks:
        return {"status": "error", "message": "当前项目还没有可调整的计划，请先生成一次计划。"}

    prompt = f"""
你是资深项目规划助手。用户对现有计划不满意，请根据反馈重新生成更好的计划。

输出必须是 JSON（不要 markdown），格式：
{{
  "tasks": [
    {{"task_id": "T1", "task_name": "...", "duration_weeks": 2}}
  ],
  "dependencies": [
        {{"from": "T1", "to": "T2", "type": "FS", "lag_weeks": 0, "overlap_weeks": 0}}
  ],
  "change_summary": "一句话说明做了哪些改动"
}}

要求：
- 任务数量建议 6-14。
- 任务名要可执行、具体。
- duration_weeks 取值 1-12。
- 依赖类型仅用 FS/SS/FF/SF。

当前计划任务：
{json.dumps(_compact_tasks(old_tasks, limit=80), ensure_ascii=False)}

当前依赖：
{json.dumps(old_dependencies[:120], ensure_ascii=False)}

用户修改意见：
{feedback}
"""

    assistant_model = _resolve_assistant_llm_model()
    raw = call_llm_messages([
        {"role": "system", "content": "只返回 JSON。"},
        {"role": "user", "content": prompt},
    ], model_override=assistant_model)
    parsed = _safe_parse_json(raw)
    if not parsed:
        return {"status": "error", "message": "我没能稳定解析新的计划，请再明确一点修改方向。"}

    raw_tasks = parsed.get("tasks", []) if isinstance(parsed.get("tasks"), list) else []
    internal_tasks = _to_internal_task_shape(raw_tasks)
    if len(internal_tasks) < 3:
        return {"status": "error", "message": "新计划任务过少或格式异常，请补充更具体的调整要求。"}

    objective = ""
    for task in old_tasks:
        objective = str(task.get("objective", "")).strip()
        if objective:
            break
    krs = sorted({str(task.get("kr", "")).strip() for task in old_tasks if str(task.get("kr", "")).strip()})
    if not krs:
        krs = ["KR1: Execution"]

    ordered_tasks = _group_tasks_into_kr_blocks(internal_tasks, objective, krs)
    valid_ids = {str(item.get("task_id", "")).strip() for item in internal_tasks}
    raw_dependencies = parsed.get("dependencies", [])
    dependencies = _sanitize_replan_dependencies(raw_dependencies, valid_ids)
    scheduled_tasks = schedule_tasks(ordered_tasks, dependencies=dependencies)
    save_tasks(scheduled_tasks, project_id, dependencies=dependencies)

    change_summary = str(parsed.get("change_summary", "")).strip() or "已根据你的反馈生成新计划。"
    assistant_state = load_project_assistant_state(project_id)
    actions = assistant_state.get("last_actions", [])[-19:]
    actions.append(
        {
            "summary": f"自动重规划完成：任务 {len(scheduled_tasks)} 个，依赖 {len(dependencies)} 条。{change_summary}",
            "ts": _now_iso(),
        }
    )
    save_project_assistant_state(project_id, last_actions=actions)

    return {
        "status": "ok",
        "message": f"{change_summary}（新计划：{len(scheduled_tasks)} 个任务，{len(dependencies)} 条依赖）",
        "tasks_count": len(scheduled_tasks),
        "dependencies_count": len(dependencies),
    }


def _recent_context_messages(history: list[dict], rounds: int = 2) -> list[dict]:
    """Build short conversational context for the next assistant turn."""
    if not isinstance(history, list) or rounds <= 0:
        return []

    max_messages = max(1, rounds) * 2
    messages: list[dict] = []
    for item in history:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role", "")).strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = str(item.get("content", "") or "").strip()
        if not content:
            continue
        messages.append({"role": role, "content": content})

    return messages[-max_messages:]


def _build_assistant_system_prompt(
    tasks: list[dict],
    dependencies: list[dict],
    summary_snippets: list[dict],
    system_memory: str,
    project_memory: str,
) -> str:
    return f"""你是 Project Copilot。你的职责是：
1) 回答项目计划和总结文件问题。
2) 当用户想修改任务或依赖关系时，只返回可执行的结构化变更建议，不直接执行。
3) 你会收到最近 2 轮真实对话消息，请优先利用这些短上下文理解“它/这个/上一条”的指代。

你必须输出 JSON（不要输出 markdown），格式如下：
{{
    "intent": "query" | "change" | "clarify" | "replan",
  "answer": "给用户的自然语言回复",
  "change": {{
        "action": "update" | "add" | "delete",
    "task_id": "可空",
    "task_name": "可空",
        "new_task_name": "新增任务名（action=add时可用）",
        "duration_days": 7,
        "owner": "Unassigned",
                "updates": {{"task": "新任务名", "progress": 80, "status": "In Progress", "owner": "Alice", "duration_days": 10, "start_week": 7, "end_week": 8}},
    "need_reschedule": true | false
  }},
    "operations": [
        {{"kind": "task_add", "task": "...", "duration_days": 7, "owner": "Unassigned", "need_reschedule": true}},
        {{"kind": "dependency_update", "dependency_change": {{"action": "add", "from": "T2", "to": "T7", "type": "FS", "lag_weeks": 0, "overlap_weeks": 0}}}},
                {{"kind": "task_update", "task_id": "T9", "updates": {{"start_week": 7, "end_week": 8}}, "need_reschedule": true}}
    ],
    "dependency_change": {{
        "action": "add" | "remove" | "update",
        "from": "T1",
        "to": "T2",
        "type": "FS" | "SS" | "FF" | "SF",
        "lag_weeks": 0,
        "overlap_weeks": 0
    }},
  "clarification": "如果信息不足，需要用户补充什么"
}}

约束：
- 仅允许修改字段 task/progress/status/owner/duration_days/start_week/end_week。
- status 仅能取 Planned/In Progress/Done/At Risk。
- intent=change 且 action=update/delete 时，change 必须给出 task_id 或 task_name。
- intent=change 且 action=add 时，必须给出 new_task_name（或 updates.task）与 duration_days。
- 若用户要求改依赖，请优先使用 dependency_change，不要放到 change。
- 当用户只是提问，不要生成 change。
- 若仅是改任务名/负责人/进度/状态，need_reschedule 应为 false。
- 当用户明确说“改为 W7-W8 / 第7周到第8周”这类排期窗口时，使用 updates.start_week / updates.end_week，并将 need_reschedule 设为 true。
- 对“新增任务/删除任务”的需求，优先用 change.action=add/delete，不要触发 replan。
- 当一句话包含多个可执行修改时，优先输出 operations（长度>=2），并保持执行顺序。
- task_id 同时支持内部编号（如 T9）、界面展示编号（如 2.1）以及带前缀写法（如 任务1.2、task 2.1）。

当前任务列表：
{json.dumps(_compact_tasks(tasks), ensure_ascii=False)}

当前依赖：
{json.dumps(dependencies[:40], ensure_ascii=False)}

系统级共享记忆（跨项目生效）：
{system_memory}

当前项目重要信息：
{project_memory}

可用总结片段（用于回答细节）：
{json.dumps(summary_snippets, ensure_ascii=False)}

当用户明确表示对当前计划不满意并希望整体调整时，请输出 intent="replan"。
replan 不需要给 change/dependency_change。
"""


def _safe_parse_json(text: str) -> dict | None:
    raw = (text or "").strip()
    if not raw:
        return None

    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        pass

    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        snippet = raw[start:end + 1]
        try:
            parsed = json.loads(snippet)
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def assistant_chat(
    project_id: str,
    user_message: str,
    summaries_folder: str | None = None,
    user_message_ts: str | None = None,
) -> dict:
    """Process one assistant turn with controlled change proposal workflow.

    Behavior:
    - User says "确认执行" / "取消执行" to handle pending change.
    - Otherwise model returns either query answer or a change proposal.
    - Change is not applied automatically; it is saved as pending and waits for confirmation.
    """
    text = (user_message or "").strip()
    if not text:
        return {"status": "error", "message": "请输入消息。"}

    assistant_state = load_project_assistant_state(project_id)
    history = assistant_state.get("chat_history", [])
    pending = assistant_state.get("pending_change")
    user_ts = _coerce_chat_message_ts(user_message_ts)
    memory_note = ""
    added_system_entries: dict[str, list[str]] = {"preferences": [], "aliases": [], "facts": []}
    added_project_entries: list[str] = []

    if _should_attempt_memory_capture(text):
        try:
            extracted_memory = _extract_long_term_memory(project_id, text)
            added_system_entries = upsert_system_memory(
                {
                    "preferences": extracted_memory.get("preferences", []),
                    "aliases": extracted_memory.get("aliases", []),
                    "facts": extracted_memory.get("facts", []),
                }
            )
            added_project_entries = upsert_project_memory(
                project_id,
                extracted_memory.get("project_important_information", []),
            )
            memory_note = _format_memory_note(added_system_entries, added_project_entries)
            if memory_note:
                save_project_assistant_state(project_id, assistant_memory=read_system_memory())
        except Exception:
            memory_note = ""

    if text in {"确认执行", "确认", "执行"} and pending:
        result = _apply_pending_change(project_id, pending)
        history = history[-29:]
        history.append({"role": "user", "content": text, "ts": user_ts})
        history.append({"role": "assistant", "content": result.get("message", "已执行。"), "ts": _now_iso()})
        save_project_assistant_state(project_id, chat_history=history)
        return {
            "status": result.get("status", "ok"),
            "reply": result.get("message", "已执行。"),
            "pending_change": None,
            "executed": result.get("status") == "ok",
            "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
        }

    if text in {"取消执行", "取消", "算了"} and pending:
        save_project_assistant_state(project_id, clear_pending_change=True)
        history = history[-29:]
        history.append({"role": "user", "content": text, "ts": user_ts})
        history.append({"role": "assistant", "content": "已取消这次待执行修改。", "ts": _now_iso()})
        save_project_assistant_state(project_id, chat_history=history)
        return {
            "status": "ok",
            "reply": "已取消这次待执行修改。",
            "pending_change": None,
            "executed": False,
            "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
        }

    if _is_global_risk_focus_request(text):
        requested_top_items = _extract_global_risk_top_items(text, default=_GLOBAL_RISK_TOP_ITEMS)
        reply = _build_global_risk_focus_reply(top_items=requested_top_items)
        if memory_note:
            reply = memory_note + "\n\n" + reply

        history = history[-29:]
        history.append({"role": "user", "content": text, "ts": user_ts})
        history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
        save_project_assistant_state(project_id, chat_history=history)

        return {
            "status": "ok",
            "reply": reply,
            "pending_change": assistant_state.get("pending_change"),
            "executed": False,
            "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
        }

    tasks = load_tasks(project_id)
    dependencies = load_dependencies(project_id)
    compound_ops = _parse_compound_operations(text)
    if len(compound_ops) >= 2:
        pending_change = {
            "kind": "batch_update",
            "operations": compound_ops,
            "requested_at": _now_iso(),
        }
        save_project_assistant_state(project_id, pending_change=pending_change)
        preview = _summarize_operations(compound_ops)
        reply = (
            "我识别到你在一句话里提出了多个修改，已生成批量待执行操作：\n"
            f"{preview}\n\n"
            "回复“确认执行”将按顺序一次性执行，回复“取消执行”可撤销这次批量修改。"
        )
        if memory_note:
            reply = memory_note + "\n\n" + reply
        history = history[-29:]
        history.append({"role": "user", "content": text, "ts": user_ts})
        history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
        save_project_assistant_state(project_id, chat_history=history)
        return {
            "status": "ok",
            "reply": reply,
            "pending_change": pending_change,
            "executed": False,
            "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
        }

    direct_rename = _try_parse_direct_task_rename(text)
    if direct_rename:
        pending_change = {
            "kind": "task_update",
            "task_id": direct_rename["task_id"],
            "task_name": "",
            "updates": {"task": direct_rename["new_name"]},
            "need_reschedule": False,
            "requested_at": _now_iso(),
        }
        save_project_assistant_state(project_id, pending_change=pending_change)
        reply = (
            f"已生成待执行修改：目标任务={direct_rename['task_id']}，"
            f"新任务名={direct_rename['new_name']}。\n\n"
            "这不会触发整盘重规划。回复“确认执行”才会落盘，回复“取消执行”可撤销这次修改。"
        )
        if memory_note:
            reply = memory_note + "\n\n" + reply
        history = history[-29:]
        history.append({"role": "user", "content": text, "ts": user_ts})
        history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
        save_project_assistant_state(project_id, chat_history=history)
        return {
            "status": "ok",
            "reply": reply,
            "pending_change": pending_change,
            "executed": False,
            "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
        }

    direct_add = _try_parse_direct_task_add(text)
    if direct_add:
        pending_change = {
            "kind": "task_add",
            "task": direct_add["task"],
            "duration_days": int(direct_add.get("duration_days", 7) or 7),
            "owner": str(direct_add.get("owner", "Unassigned") or "Unassigned"),
            "need_reschedule": True,
            "requested_at": _now_iso(),
        }
        save_project_assistant_state(project_id, pending_change=pending_change)
        reply = (
            f"已生成待执行新增：任务名={pending_change['task']}，"
            f"duration_days={pending_change['duration_days']}，owner={pending_change['owner']}。\n\n"
            "这是局部新增任务，不会触发整盘重规划。回复“确认执行”才会落盘，回复“取消执行”可撤销这次修改。"
        )
        if memory_note:
            reply = memory_note + "\n\n" + reply
        history = history[-29:]
        history.append({"role": "user", "content": text, "ts": user_ts})
        history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
        save_project_assistant_state(project_id, chat_history=history)
        return {
            "status": "ok",
            "reply": reply,
            "pending_change": pending_change,
            "executed": False,
            "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
        }

    direct_delete = _try_parse_direct_task_delete(text)
    if direct_delete:
        pending_change = {
            "kind": "task_delete",
            "task_id": str(direct_delete.get("task_id", "")).strip(),
            "task_name": str(direct_delete.get("task_name", "")).strip(),
            "need_reschedule": True,
            "requested_at": _now_iso(),
        }
        save_project_assistant_state(project_id, pending_change=pending_change)
        target = pending_change["task_id"] or pending_change["task_name"] or "未识别"
        reply = (
            f"已生成待执行删除：目标任务={target}。\n\n"
            "这是局部删除任务，不会触发整盘重规划。回复“确认执行”才会落盘，回复“取消执行”可撤销这次修改。"
        )
        if memory_note:
            reply = memory_note + "\n\n" + reply
        history = history[-29:]
        history.append({"role": "user", "content": text, "ts": user_ts})
        history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
        save_project_assistant_state(project_id, chat_history=history)
        return {
            "status": "ok",
            "reply": reply,
            "pending_change": pending_change,
            "executed": False,
            "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
        }

    direct_schedule_change = _try_parse_direct_schedule_window_change(text)
    if direct_schedule_change:
        pending_change = {
            "kind": "task_update",
            "task_id": str(direct_schedule_change.get("task_id", "")).strip(),
            "task_name": "",
            "updates": {
                "start_week": int(direct_schedule_change.get("start_week", 1) or 1),
                "end_week": int(direct_schedule_change.get("end_week", 1) or 1),
            },
            "need_reschedule": True,
            "requested_at": _now_iso(),
        }
        save_project_assistant_state(project_id, pending_change=pending_change)
        schedule_label = _format_schedule_window(
            pending_change["updates"].get("start_week"),
            pending_change["updates"].get("end_week"),
        )
        reply = (
            f"已生成待执行排期修改：目标任务={pending_change['task_id']}，"
            f"排期改为 {schedule_label}。\n\n"
            "回复“确认执行”才会落盘并重排期，回复“取消执行”可撤销这次修改。"
        )
        if memory_note:
            reply = memory_note + "\n\n" + reply
        history = history[-29:]
        history.append({"role": "user", "content": text, "ts": user_ts})
        history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
        save_project_assistant_state(project_id, chat_history=history)
        return {
            "status": "ok",
            "reply": reply,
            "pending_change": pending_change,
            "executed": False,
            "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
        }

    direct_extend = _try_parse_direct_duration_extend(text)
    if direct_extend:
        pending_change = {
            "kind": "task_extend",
            "task_id": str(direct_extend.get("task_id", "")).strip(),
            "task_name": "",
            "delta_days": int(direct_extend.get("delta_days", 0) or 0),
            "need_reschedule": True,
            "requested_at": _now_iso(),
        }
        save_project_assistant_state(project_id, pending_change=pending_change)

        delta_days = int(pending_change.get("delta_days", 0) or 0)
        delta_weeks = max(1, (delta_days + 6) // 7)
        reply = (
            f"已生成待执行延长工期：目标任务={pending_change['task_id']}，"
            f"延长 {delta_weeks} 周（{delta_days} 天）。\n\n"
            "回复“确认执行”才会落盘并重排期，回复“取消执行”可撤销这次修改。"
        )
        if memory_note:
            reply = memory_note + "\n\n" + reply
        history = history[-29:]
        history.append({"role": "user", "content": text, "ts": user_ts})
        history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
        save_project_assistant_state(project_id, chat_history=history)
        return {
            "status": "ok",
            "reply": reply,
            "pending_change": pending_change,
            "executed": False,
            "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
        }

    snippets = []
    if summaries_folder:
        snippets = _pick_summary_snippets(text, summaries_folder)

    system_memory = read_system_memory()
    project_memory = read_project_memory(project_id)
    recent_context = _recent_context_messages(history, rounds=2)
    system_prompt = _build_assistant_system_prompt(
        tasks,
        dependencies,
        snippets,
        system_memory,
        project_memory,
    )
    model_messages = [
        {"role": "system", "content": system_prompt},
        *recent_context,
        {"role": "user", "content": text},
    ]
    assistant_model = _resolve_assistant_llm_model()
    model_raw = call_llm_messages(
        model_messages,
        inject_system_memory=False,
        model_override=assistant_model,
    )
    model_obj = _safe_parse_json(model_raw)
    if not model_obj:
        reply = "我没能稳定解析这次请求，请换一种更明确的说法（例如：把 T3 进度改为 60%）。"
        if memory_note:
            reply = memory_note + "\n\n" + reply
        history = history[-29:]
        history.append({"role": "user", "content": text, "ts": user_ts})
        history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
        save_project_assistant_state(project_id, chat_history=history)
        return {
            "status": "ok",
            "reply": reply,
            "pending_change": pending,
            "executed": False,
            "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
        }

    intent = str(model_obj.get("intent", "query")).strip().lower()
    answer = str(model_obj.get("answer", "")).strip()

    if intent == "change":
        change = model_obj.get("change") if isinstance(model_obj.get("change"), dict) else {}
        change_action = str(change.get("action", "update")).strip().lower() or "update"
        operations = model_obj.get("operations") if isinstance(model_obj.get("operations"), list) else []
        dependency_change = (
            model_obj.get("dependency_change")
            if isinstance(model_obj.get("dependency_change"), dict)
            else {}
        )

        normalized_ops: list[dict] = []
        for op in operations:
            if not isinstance(op, dict):
                continue
            kind = str(op.get("kind", "")).strip().lower()
            if kind == "task_add":
                try:
                    duration_days = max(1, int(op.get("duration_days", 7) or 7))
                except (TypeError, ValueError):
                    duration_days = 7
                normalized_ops.append(
                    {
                        "kind": "task_add",
                        "task": str(op.get("task", "")).strip(),
                        "duration_days": duration_days,
                        "owner": str(op.get("owner", "Unassigned") or "Unassigned").strip() or "Unassigned",
                        "need_reschedule": bool(op.get("need_reschedule", True)),
                    }
                )
            elif kind == "task_delete":
                normalized_ops.append(
                    {
                        "kind": "task_delete",
                        "task_id": _normalize_task_ref_text(op.get("task_id")),
                        "task_name": str(op.get("task_name", "")).strip(),
                        "need_reschedule": bool(op.get("need_reschedule", True)),
                    }
                )
            elif kind == "task_update":
                normalized_ops.append(
                    {
                        "kind": "task_update",
                        "task_id": _normalize_task_ref_text(op.get("task_id")),
                        "task_name": str(op.get("task_name", "")).strip(),
                        "updates": op.get("updates", {}) if isinstance(op.get("updates"), dict) else {},
                        "need_reschedule": bool(op.get("need_reschedule", False)),
                    }
                )
            elif kind == "task_extend":
                try:
                    delta_days = int(op.get("delta_days", 0) or 0)
                except (TypeError, ValueError):
                    delta_days = 0
                normalized_ops.append(
                    {
                        "kind": "task_extend",
                        "task_id": _normalize_task_ref_text(op.get("task_id")),
                        "task_name": str(op.get("task_name", "")).strip(),
                        "delta_days": delta_days,
                        "need_reschedule": bool(op.get("need_reschedule", True)),
                    }
                )
            elif kind == "dependency_update" and isinstance(op.get("dependency_change"), dict):
                dep_change = dict(op.get("dependency_change") or {})
                dep_change["from"] = _normalize_task_ref_text(dep_change.get("from"))
                dep_change["to"] = _normalize_task_ref_text(dep_change.get("to"))
                normalized_ops.append({"kind": "dependency_update", "dependency_change": dep_change})
        if len(normalized_ops) >= 2:
            pending_change = {
                "kind": "batch_update",
                "operations": normalized_ops,
                "requested_at": _now_iso(),
            }
            save_project_assistant_state(project_id, pending_change=pending_change)
            preview = _summarize_operations(normalized_ops)
            reply = ((memory_note + "\n\n") if memory_note else "") + ((answer + "\n\n") if answer else "") + (
                "已生成批量待执行操作：\n"
                f"{preview}\n\n"
                "回复“确认执行”将按顺序一次性执行，回复“取消执行”可撤销这次批量修改。"
            )
            history = history[-29:]
            history.append({"role": "user", "content": text, "ts": user_ts})
            history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
            save_project_assistant_state(project_id, chat_history=history)
            return {
                "status": "ok",
                "reply": reply,
                "pending_change": pending_change,
                "executed": False,
                "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
            }

        if len(normalized_ops) == 1 and str(normalized_ops[0].get("kind", "")).strip().lower() == "task_extend":
            op = normalized_ops[0]
            pending_change = {
                "kind": "task_extend",
                "task_id": _normalize_task_ref_text(op.get("task_id")),
                "task_name": str(op.get("task_name", "")).strip(),
                "delta_days": int(op.get("delta_days", 0) or 0),
                "need_reschedule": True,
                "requested_at": _now_iso(),
            }
            save_project_assistant_state(project_id, pending_change=pending_change)
            preview = _summarize_operations(normalized_ops)
            reply = ((memory_note + "\n\n") if memory_note else "") + ((answer + "\n\n") if answer else "") + (
                "已生成待执行修改：\n"
                f"{preview}\n\n"
                "回复“确认执行”才会落盘并重排期，回复“取消执行”可撤销这次修改。"
            )
            history = history[-29:]
            history.append({"role": "user", "content": text, "ts": user_ts})
            history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
            save_project_assistant_state(project_id, chat_history=history)
            return {
                "status": "ok",
                "reply": reply,
                "pending_change": pending_change,
                "executed": False,
                "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
            }

        dep_change, dep_err = _sanitize_dependency_change(dependency_change) if dependency_change else ({}, None)
        if dep_change and not dep_err:
            pending_change = {
                "kind": "dependency_update",
                "dependency_change": dep_change,
                "requested_at": _now_iso(),
            }
            save_project_assistant_state(project_id, pending_change=pending_change)
            preview = (
                "已生成待执行依赖变更："
                f"action={dep_change['action']}，{dep_change['from']} -> {dep_change['to']}，"
                f"type={dep_change['type']}，lag={dep_change['lag_weeks']}w，overlap={dep_change['overlap_weeks']}w。"
                "\n\n回复“确认执行”才会落盘，回复“取消执行”可撤销这次修改。"
            )
            reply = ((memory_note + "\n\n") if memory_note else "") + ((answer + "\n\n") if answer else "") + preview
            history = history[-29:]
            history.append({"role": "user", "content": text, "ts": user_ts})
            history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
            save_project_assistant_state(project_id, chat_history=history)
            return {
                "status": "ok",
                "reply": reply,
                "pending_change": pending_change,
                "executed": False,
                "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
            }

        if dep_err and dependency_change:
            reply = f"我理解到你要改依赖关系，但参数不完整：{dep_err}"
            history = history[-29:]
            history.append({"role": "user", "content": text, "ts": user_ts})
            history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
            save_project_assistant_state(project_id, chat_history=history)
            return {
                "status": "ok",
                "reply": reply,
                "pending_change": pending,
                "executed": False,
                "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
            }

        if change_action in {"add", "create", "insert"}:
            new_task_name = str(
                change.get("new_task_name")
                or (change.get("updates", {}) or {}).get("task")
                or change.get("task")
                or ""
            ).strip()
            if not new_task_name:
                reply = "我识别到你要新增任务，但缺少任务名。请补充，例如：新增任务：灰度验收。"
                history = history[-29:]
                history.append({"role": "user", "content": text, "ts": user_ts})
                history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
                save_project_assistant_state(project_id, chat_history=history)
                return {
                    "status": "ok",
                    "reply": reply,
                    "pending_change": pending,
                    "executed": False,
                    "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
                }

            try:
                add_duration_days = max(1, int(change.get("duration_days", 7) or 7))
            except (TypeError, ValueError):
                add_duration_days = 7
            add_owner = str(change.get("owner", "")).strip() or "Unassigned"

            pending_change = {
                "kind": "task_add",
                "task": new_task_name,
                "duration_days": add_duration_days,
                "owner": add_owner,
                "need_reschedule": bool(change.get("need_reschedule", True)),
                "requested_at": _now_iso(),
            }
            save_project_assistant_state(project_id, pending_change=pending_change)
            preview = (
                f"已生成待执行新增：任务名={new_task_name}，duration_days={add_duration_days}，owner={add_owner}。"
                "\n\n回复“确认执行”才会落盘，回复“取消执行”可撤销这次修改。"
            )
            reply = ((memory_note + "\n\n") if memory_note else "") + ((answer + "\n\n") if answer else "") + preview
            history = history[-29:]
            history.append({"role": "user", "content": text, "ts": user_ts})
            history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
            save_project_assistant_state(project_id, chat_history=history)
            return {
                "status": "ok",
                "reply": reply,
                "pending_change": pending_change,
                "executed": False,
                "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
            }

        if change_action in {"delete", "remove"}:
            task_id = _normalize_task_ref_text(change.get("task_id"))
            task_name = str(change.get("task_name", "")).strip()
            if not task_id and task_name and re.fullmatch(rf"{_TASK_REF_TOKEN_PATTERN}", task_name, flags=re.IGNORECASE):
                task_id = _normalize_task_ref_text(task_name)
                task_name = ""
            if not task_id and not task_name:
                reply = "我识别到你要删除任务，但没有定位到目标。请补充任务序号（如 1.3）或完整任务名。"
                history = history[-29:]
                history.append({"role": "user", "content": text, "ts": user_ts})
                history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
                save_project_assistant_state(project_id, chat_history=history)
                return {
                    "status": "ok",
                    "reply": reply,
                    "pending_change": pending,
                    "executed": False,
                    "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
                }

            pending_change = {
                "kind": "task_delete",
                "task_id": task_id,
                "task_name": task_name,
                "need_reschedule": bool(change.get("need_reschedule", True)),
                "requested_at": _now_iso(),
            }
            save_project_assistant_state(project_id, pending_change=pending_change)
            target = task_id or task_name
            preview = (
                f"已生成待执行删除：目标任务={target}。"
                "\n\n回复“确认执行”才会落盘，回复“取消执行”可撤销这次修改。"
            )
            reply = ((memory_note + "\n\n") if memory_note else "") + ((answer + "\n\n") if answer else "") + preview
            history = history[-29:]
            history.append({"role": "user", "content": text, "ts": user_ts})
            history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
            save_project_assistant_state(project_id, chat_history=history)
            return {
                "status": "ok",
                "reply": reply,
                "pending_change": pending_change,
                "executed": False,
                "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
            }

        updates, err = _sanitize_update_fields(change.get("updates", {}))
        if err:
            reply = f"我理解到你想修改任务，但参数还不完整：{err}"
            history = history[-29:]
            history.append({"role": "user", "content": text, "ts": user_ts})
            history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
            save_project_assistant_state(project_id, chat_history=history)
            return {
                "status": "ok",
                "reply": reply,
                "pending_change": pending,
                "executed": False,
                "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
            }

        task_id = _normalize_task_ref_text(change.get("task_id"))
        task_name = str(change.get("task_name", "")).strip()
        if not task_id and task_name and re.fullmatch(rf"{_TASK_REF_TOKEN_PATTERN}", task_name, flags=re.IGNORECASE):
            task_id = _normalize_task_ref_text(task_name)
            task_name = ""
        if not task_id and not task_name:
            reply = "我识别到你要修改任务，但没有定位到具体任务。请补充 task_id（如 T3）或完整任务名。"
            history = history[-29:]
            history.append({"role": "user", "content": text, "ts": user_ts})
            history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
            save_project_assistant_state(project_id, chat_history=history)
            return {
                "status": "ok",
                "reply": reply,
                "pending_change": pending,
                "executed": False,
                "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
            }

        pending_change = {
            "kind": "task_update",
            "task_id": task_id,
            "task_name": task_name,
            "updates": updates,
            "need_reschedule": bool(change.get("need_reschedule", False)),
            "requested_at": _now_iso(),
        }
        save_project_assistant_state(project_id, pending_change=pending_change)

        target = pending_change["task_id"] or pending_change["task_name"] or "(未识别目标任务)"
        schedule_label = _updates_schedule_window_label(updates)
        preview = (
            (
                f"已生成待执行修改：目标任务={target}，排期改为 {schedule_label}"
                if schedule_label
                else f"已生成待执行修改：目标任务={target}，更新={updates}"
            )
            + ("，执行后会重排期。" if pending_change["need_reschedule"] or _has_schedule_update_fields(updates) else "。")
            + "\n\n回复“确认执行”才会落盘，回复“取消执行”可撤销这次修改。"
        )
        reply = ((memory_note + "\n\n") if memory_note else "") + ((answer + "\n\n" if answer else "") + preview)

        history = history[-29:]
        history.append({"role": "user", "content": text, "ts": user_ts})
        history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
        save_project_assistant_state(project_id, chat_history=history)
        return {
            "status": "ok",
            "reply": reply,
            "pending_change": pending_change,
            "executed": False,
            "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
        }

    if intent == "replan" or _looks_like_replan_request(text):
        replan_result = _replan_from_feedback(project_id, text)
        reply = replan_result.get("message", "已尝试重生成计划。")
        if memory_note:
            reply = memory_note + "\n\n" + reply
        history = history[-29:]
        history.append({"role": "user", "content": text, "ts": user_ts})
        history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
        save_project_assistant_state(project_id, chat_history=history)
        return {
            "status": replan_result.get("status", "ok"),
            "reply": reply,
            "pending_change": assistant_state.get("pending_change"),
            "executed": replan_result.get("status") == "ok",
            "summary_snippets": snippets,
            "plan_rebuilt": replan_result.get("status") == "ok",
            "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
        }

    if intent == "clarify":
        clarification = str(model_obj.get("clarification", "")).strip()
        reply = answer or clarification or "我需要更多信息才能执行这一步。"
    else:
        reply = answer or "我已读取当前项目数据，但没有形成有效回答，请补充问题细节。"

    if memory_note:
        reply = memory_note + "\n\n" + reply

    history = history[-29:]
    history.append({"role": "user", "content": text, "ts": user_ts})
    history.append({"role": "assistant", "content": reply, "ts": _now_iso()})
    save_project_assistant_state(project_id, chat_history=history)

    return {
        "status": "ok",
        "reply": reply,
        "pending_change": assistant_state.get("pending_change"),
        "executed": False,
        "summary_snippets": snippets,
        "memory_updates": _memory_updates_payload(added_system_entries, added_project_entries),
    }

