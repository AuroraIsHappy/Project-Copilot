from __future__ import annotations

import os
from pathlib import Path

from utils.helpers import ensure_dir


_ROOT_MEMORY_DIR = ensure_dir(Path(__file__).resolve().parents[1] / "memory")
_PROJECT_MEMORY_ROOT = ensure_dir(_ROOT_MEMORY_DIR / "projects")
_SYSTEM_SECTION_TITLES = {
    "preferences": "User Preferences",
    "aliases": "Project Aliases And Terms",
    "facts": "Stable Facts",
}
_PROJECT_SECTION_TITLE = "Project Important Information"
_DEFAULT_SYSTEM_MEMORY_MAX_CHARS = 5000
_DEFAULT_PROJECT_MEMORY_MAX_CHARS = 3000
_MIN_COMPRESSION_TARGET_CHARS = 240


def system_memory_file() -> Path:
    return _ROOT_MEMORY_DIR / "MEMORY.md"


def _project_memory_dir(project_id: str) -> Path:
    return ensure_dir(_PROJECT_MEMORY_ROOT / str(project_id).strip())


def project_memory_file(project_id: str) -> Path:
    return _project_memory_dir(project_id) / "MEMORY.md"


def _default_system_memory_text() -> str:
    return (
        "# System Memory\n\n"
        "## User Preferences\n"
        "- (empty)\n\n"
        "## Project Aliases And Terms\n"
        "- (empty)\n\n"
        "## Stable Facts\n"
        "- (empty)\n"
    )


def _default_project_memory_text() -> str:
    return (
        "# Project Memory\n\n"
        f"## {_PROJECT_SECTION_TITLE}\n"
        "- (empty)\n"
    )


def _safe_positive_int(value: str | None, default: int) -> int:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _system_memory_max_chars() -> int:
    return _safe_positive_int(
        os.getenv("OKR_SYSTEM_MEMORY_MAX_CHARS"),
        _DEFAULT_SYSTEM_MEMORY_MAX_CHARS,
    )


def _project_memory_max_chars() -> int:
    return _safe_positive_int(
        os.getenv("OKR_PROJECT_MEMORY_MAX_CHARS"),
        _DEFAULT_PROJECT_MEMORY_MAX_CHARS,
    )


def _collapse_markdown_lines(lines: list[str]) -> str:
    kept_lines = [line.rstrip() for line in lines if isinstance(line, str)]
    compacted: list[str] = []
    prev_blank = False
    for line in kept_lines:
        is_blank = not line.strip()
        if is_blank and prev_blank:
            continue
        compacted.append(line)
        prev_blank = is_blank
    text = "\n".join(compacted).strip()
    return (text + "\n") if text else ""


def _strip_markdown_fence(text: str) -> str:
    stripped = str(text or "").strip()
    if not stripped.startswith("```"):
        return stripped
    lines = stripped.splitlines()
    if lines and lines[0].lstrip().startswith("```"):
        lines = lines[1:]
    while lines and not lines[-1].strip():
        lines.pop()
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _truncate_text(text: str, max_len: int) -> str:
    value = str(text or "").strip()
    if max_len <= 0:
        return ""
    if len(value) <= max_len:
        return value
    if max_len <= 3:
        return value[:max_len]
    return value[: max_len - 3].rstrip() + "..."


def _compress_memory_markdown_with_llm(content: str, target_chars: int, max_chars: int) -> str | None:
    source = str(content or "").strip()
    if not source:
        return None

    try:
        from utils.llm_client import call_llm_messages
    except Exception:
        return None

    system_prompt = (
        "You are a memory compression assistant. "
        "Rewrite the memory markdown to be concise while preserving all unique long-term facts. "
        "Do not remove entries just because they are old. "
        "Delete only semantically duplicated or near-duplicated items, and shorten language. "
        "Keep markdown format, keep section headings, keep bullet style, and output markdown only."
    )

    current = source
    for _ in range(2):
        try:
            raw = call_llm_messages(
                [
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": (
                            f"Original length: {len(current)} chars.\n"
                            f"Target length: <= {target_chars} chars (about half of original).\n"
                            f"Hard limit: <= {max_chars} chars.\n"
                            "Rules: remove semantic duplicates, compress wording, keep unique facts.\n\n"
                            "Memory markdown:\n"
                            f"{current}"
                        ),
                    },
                ],
                inject_system_memory=False,
            )
        except Exception:
            return None

        candidate = _strip_markdown_fence(raw)
        candidate = _collapse_markdown_lines(candidate.splitlines())
        if not candidate:
            return None
        if len(candidate) >= len(current):
            break
        current = candidate
        if len(current) <= target_chars:
            break

    if len(current) >= len(source):
        return None
    return current


def _compress_memory_without_dropping_items(content: str, target_chars: int, max_chars: int) -> str:
    source = str(content or "").strip()
    if not source:
        return ""

    lines = source.splitlines()
    seen_bullets: set[str] = set()
    normalized_lines: list[str] = []

    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped:
            normalized_lines.append("")
            continue
        if stripped.startswith("#"):
            normalized_lines.append(stripped)
            continue
        if stripped.startswith("- "):
            payload = " ".join(stripped[2:].split())
            if not payload:
                continue
            dedup_key = payload.lower()
            if dedup_key in seen_bullets:
                continue
            seen_bullets.add(dedup_key)
            normalized_lines.append(f"- {payload}")
            continue
        normalized_lines.append(" ".join(stripped.split()))

    compacted = _collapse_markdown_lines(normalized_lines)
    if not compacted:
        return ""
    if len(compacted) <= max_chars and len(compacted) <= target_chars:
        return compacted

    target_total = max(120, min(target_chars, max_chars))
    lines_for_budget = compacted.rstrip("\n").splitlines()
    editable_idx = [
        idx
        for idx, line in enumerate(lines_for_budget)
        if line.strip() and not line.lstrip().startswith("#")
    ]

    if editable_idx:
        fixed_cost = sum(
            len(line) + 1
            for line in lines_for_budget
            if not line.strip() or line.lstrip().startswith("#")
        )
        budget = max(12, (target_total - fixed_cost) // len(editable_idx))
        for idx in editable_idx:
            line = lines_for_budget[idx].strip()
            if line.startswith("- "):
                body = _truncate_text(line[2:], max(8, budget - 2))
                lines_for_budget[idx] = f"- {body}" if body else "-"
            else:
                lines_for_budget[idx] = _truncate_text(line, budget)

    compacted = _collapse_markdown_lines(lines_for_budget)
    if len(compacted) <= max_chars:
        return compacted

    hard_limited = compacted[:max_chars].rstrip()
    return (hard_limited + "\n") if hard_limited else ""


def _simplify_memory_markdown(content: str, max_chars: int) -> str:
    text = str(content or "").strip()
    if not text:
        return ""
    normalized = text + "\n"
    if len(normalized) <= max_chars:
        return normalized

    target_chars = max(_MIN_COMPRESSION_TARGET_CHARS, min(max_chars, len(normalized) // 2))

    llm_compacted = _compress_memory_markdown_with_llm(normalized, target_chars, max_chars)
    if llm_compacted:
        compacted = _collapse_markdown_lines(llm_compacted.splitlines())
    else:
        compacted = ""

    if not compacted or len(compacted) > max_chars:
        compacted = _compress_memory_without_dropping_items(normalized, target_chars, max_chars)

    if len(compacted) <= max_chars:
        return compacted

    hard_limited = compacted[:max_chars].rstrip()
    return (hard_limited + "\n") if hard_limited else ""


def _write_memory_markdown(file_path: Path, content: str, *, default_text: str, max_chars: int) -> Path:
    text = str(content or "").strip()
    rendered = (text + "\n") if text else default_text
    if len(rendered) > max_chars:
        rendered = _simplify_memory_markdown(rendered, max_chars)
    if not str(rendered or "").strip():
        rendered = default_text
    file_path.write_text(rendered, encoding="utf-8")
    return file_path


def ensure_system_memory() -> Path:
    file_path = system_memory_file()
    if not file_path.exists():
        file_path.write_text(_default_system_memory_text(), encoding="utf-8")
    return file_path


def read_system_memory() -> str:
    file_path = ensure_system_memory()
    return file_path.read_text(encoding="utf-8")


def write_system_memory(content: str) -> Path:
    file_path = ensure_system_memory()
    return _write_memory_markdown(
        file_path,
        content,
        default_text=_default_system_memory_text(),
        max_chars=_system_memory_max_chars(),
    )


def ensure_project_memory(project_id: str) -> Path:
    file_path = project_memory_file(project_id)
    if not file_path.exists():
        file_path.write_text(_default_project_memory_text(), encoding="utf-8")
    return file_path


def read_project_memory(project_id: str) -> str:
    file_path = ensure_project_memory(project_id)
    return file_path.read_text(encoding="utf-8")


def write_project_memory(project_id: str, content: str) -> Path:
    file_path = ensure_project_memory(project_id)
    return _write_memory_markdown(
        file_path,
        content,
        default_text=_default_project_memory_text(),
        max_chars=_project_memory_max_chars(),
    )


def _parse_system_memory_sections(content: str) -> dict[str, list[str]]:
    current_key = ""
    sections = {key: [] for key in _SYSTEM_SECTION_TITLES}

    for line in (content or "").splitlines():
        stripped = line.strip()
        if stripped == f"## {_SYSTEM_SECTION_TITLES['preferences']}":
            current_key = "preferences"
            continue
        if stripped == f"## {_SYSTEM_SECTION_TITLES['aliases']}":
            current_key = "aliases"
            continue
        if stripped == f"## {_SYSTEM_SECTION_TITLES['facts']}":
            current_key = "facts"
            continue
        if current_key and stripped.startswith("- "):
            item = stripped[2:].strip()
            if item and item != "(empty)":
                sections[current_key].append(item)

    return sections


def _render_system_memory_sections(sections: dict[str, list[str]]) -> str:
    parts = ["# System Memory", ""]
    for key in ("preferences", "aliases", "facts"):
        parts.append(f"## {_SYSTEM_SECTION_TITLES[key]}")
        values = [item.strip() for item in sections.get(key, []) if str(item).strip()]
        if values:
            for item in values:
                parts.append(f"- {item}")
        else:
            parts.append("- (empty)")
        parts.append("")
    return "\n".join(parts).rstrip() + "\n"


def _parse_project_memory_items(content: str) -> list[str]:
    items: list[str] = []
    inside_section = False
    for line in (content or "").splitlines():
        stripped = line.strip()
        if stripped == f"## {_PROJECT_SECTION_TITLE}":
            inside_section = True
            continue
        if inside_section and stripped.startswith("## "):
            inside_section = False
        if inside_section and stripped.startswith("- "):
            item = stripped[2:].strip()
            if item and item != "(empty)":
                items.append(item)
    return items


def _render_project_memory_items(items: list[str]) -> str:
    parts = ["# Project Memory", "", f"## {_PROJECT_SECTION_TITLE}"]
    cleaned = [item.strip() for item in items if str(item).strip()]
    if cleaned:
        for item in cleaned:
            parts.append(f"- {item}")
    else:
        parts.append("- (empty)")
    return "\n".join(parts).rstrip() + "\n"


def upsert_system_memory(entries: dict[str, list[str]]) -> dict[str, list[str]]:
    """Insert shared memory entries into root MEMORY.md and return only newly added items."""
    current = read_system_memory()
    sections = _parse_system_memory_sections(current)
    added = {key: [] for key in _SYSTEM_SECTION_TITLES}

    for key in _SYSTEM_SECTION_TITLES:
        existing_normalized = {item.strip().lower() for item in sections.get(key, [])}
        for raw in entries.get(key, []) if isinstance(entries.get(key, []), list) else []:
            item = str(raw or "").strip()
            if not item:
                continue
            norm = item.lower()
            if norm in existing_normalized:
                continue
            sections[key].append(item)
            added[key].append(item)
            existing_normalized.add(norm)

    write_system_memory(_render_system_memory_sections(sections))
    return added


def upsert_project_memory(project_id: str, items: list[str]) -> list[str]:
    """Insert project-specific important information and return only newly added items."""
    current = read_project_memory(project_id)
    existing_items = _parse_project_memory_items(current)
    existing_normalized = {item.strip().lower() for item in existing_items}
    added: list[str] = []

    for raw in items if isinstance(items, list) else []:
        item = str(raw or "").strip()
        if not item:
            continue
        norm = item.lower()
        if norm in existing_normalized:
            continue
        existing_items.append(item)
        existing_normalized.add(norm)
        added.append(item)

    write_project_memory(project_id, _render_project_memory_items(existing_items))
    return added


def reclassify_memory_item(project_id: str, item: str, target_scope: str) -> bool:
    """Move one memory item to target scope: 'system' or 'project'.

    - target_scope='system': remove from project memory, add to system facts
    - target_scope='project': remove from system sections, add to project memory
    """
    value = str(item or "").strip()
    scope = str(target_scope or "").strip().lower()
    if not value or scope not in {"system", "project"}:
        return False

    changed = False

    # Remove from system memory sections if present.
    system_sections = _parse_system_memory_sections(read_system_memory())
    for key in ("preferences", "aliases", "facts"):
        before = len(system_sections[key])
        system_sections[key] = [x for x in system_sections[key] if x.strip().lower() != value.lower()]
        if len(system_sections[key]) != before:
            changed = True
    write_system_memory(_render_system_memory_sections(system_sections))

    # Remove from project memory if present.
    project_items = _parse_project_memory_items(read_project_memory(project_id))
    filtered_project_items = [x for x in project_items if x.strip().lower() != value.lower()]
    if len(filtered_project_items) != len(project_items):
        changed = True
    write_project_memory(project_id, _render_project_memory_items(filtered_project_items))

    # Add into target scope.
    if scope == "system":
        added = upsert_system_memory({"facts": [value], "preferences": [], "aliases": []})
        return changed or bool(added.get("facts"))

    added_project = upsert_project_memory(project_id, [value])
    return changed or bool(added_project)