from pathlib import Path
import re
import shutil
import subprocess
import zipfile
import xml.etree.ElementTree as ET

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover - graceful fallback when dependency is missing
    PdfReader = None


_TEXT_SUFFIXES = {".txt", ".md", ".markdown", ".csv", ".log", ".json", ".docx", ".pdf", ".xlsx", ".doc"}


def _strip_wrapping_quotes(text: str) -> str:
    normalized = str(text or "").strip()
    quote_pairs = (("'", "'"), ('"', '"'), ("‘", "’"), ("“", "”"))

    changed = True
    while changed and len(normalized) >= 2:
        changed = False
        for left, right in quote_pairs:
            if normalized.startswith(left) and normalized.endswith(right):
                normalized = normalized[len(left): len(normalized) - len(right)].strip()
                changed = True
                break

    return normalized


def normalize_summary_folder_path(folder) -> str:
    folder_text = _strip_wrapping_quotes(str(folder or "").strip())
    if not folder_text:
        return ""

    try:
        folder_path = Path(folder_text).expanduser()
    except (OSError, RuntimeError, ValueError):
        return folder_text

    try:
        return str(folder_path.resolve(strict=False))
    except OSError:
        return str(folder_path)


def _resolve_summary_folder_path(folder) -> Path | None:
    normalized = normalize_summary_folder_path(folder)
    if not normalized:
        return None

    try:
        return Path(normalized)
    except (OSError, RuntimeError, ValueError):
        return None


def _xml_local_name(tag) -> str:
    text = str(tag or "")
    if "}" in text:
        return text.rsplit("}", 1)[-1]
    return text


def _looks_binary(raw_bytes: bytes) -> bool:
    if not raw_bytes:
        return False

    if b"\x00" in raw_bytes:
        return True

    sample = raw_bytes[:4096]
    control_count = sum(1 for b in sample if b < 9 or (13 < b < 32))
    return control_count > max(8, len(sample) // 8)


def _read_text_file(file_path: Path) -> str:
    try:
        raw = file_path.read_bytes()
    except OSError:
        return ""

    if _looks_binary(raw):
        return ""

    for encoding in ("utf-8-sig", "utf-8", "gb18030", "gbk", "utf-16", "utf-16-le", "utf-16-be"):
        try:
            text = raw.decode(encoding=encoding)
            if text.strip():
                return text
        except UnicodeDecodeError:
            continue

    return ""


def _read_docx_file(file_path: Path) -> str:
    """Read text from .docx without external dependencies."""
    try:
        with zipfile.ZipFile(file_path, "r") as docx_zip:
            xml_bytes = docx_zip.read("word/document.xml")
    except (OSError, KeyError, zipfile.BadZipFile):
        return ""

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return ""

    paragraphs: list[str] = []
    for para in root.iter():
        if _xml_local_name(getattr(para, "tag", "")) != "p":
            continue

        parts: list[str] = []
        for node in para.iter():
            local_name = _xml_local_name(getattr(node, "tag", ""))
            if local_name == "t" and node.text:
                parts.append(node.text)
            elif local_name == "tab":
                parts.append("\t")
            elif local_name in {"br", "cr"}:
                parts.append("\n")

        line = "".join(parts).strip()
        if line:
            paragraphs.append(line)

    return "\n".join(paragraphs).strip()


def _xlsx_shared_strings(xlsx_zip: zipfile.ZipFile) -> list[str]:
    try:
        xml_bytes = xlsx_zip.read("xl/sharedStrings.xml")
    except KeyError:
        return []

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []

    ns = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    values: list[str] = []
    for si in root.findall(".//main:si", ns):
        parts = [node.text for node in si.findall(".//main:t", ns) if node.text]
        values.append("".join(parts).strip())
    return values


def _xlsx_sheet_refs(xlsx_zip: zipfile.ZipFile) -> list[tuple[str, str]]:
    try:
        workbook_xml = xlsx_zip.read("xl/workbook.xml")
    except KeyError:
        workbook_xml = b""

    refs: list[tuple[str, str]] = []
    if workbook_xml:
        try:
            workbook_root = ET.fromstring(workbook_xml)
            rels_root = ET.fromstring(xlsx_zip.read("xl/_rels/workbook.xml.rels"))
        except (ET.ParseError, KeyError):
            workbook_root = None
            rels_root = None

        if workbook_root is not None and rels_root is not None:
            ns_workbook = {
                "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
                "rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
            }
            ns_pkg = {"pkg": "http://schemas.openxmlformats.org/package/2006/relationships"}

            rid_to_target: dict[str, str] = {}
            for rel in rels_root.findall(".//pkg:Relationship", ns_pkg):
                rid = str(rel.attrib.get("Id") or "").strip()
                target = str(rel.attrib.get("Target") or "").strip()
                if rid and target:
                    rid_to_target[rid] = target

            rel_key = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
            for sheet in workbook_root.findall(".//main:sheets/main:sheet", ns_workbook):
                sheet_name = str(sheet.attrib.get("name") or "Sheet").strip() or "Sheet"
                rid = str(sheet.attrib.get(rel_key) or "").strip()
                target = rid_to_target.get(rid, "")
                if not target:
                    continue

                target = target.lstrip("/")
                if not target.startswith("xl/"):
                    target = f"xl/{target}"
                refs.append((sheet_name, target))

    if refs:
        return refs

    # Fallback for minimally structured workbooks.
    fallback_refs: list[tuple[str, str]] = []
    for name in sorted(xlsx_zip.namelist()):
        if not name.startswith("xl/worksheets/") or not name.endswith(".xml"):
            continue
        sheet_name = Path(name).stem
        fallback_refs.append((sheet_name, name))
    return fallback_refs


def _read_xlsx_sheet_rows(xlsx_zip: zipfile.ZipFile, sheet_path: str, shared_strings: list[str]) -> list[str]:
    try:
        xml_bytes = xlsx_zip.read(sheet_path)
    except KeyError:
        return []

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []

    ns = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    lines: list[str] = []

    for row in root.findall(".//main:sheetData/main:row", ns):
        row_values: list[str] = []
        for cell in row.findall("main:c", ns):
            cell_type = str(cell.attrib.get("t") or "").strip()
            value = ""

            if cell_type == "inlineStr":
                inline_node = cell.find("main:is", ns)
                if inline_node is not None:
                    parts = [node.text for node in inline_node.findall(".//main:t", ns) if node.text]
                    value = "".join(parts)
            else:
                value_node = cell.find("main:v", ns)
                if value_node is None or value_node.text is None:
                    value = ""
                else:
                    raw_value = str(value_node.text).strip()
                    if cell_type == "s":
                        try:
                            shared_idx = int(raw_value)
                            value = shared_strings[shared_idx] if 0 <= shared_idx < len(shared_strings) else ""
                        except (TypeError, ValueError):
                            value = ""
                    elif cell_type == "b":
                        value = "TRUE" if raw_value == "1" else "FALSE"
                    else:
                        value = raw_value

            value = " ".join(value.split())
            if value:
                row_values.append(value)

        if row_values:
            lines.append(" | ".join(row_values))

    return lines


def _read_xlsx_file(file_path: Path) -> str:
    """Read text-like content from .xlsx without external dependencies."""
    try:
        with zipfile.ZipFile(file_path, "r") as xlsx_zip:
            shared_strings = _xlsx_shared_strings(xlsx_zip)
            sheet_refs = _xlsx_sheet_refs(xlsx_zip)

            output_lines: list[str] = []
            for sheet_name, sheet_path in sheet_refs:
                rows = _read_xlsx_sheet_rows(xlsx_zip, sheet_path, shared_strings)
                if not rows:
                    continue
                output_lines.append(f"[{sheet_name}]")
                output_lines.extend(rows)

            return "\n".join(output_lines).strip()
    except (OSError, zipfile.BadZipFile):
        return ""


def _extract_ascii_chunks(raw_bytes: bytes) -> list[str]:
    chunks: list[str] = []
    for match in re.finditer(rb"[ -~]{5,}", raw_bytes):
        text = match.group().decode("ascii", errors="ignore").strip()
        if text:
            chunks.append(text)
    return chunks


def _extract_utf16le_ascii_chunks(raw_bytes: bytes) -> list[str]:
    chunks: list[str] = []
    for match in re.finditer(rb"(?:[\x20-\x7E]\x00){5,}", raw_bytes):
        try:
            text = match.group().decode("utf-16-le", errors="ignore").strip()
        except UnicodeDecodeError:
            continue
        if text:
            chunks.append(text)
    return chunks


def _run_external_doc_converter(file_path: Path) -> str:
    for binary in ("antiword", "catdoc"):
        if not shutil.which(binary):
            continue

        try:
            completed = subprocess.run(
                [binary, str(file_path)],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="ignore",
                timeout=20,
            )
        except Exception:
            continue

        content = (completed.stdout or "").strip()
        if content:
            return content

    return ""


def _read_doc_file(file_path: Path) -> str:
    """Best-effort reader for legacy .doc files."""
    # Handle mis-labeled OOXML files (some systems save .doc extension by mistake).
    if zipfile.is_zipfile(file_path):
        return _read_docx_file(file_path)

    external_text = _run_external_doc_converter(file_path)
    if external_text:
        return external_text

    try:
        raw = file_path.read_bytes()
    except OSError:
        return ""

    if not raw:
        return ""

    candidates = _extract_utf16le_ascii_chunks(raw)
    candidates.extend(_extract_ascii_chunks(raw))

    cleaned_lines: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        text = " ".join(candidate.split())
        if len(text) < 6:
            continue
        if text in seen:
            continue
        seen.add(text)
        cleaned_lines.append(text)
        if len(cleaned_lines) >= 300:
            break

    return "\n".join(cleaned_lines).strip()


def _read_pdf_file(file_path: Path) -> str:
    """Read text from .pdf via pypdf, skipping encrypted/unreadable files."""
    if PdfReader is None:
        return ""

    try:
        reader = PdfReader(str(file_path))
    except Exception:
        return ""

    if getattr(reader, "is_encrypted", False):
        return ""

    pages_text: list[str] = []
    for page in getattr(reader, "pages", []):
        try:
            text = page.extract_text() or ""
        except Exception:
            continue
        cleaned = text.strip()
        if cleaned:
            pages_text.append(cleaned)

    return "\n".join(pages_text).strip()


def load_summaries_with_names(folder) -> list[tuple[str, str]]:
    """Return list of (relative_filename, content) for all readable summary files."""
    folder_path = _resolve_summary_folder_path(folder)
    if folder_path is None or not folder_path.exists() or not folder_path.is_dir():
        return []

    results: list[tuple[str, str]] = []
    seen_names: set[str] = set()
    for file in sorted(folder_path.rglob("*")):
        if not file.is_file():
            continue
        if file.suffix.lower() not in _TEXT_SUFFIXES:
            continue

        # Normalize to POSIX-style separators so incremental records are stable across OSes.
        rel_name = file.relative_to(folder_path).as_posix()
        if rel_name in seen_names:
            continue
        seen_names.add(rel_name)

        suffix = file.suffix.lower()
        if suffix == ".docx":
            content = _read_docx_file(file)
        elif suffix == ".pdf":
            content = _read_pdf_file(file)
        elif suffix == ".xlsx":
            content = _read_xlsx_file(file)
        elif suffix == ".doc":
            content = _read_doc_file(file)
        else:
            content = _read_text_file(file)
        if content.strip():
            results.append((rel_name, content))

    return results


def load_summaries(folder) -> list[str]:
    """Return list of summary contents (backward-compat wrapper)."""
    return [content for _, content in load_summaries_with_names(folder)]


def _extract_query_terms(question: str, limit: int = 12) -> list[str]:
    q = (question or "").lower().strip()
    if not q:
        return []

    # Keep both English tokens and contiguous Chinese chunks.
    english = re.findall(r"[a-z0-9_\-]{2,}", q)
    chinese = re.findall(r"[\u4e00-\u9fff]{2,}", q)
    terms: list[str] = []
    for term in english + chinese:
        if term not in terms:
            terms.append(term)
        if len(terms) >= limit:
            break
    return terms


def _highlight_text(text: str, terms: list[str], max_chars: int = 700) -> str:
    if not text:
        return ""

    lower = text.lower()
    first_hit = -1
    first_term = ""
    for term in terms:
        idx = lower.find(term.lower())
        if idx >= 0 and (first_hit < 0 or idx < first_hit):
            first_hit = idx
            first_term = term

    if first_hit < 0:
        snippet = text[:max_chars]
    else:
        start = max(0, first_hit - max_chars // 3)
        end = min(len(text), start + max_chars)
        snippet = text[start:end]
        # mark the first hit for quick visual scan
        local_lower = snippet.lower()
        local_idx = local_lower.find(first_term.lower())
        if local_idx >= 0:
            local_end = local_idx + len(first_term)
            snippet = snippet[:local_idx] + "<<" + snippet[local_idx:local_end] + ">>" + snippet[local_end:]

    return snippet + ("..." if len(text) > len(snippet) else "")


def find_summary_snippets(
    folder: str,
    question: str,
    max_files: int = 5,
    max_chars: int = 700,
) -> list[dict]:
    """Find relevant summary snippets for a question.

    Returns list of dict:
      {
        "filename": str,
        "score": int,
        "match_terms": list[str],
        "snippet": str
      }
    """
    pairs = load_summaries_with_names(folder)
    if not pairs:
        return []

    terms = _extract_query_terms(question)
    if not terms:
        terms = [str(question or "").strip().lower()]

    scored: list[dict] = []
    for filename, content in pairs:
        text = content or ""
        lower_filename = filename.lower()
        lower_text = text.lower()
        score = 0
        hit_terms: list[str] = []

        for term in terms:
            t = term.lower().strip()
            if not t:
                continue
            if t in lower_filename:
                score += 4
                if term not in hit_terms:
                    hit_terms.append(term)
            if t in lower_text:
                score += 1
                if term not in hit_terms:
                    hit_terms.append(term)

        # Keep mildly relevant files too, so user can inspect evidence context.
        if score <= 0 and terms:
            continue

        scored.append(
            {
                "filename": filename,
                "score": score,
                "match_terms": hit_terms,
                "snippet": _highlight_text(text, hit_terms or terms, max_chars=max_chars),
            }
        )

    scored.sort(key=lambda item: int(item.get("score", 0)), reverse=True)
    return scored[:max_files]