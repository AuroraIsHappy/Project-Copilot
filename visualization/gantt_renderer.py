import html
from datetime import date
from datetime import datetime
from datetime import timedelta


GANTT_THEMES: dict[str, dict] = {
  "default": {
    "label": "默认",
    "description": "默认配色，清晰区分进度状态。",
    "status_colors": {
      "Done": "#10b981",
      "In Progress": "#0ea5e9",
      "Planned": "#f59e0b",
      "At Risk": "#ef4444",
      "KR Summary": "#94a3b8",
    },
    "default_status_color": "#64748b",
    "task_bar_color": "#0ea5e9",
    "kr_palette": ["#dbeafe", "#dcfce7", "#fef3c7", "#fce7f3", "#ede9fe", "#e0f2fe"],
  },
  "minimal": {
    "label": "简约",
    "description": "低饱和极简风格。",
    "status_colors": {
      "Done": "#334155",
      "In Progress": "#475569",
      "Planned": "#64748b",
      "At Risk": "#0f172a",
      "KR Summary": "#94a3b8",
    },
    "default_status_color": "#64748b",
    "task_bar_color": "#64748b",
    "kr_palette": ["#f8fafc", "#f1f5f9", "#e2e8f0", "#e5e7eb", "#f3f4f6", "#e9edf2"],
  },
  "retro": {
    "label": "复古",
    "description": "较深的彩色方案，带一点复古海报感。",
    "status_colors": {
      "Done": "#3f7d6b",
      "In Progress": "#2f5d8a",
      "Planned": "#b36a2e",
      "At Risk": "#8a3c3c",
      "KR Summary": "#6b7280",
    },
    "default_status_color": "#6b7280",
    "task_bar_color": "#2f5d8a",
    "kr_palette": ["#d8c3a5", "#cdb08a", "#c6a57f", "#d2b391", "#bfa37a", "#ceb08a"],
  },
}

_STATUS_SWATCH_ORDER = ("Done", "In Progress", "Planned", "At Risk")
_STATUS_ALIASES = {
  "done": "Done",
  "in progress": "In Progress",
  "planned": "Planned",
  "at risk": "At Risk",
  "kr summary": "KR Summary",
}


def _normalize_status(status: str) -> str:
  text = str(status or "").strip().lower().replace("_", " ").replace("-", " ")
  text = " ".join(text.split())
  return _STATUS_ALIASES.get(text, str(status or "").strip())


def _build_theme_swatches(theme_config: dict) -> list[str]:
  return [_status_color(status, theme_config) for status in _STATUS_SWATCH_ORDER]


def list_gantt_themes() -> dict[str, dict]:
  return {
    key: {
      "label": str(value.get("label", key)),
      "description": str(value.get("description", "")),
      "swatches": _build_theme_swatches(value),
    }
    for key, value in GANTT_THEMES.items()
  }


def _resolve_theme(theme: str) -> dict:
  normalized = str(theme or "").strip().lower()
  return GANTT_THEMES.get(normalized, GANTT_THEMES["default"])


def _to_date(value) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value:
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _fmt_date(value) -> str:
    parsed = _to_date(value)
    return parsed.isoformat() if parsed else ""


def _status_color(status: str, theme_config: dict) -> str:
  mapping = theme_config.get("status_colors", {})
  canonical_status = _normalize_status(status)
  return mapping.get(canonical_status, str(theme_config.get("default_status_color", "#64748b")))


def _task_bar_color(theme_config: dict) -> str:
  raw = theme_config.get("task_bar_color")
  if isinstance(raw, str) and raw.strip():
    return raw.strip()
  status_mapping = theme_config.get("status_colors", {})
  fallback = status_mapping.get("In Progress")
  if isinstance(fallback, str) and fallback.strip():
    return fallback.strip()
  return str(theme_config.get("default_status_color", "#64748b"))


def _hex_to_rgb(color: str) -> tuple[int, int, int] | None:
  text = str(color or "").strip()
  if not text:
    return None
  if text.startswith("#"):
    text = text[1:]

  if len(text) == 3:
    text = "".join(ch * 2 for ch in text)
  if len(text) != 6:
    return None

  try:
    return (int(text[0:2], 16), int(text[2:4], 16), int(text[4:6], 16))
  except ValueError:
    return None


def _rgb_to_hex(rgb: tuple[int, int, int]) -> str:
  r, g, b = rgb
  return f"#{r:02x}{g:02x}{b:02x}"


def _normalize_hex_color(color: str) -> str | None:
  rgb = _hex_to_rgb(color)
  if not rgb:
    return None
  return _rgb_to_hex(rgb)


def _mix_rgb(source: tuple[int, int, int], target: tuple[int, int, int], ratio: float) -> tuple[int, int, int]:
  ratio = max(0.0, min(1.0, float(ratio)))
  return (
    int(round(source[0] + (target[0] - source[0]) * ratio)),
    int(round(source[1] + (target[1] - source[1]) * ratio)),
    int(round(source[2] + (target[2] - source[2]) * ratio)),
  )


def _expand_theme_kr_palette(kr_count: int, theme_config: dict) -> list[str]:
  if kr_count <= 0:
    return []

  raw_palette = theme_config.get("kr_palette") or []
  base_palette: list[str] = []
  seen: set[str] = set()
  for raw in raw_palette:
    normalized = _normalize_hex_color(str(raw))
    if not normalized or normalized in seen:
      continue
    seen.add(normalized)
    base_palette.append(normalized)

  if not base_palette:
    base_palette = ["#dbeafe", "#dcfce7", "#fef3c7", "#fce7f3", "#ede9fe", "#e0f2fe"]
    seen = set(base_palette)

  if kr_count <= len(base_palette):
    return base_palette[:kr_count]

  expanded = list(base_palette)
  white = (255, 255, 255)
  black = (0, 0, 0)
  tint_steps = (0.10, 0.18, 0.26, 0.34)
  shade_steps = (0.08, 0.16, 0.24, 0.32)

  round_idx = 0
  while len(expanded) < kr_count and round_idx < 12:
    tint_ratio = tint_steps[round_idx % len(tint_steps)]
    shade_ratio = shade_steps[round_idx % len(shade_steps)]
    for color in base_palette:
      rgb = _hex_to_rgb(color)
      if not rgb:
        continue

      if len(expanded) < kr_count:
        tinted = _rgb_to_hex(_mix_rgb(rgb, white, tint_ratio))
        if tinted not in seen:
          seen.add(tinted)
          expanded.append(tinted)

      if len(expanded) < kr_count:
        shaded = _rgb_to_hex(_mix_rgb(rgb, black, shade_ratio))
        if shaded not in seen:
          seen.add(shaded)
          expanded.append(shaded)

    round_idx += 1

  fallback_idx = 0
  while len(expanded) < kr_count:
    source = base_palette[fallback_idx % len(base_palette)]
    source_rgb = _hex_to_rgb(source) or (219, 234, 254)
    depth = (fallback_idx // len(base_palette)) + 1
    ratio = min(0.92, depth * 0.06)
    target = white if fallback_idx % 2 == 0 else black
    candidate = _rgb_to_hex(_mix_rgb(source_rgb, target, ratio))
    if candidate not in seen:
      seen.add(candidate)
      expanded.append(candidate)
    fallback_idx += 1

  return expanded[:kr_count]


def _unique_kr_palette(kr_count: int, theme_config: dict) -> list[str]:
  return _expand_theme_kr_palette(kr_count, theme_config)


def _to_progress_percent(raw_progress, status: str = "") -> int:
    try:
        value = float(raw_progress)
    except (TypeError, ValueError):
        value = 0.0

    if value <= 1:
        value *= 100

    progress = max(0, min(100, int(round(value))))
    if str(status or "").strip() == "Done":
        return 100
    return progress



def _kr_color(kr_name: str, theme_config: dict) -> str:
  palette = theme_config.get("kr_palette") or ["#dbeafe", "#dcfce7", "#fef3c7", "#fce7f3", "#ede9fe", "#e0f2fe"]
  idx = sum(ord(ch) for ch in kr_name) % len(palette)
  return palette[idx]


def _calc_height(task_count: int) -> int:
    rows = max(1, task_count)
    return max(300, min(980, 120 + rows * 50))


def create_gantt(tasks: list[dict], objective: str = "", theme: str = "default") -> dict:
    valid_dates = []
    for task in tasks:
        start = _to_date(task.get("start"))
        end = _to_date(task.get("end"))
        if start and end:
            valid_dates.extend([start, end])

    if not tasks or not valid_dates:
        empty_html = """
        <div style='border:1px solid #e5e7eb;border-radius:12px;background:#fff;padding:18px;color:#6b7280;'>
            No timeline data available.
        </div>
        """
        return {"html": empty_html, "height": 180}

    theme_config = _resolve_theme(theme)

    min_start = min(valid_dates)
    max_end = max(valid_dates)
    total_days = max(7, (max_end - min_start).days + 1)
    total_weeks = max(1, (total_days + 6) // 7)
    week_pct = (7 / total_days) * 100

    axis_ticks = []
    for week in range(1, total_weeks + 1, 4):
      week_start_day = (week - 1) * 7
      week_end_day = min(total_days, week_start_day + 7)
      week_span_days = max(0.0, float(week_end_day - week_start_day))
      center_day = week_start_day + (week_span_days / 2)
      center_pct = (center_day / total_days) * 100
      width_pct = (week_span_days / total_days) * 100
      # Only annotate weeks that are between two divider lines.
      if week < total_weeks:
        axis_ticks.append((week, center_pct, width_pct))

    axis_ticks_html = "".join(
      f"<span class='axis-tick' style='left:{left:.2f}%;width:{width:.2f}%;'><span class='axis-tick-text'>W{week}</span></span>"
      for week, left, width in axis_ticks
    )

    key_ticks_json = "[" + ",".join(
        f'{{"week":{week},"leftPct":{left:.4f}}}'
        for week, left, _ in axis_ticks
    ) + "]"
    all_week_ticks_json = "[" + ",".join(
      f'{{"week":{week},"leftPct":{(((week - 1) * 7 / total_days) * 100):.4f}}}'
      for week in range(1, total_weeks + 1)
    ) + "]"

    kr_names_in_order: list[str] = []
    seen_kr_names: set[str] = set()
    for task in tasks:
      if str(task.get("status", "")) != "KR Summary":
        continue
      kr_name = str(task.get("kr", task.get("task", "KR")))
      if kr_name in seen_kr_names:
        continue
      seen_kr_names.add(kr_name)
      kr_names_in_order.append(kr_name)

    kr_palette = _unique_kr_palette(len(kr_names_in_order), theme_config)
    kr_color_map = {
      name: kr_palette[index]
      for index, name in enumerate(kr_names_in_order)
    }
    task_fill_color = _task_bar_color(theme_config)

    rows_html = []
    for task in tasks:
        start = _to_date(task.get("start"))
        end = _to_date(task.get("end"))
        if not start or not end:
            continue

        is_kr_row = str(task.get("status", "")) == "KR Summary"
        task_name = html.escape(str(task.get("task", "")))
        kr_name = str(task.get("kr", task.get("task", "KR")))
        kr_key = html.escape(kr_name, quote=True)

        if is_kr_row:
            kr_background = kr_color_map.get(kr_name, _kr_color(kr_name, theme_config))
            rows_html.append(
                f"""
          <div class='gantt-row kr-row' data-kr='{kr_key}' style='background:{kr_background};'>
                    <div class='gantt-label'>
              <button class='kr-toggle' type='button' data-kr='{kr_key}' aria-expanded='true' title='折叠/展开该 KR 的子任务'>
                          <span class='kr-triangle' aria-hidden='true'>
                            <svg viewBox='0 0 20 20' focusable='false'>
                              <path d='M6 4L14 10L6 16Z'></path>
                            </svg>
                          </span>
                <span class='task-name kr-name'>{task_name}</span>
              </button>
                    </div>
                    <div class='gantt-track kr-track'></div>
                </div>
                """
            )
            continue

        offset_days = max(0, (start - min_start).days)
        duration_days = max(1, (end - start).days)
        start_week = max(1, offset_days // 7 + 1)
        label_end_date = start if end <= start else (end - timedelta(days=1))
        end_week = max(start_week, ((label_end_date - min_start).days) // 7 + 1)
        left_pct = (offset_days / total_days) * 100
        width_pct = max(8.0, (duration_days / total_days) * 100)

        status_text = str(task.get("status", "Planned"))
        progress_percent = _to_progress_percent(task.get("progress", 0), status_text)
        bar_label = html.escape(f"W{start_week}-W{end_week} | {progress_percent}%")
        date_tooltip = html.escape(f"{_fmt_date(start)} to {_fmt_date(end)}")
        fill_color = task_fill_color

        rows_html.append(
            f"""
          <div class='gantt-row subtask-row' data-kr='{kr_key}'>
                <div class='gantt-label'>
                    <div class='task-name'>{task_name}</div>
                </div>
                <div class='gantt-track'>
              <div class='gantt-bar' title='{date_tooltip}' style='left:{left_pct:.2f}%;width:{width_pct:.2f}%;'>
                <div class='gantt-bar-fill' style='width:{progress_percent:.2f}%;background:{fill_color};'></div>
                <span class='bar-label'>{bar_label}</span>
                    </div>
                </div>
            </div>
            """
        )

    title_text = html.escape(objective) if objective else "OKR Weekly Timeline"
    container_height = _calc_height(len(tasks))

    html_content = f"""
    <style>
      .gantt-wrap {{
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        background: #ffffff;
        overflow: auto;
        padding: 12px;
        font-family: 'IBM Plex Sans', sans-serif;
        position: relative;
      }}
      .gantt-toolbar {{
        position: sticky;
        top: 0;
        z-index: 2;
        display: flex;
        align-items: center;
        justify-content: flex-end;
        gap: 8px;
        padding: 2px 0 10px 0;
        background: #ffffff;
      }}
      .gantt-zoom-btn {{
        border: 1px solid #d1d5db;
        background: #ffffff;
        color: #334155;
        border-radius: 6px;
        font-size: 12px;
        font-weight: 600;
        line-height: 1;
        height: 28px;
        min-width: 32px;
        padding: 0 10px;
        cursor: pointer;
      }}
      .gantt-zoom-btn:hover {{
        background: #f8fafc;
      }}
      .gantt-zoom-range {{
        width: 120px;
        accent-color: #64748b;
      }}
      .gantt-zoom-readout {{
        color: #64748b;
        font-size: 12px;
        font-weight: 600;
        min-width: 42px;
        text-align: right;
      }}
      .gantt-canvas {{
        min-width: calc(510px + 18px + 1680px);
        transform-origin: top left;
      }}
      .gantt-title {{
        position: relative;
        z-index: 1;
        font-weight: 700;
        color: #111827;
        margin: 2px 0 14px 0;
        font-size: 15px;
        text-align: center;
        background: #eff6ff;
        border: 1px solid #dbeafe;
        border-radius: 10px;
        padding: 10px 12px;
      }}
      .gantt-row {{
        position: relative;
        z-index: 1;
        display: grid;
        grid-template-columns: minmax(510px, 1.1fr) minmax(1680px, 5.6fr);
        gap: 18px;
        align-items: start;
        margin: 8px 0;
      }}
      .axis-row {{
        margin-top: 0;
        margin-bottom: 10px;
      }}
      .axis-label {{
        color: #94a3b8;
        font-size: 11px;
        font-weight: 600;
        padding-top: 2px;
      }}
      .timeline-axis {{
        position: relative;
        height: 28px;
        border-top: 1px solid #e5e7eb;
        border-bottom: 1px solid #e5e7eb;
        background:
          repeating-linear-gradient(
            to right,
            transparent,
            transparent calc({week_pct:.5f}% - 1px),
            #edf2f7 calc({week_pct:.5f}% - 1px),
            #edf2f7 {week_pct:.5f}%
          );
      }}
      .axis-tick {{
        position: absolute;
        top: 4px;
        transform: translateX(-50%);
        height: 18px;
        display: flex;
        align-items: center;
        justify-content: center;
        box-sizing: border-box;
        font-size: 11px;
        color: #64748b;
        font-weight: 600;
        white-space: nowrap;
        background: #e5e7eb;
        border: 1px solid #cbd5e1;
        border-radius: 0;
        padding: 0;
      }}
      .axis-tick-text {{
        position: relative;
        left: 2px;
      }}
      .kr-row {{
        border-radius: 10px;
        padding: 8px 10px;
        margin-top: 16px;
        margin-bottom: 10px;
      }}
      .gantt-label {{
        min-width: 0;
      }}
      .task-name {{
        color: #111827;
        font-size: 13px;
        font-weight: 600;
        white-space: normal;
        word-break: break-word;
        line-height: 1.35;
      }}
      .kr-name {{
        font-size: 14px;
        font-weight: 700;
      }}
      .kr-toggle {{
        display: inline-flex;
        align-items: center;
        gap: 6px;
        border: none;
        background: transparent;
        padding: 0;
        margin: 0;
        cursor: pointer;
        text-align: left;
      }}
      .kr-triangle {{
        width: 16px;
        height: 16px;
        color: #1f2937;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        user-select: none;
        transition: transform 0.15s ease;
      }}
      .kr-triangle svg {{
        width: 14px;
        height: 14px;
        fill: currentColor;
        transform: rotate(90deg);
      }}
      .kr-toggle[aria-expanded='false'] .kr-triangle svg {{
        transform: rotate(0deg);
      }}
      .gantt-track {{
        position: relative;
        height: 30px;
        border-radius: 8px;
        border: none;
        background: transparent;
      }}
      .subtask-row {{
        position: relative;
        padding-bottom: 6px;
        margin-bottom: 6px;
      }}
      .subtask-row::after {{
        content: '';
        position: absolute;
        left: 0;
        right: 0;
        bottom: 0;
        height: 1px;
        background: #f1f5f9;
      }}
      .kr-track {{
        display: none;
      }}
      .gantt-bar {{
        position: absolute;
        top: 2px;
        height: 26px;
        border-radius: 10px;
        background: #e5e7eb;
        border: 1px solid #d1d5db;
        display: flex;
        align-items: center;
        padding: 0 8px;
        min-width: 110px;
        overflow: hidden;
        box-sizing: border-box;
      }}
      .gantt-bar-fill {{
        position: absolute;
        top: 0;
        left: 0;
        bottom: 0;
        border-radius: 9px;
        min-width: 0;
      }}
      .bar-label {{
        color: #111827;
        font-size: 11px;
        font-weight: 600;
        white-space: nowrap;
        position: relative;
        z-index: 1;
      }}
      @media (max-width: 960px) {{
        .gantt-canvas {{
          min-width: calc(330px + 18px + 1080px);
        }}
        .gantt-row {{
          grid-template-columns: minmax(330px, 1fr) minmax(1080px, 3.4fr);
        }}
      }}
    </style>
    <div class='gantt-wrap'>
      <div class='gantt-toolbar' role='group' aria-label='Gantt zoom controls'>
        <button type='button' class='gantt-zoom-btn' data-zoom-action='out' title='缩小'>-</button>
        <button type='button' class='gantt-zoom-btn' data-zoom-action='reset' title='重置缩放'>100%</button>
        <button type='button' class='gantt-zoom-btn' data-zoom-action='in' title='放大'>+</button>
        <input class='gantt-zoom-range' type='range' min='40' max='140' step='5' value='100' aria-label='缩放比例'>
        <span class='gantt-zoom-readout'>100%</span>
      </div>
      <div class='gantt-canvas'>
        <div class='gantt-title'>{title_text}</div>
        <div class='gantt-row axis-row'>
          <div class='gantt-label axis-label'>Week Axis</div>
          <div class='gantt-track timeline-axis'>
            {axis_ticks_html}
          </div>
        </div>
        {''.join(rows_html)}
      </div>
    </div>
    <script>
      (function() {{
        const scriptEl = document.currentScript;
        const wrap = scriptEl ? scriptEl.previousElementSibling : null;
        if (!wrap || !wrap.classList || !wrap.classList.contains('gantt-wrap')) return;
        if (!wrap) return;
        const canvas = wrap.querySelector('.gantt-canvas');
        const zoomRange = wrap.querySelector('.gantt-zoom-range');
        const zoomReadout = wrap.querySelector('.gantt-zoom-readout');
        const zoomInBtn = wrap.querySelector("[data-zoom-action='in']");
        const zoomOutBtn = wrap.querySelector("[data-zoom-action='out']");
        const zoomResetBtn = wrap.querySelector("[data-zoom-action='reset']");
        const supportsCssZoom = 'zoom' in document.documentElement.style;

        const MIN_ZOOM = 0.4;
        const MAX_ZOOM = 1.4;
        const ZOOM_STEP = 0.1;
        let zoomLevel = 1;

        function clampZoom(value) {{
          if (!Number.isFinite(value)) return 1;
          return Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, value));
        }}

        function setZoom(nextZoom, redraw = true) {{
          zoomLevel = clampZoom(nextZoom);
          const pct = Math.round(zoomLevel * 100);
          if (canvas) {{
            if (supportsCssZoom) {{
              canvas.style.zoom = String(zoomLevel);
              canvas.style.transform = 'none';
            }} else {{
              canvas.style.transform = 'scale(' + zoomLevel.toFixed(2) + ')';
            }}
          }}
          if (zoomRange) zoomRange.value = String(pct);
          if (zoomReadout) zoomReadout.textContent = pct + '%';
          if (redraw) requestAnimationFrame(drawWeekLines);
        }}

        const toggles = wrap.querySelectorAll('.kr-toggle');
        toggles.forEach((btn) => {{
          btn.addEventListener('click', () => {{
            const kr = btn.getAttribute('data-kr');
            if (!kr) return;

            const expanded = btn.getAttribute('aria-expanded') !== 'false';
            const nextExpanded = !expanded;
            btn.setAttribute('aria-expanded', nextExpanded ? 'true' : 'false');

            const tri = btn.querySelector('.kr-triangle');
            if (tri) tri.setAttribute('data-expanded', nextExpanded ? 'true' : 'false');

            const subRows = Array.from(wrap.querySelectorAll('.subtask-row')).filter(
              (row) => row.getAttribute('data-kr') === kr
            );
            subRows.forEach((row) => {{
              row.style.display = nextExpanded ? 'grid' : 'none';
            }});
            requestAnimationFrame(drawWeekLines);
          }});
        }});

        if (zoomRange) {{
          zoomRange.addEventListener('input', (event) => {{
            const val = Number(event.target.value);
            setZoom(val / 100);
          }});
        }}

        if (zoomInBtn) {{
          zoomInBtn.addEventListener('click', () => setZoom(zoomLevel + ZOOM_STEP));
        }}

        if (zoomOutBtn) {{
          zoomOutBtn.addEventListener('click', () => setZoom(zoomLevel - ZOOM_STEP));
        }}

        if (zoomResetBtn) {{
          zoomResetBtn.addEventListener('click', () => setZoom(1));
        }}

        function drawWeekLines() {{
          const axis = wrap.querySelector('.timeline-axis');
          if (!axis) return;
          const existing = wrap.querySelector('.gantt-week-overlay');
          if (existing) existing.remove();
          const wrapRect = wrap.getBoundingClientRect();
          const axisRect = axis.getBoundingClientRect();
          const axisBottom = axisRect.bottom - wrapRect.top + wrap.scrollTop;
          const axisLeft = axisRect.left - wrapRect.left + wrap.scrollLeft;
          const axisWidth = axisRect.width;
          const overlayHeight = Math.max(0, wrap.scrollHeight - axisBottom);
          const overlay = document.createElement('div');
          overlay.className = 'gantt-week-overlay';
          overlay.style.cssText = 'position:absolute;top:' + axisBottom + 'px;left:' + axisLeft + 'px;width:' + axisWidth + 'px;height:' + overlayHeight + 'px;pointer-events:none;z-index:0;';
          const keyWeeks = {key_ticks_json};
          const allWeeks = {all_week_ticks_json};
          const keyWeekSet = new Set(keyWeeks.map((kw) => kw.week));
          allWeeks.forEach(function(w) {{
            const line = document.createElement('div');
            const lPx = (w.leftPct / 100) * axisWidth;
            const isKeyWeek = keyWeekSet.has(w.week);
            const width = isKeyWeek ? 0.7 : 1;
            const color = isKeyWeek ? 'rgba(100,116,139,0.36)' : '#f1f5f9';
            const left = isKeyWeek ? Math.max(0, lPx - (width / 2)) : Math.max(0, Math.round(lPx));
            line.style.cssText = 'position:absolute;top:0;height:100%;left:' + left + 'px;width:' + width + 'px;background:' + color + ';';
            overlay.appendChild(line);
          }});
          wrap.insertBefore(overlay, wrap.firstChild);
        }}
        setZoom(1, false);
        setTimeout(drawWeekLines, 0);
        window.addEventListener('resize', drawWeekLines);
      }})();
    </script>
    """

    return {"html": html_content, "height": container_height}
