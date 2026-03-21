import html
from collections import defaultdict
from collections import deque
from datetime import date


NODE_W = 224
NODE_H = 92
MILESTONE_W = 88
MILESTONE_H = 36
MAX_COLUMNS_PER_BAND = 4
COLUMN_GAP = 150
ROW_GAP = 116
BAND_GAP_Y = 88
BAND_HEADER_H = 54
BAND_SIDE_PAD = 42
MARGIN_X = 56
MARGIN_Y = 32
VIEWPORT_HEIGHT = 620
EXPORT_FONT_STACK = "'Microsoft YaHei', 'Microsoft YaHei UI', 'PingFang SC', 'Noto Sans CJK SC', 'SimHei', 'SimSun', 'Arial Unicode MS', sans-serif"


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


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


def _duration_days(task: dict) -> int:
    raw_weeks = task.get("duration_weeks")
    if raw_weeks is None:
        raw_weeks = task.get("duration")
    if raw_weeks is not None:
        try:
            weeks = max(1, int(raw_weeks))
            return weeks * 7
        except (TypeError, ValueError):
            pass

    raw_days = task.get("duration_days")
    try:
        days = max(1, int(raw_days))
    except (TypeError, ValueError):
        return 7

    weeks = max(1, (days + 6) // 7)
    return weeks * 7


def _constraint_weight(dep: dict, duration_a: int, duration_b: int) -> int:
    dep_type = str(dep.get("type") or "FS").upper().strip()
    raw_lag_weeks = dep.get("lag_weeks")
    if raw_lag_weeks is None:
        raw_lag_weeks = _days_to_weeks(dep.get("lag_days", 0), allow_negative=True)
    lag_days = _safe_int(raw_lag_weeks, 0) * 7

    raw_overlap_weeks = dep.get("overlap_weeks")
    if raw_overlap_weeks is None:
        raw_overlap_weeks = dep.get("overlap")
    if raw_overlap_weeks is None:
        raw_overlap_weeks = _days_to_weeks(dep.get("overlap_days", 0), allow_negative=False)
    overlap_days = max(0, _safe_int(raw_overlap_weeks, 0)) * 7

    if dep_type == "SS":
        return lag_days - overlap_days
    if dep_type == "FF":
        return duration_a - duration_b + lag_days - overlap_days
    if dep_type == "SF":
        return -duration_b + lag_days - overlap_days
    return duration_a + lag_days - overlap_days


def _safe_date(value) -> date | None:
    if not value:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _date_rank_value(task: dict) -> tuple[int, str]:
    parsed = _safe_date(task.get("start"))
    if parsed is None:
        return 10**9, ""
    return parsed.toordinal(), parsed.isoformat()


def _task_sort_key(task: dict) -> tuple[int, int, int, str, str]:
    date_rank, _ = _date_rank_value(task)
    return (
        date_rank,
        _safe_int(task.get("kr_index"), 10**6),
        _safe_int(task.get("subtask_index"), 10**6),
        str(task.get("end", "")),
        str(task.get("task") or task.get("task_name") or ""),
    )


def _short_label(label: str, limit: int = 30) -> str:
    compact = " ".join(str(label or "").split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 1].rstrip() + "..."


def _format_window(task: dict) -> str:
    start = _safe_date(task.get("start"))
    end = _safe_date(task.get("end"))
    if start and end:
        return f"{start.isoformat()} -> {end.isoformat()}"
    if start:
        return f"Start {start.isoformat()}"
    if end:
        return f"Finish {end.isoformat()}"
    return "Schedule pending"


def _build_graph_maps(
    task_by_id: dict[str, dict], dependencies: list[dict]
) -> tuple[
    dict[str, set[str]],
    dict[str, set[str]],
    dict[str, list[tuple[str, dict]]],
    dict[str, int],
]:
    node_ids = list(task_by_id.keys())
    predecessors: dict[str, set[str]] = {nid: set() for nid in node_ids}
    successors: dict[str, set[str]] = {nid: set() for nid in node_ids}
    adjacency: dict[str, list[tuple[str, dict]]] = {nid: [] for nid in node_ids}
    indegree: dict[str, int] = {nid: 0 for nid in node_ids}

    for dep in dependencies:
        source = str(dep.get("from") or "")
        target = str(dep.get("to") or "")
        if source not in task_by_id or target not in task_by_id or source == target:
            continue
        if target in successors[source]:
            continue
        predecessors[target].add(source)
        successors[source].add(target)
        adjacency[source].append((target, dep))
        indegree[target] += 1

    return predecessors, successors, adjacency, indegree


def _topological_order(task_by_id: dict[str, dict], dependencies: list[dict]) -> tuple[list[str], bool]:
    _, _, adjacency, indegree = _build_graph_maps(task_by_id, dependencies)
    sort_order = sorted(task_by_id.keys(), key=lambda tid: _task_sort_key(task_by_id[tid]))
    queue: deque[str] = deque([tid for tid in sort_order if indegree[tid] == 0])
    topo_order: list[str] = []

    while queue:
        current = queue.popleft()
        topo_order.append(current)
        for nxt, _ in sorted(adjacency[current], key=lambda item: _task_sort_key(task_by_id[item[0]])):
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)

    has_cycle = len(topo_order) != len(task_by_id)
    if has_cycle:
        return sort_order, True
    return topo_order, False


def _layout_nodes(
    tasks: list[dict],
    dependencies: list[dict],
    node_w: int,
    node_h: int,
) -> tuple[dict[str, dict], list[dict], list[str], list[str], bool, int, int]:
    sorted_tasks = sorted(tasks, key=_task_sort_key)
    task_by_id = {
        str(task.get("task_id") or f"T{idx + 1}"): task
        for idx, task in enumerate(sorted_tasks)
    }
    if not task_by_id:
        return {}, [], [], [], False, 0, 0

    predecessors, successors, _, _ = _build_graph_maps(task_by_id, dependencies)
    topo_order, has_cycle = _topological_order(task_by_id, dependencies)
    original_index = {tid: idx for idx, tid in enumerate(topo_order)}

    unique_dates: list[str] = []
    for task in sorted_tasks:
        _, date_text = _date_rank_value(task)
        if date_text and date_text not in unique_dates:
            unique_dates.append(date_text)
    date_rank = {date_text: idx for idx, date_text in enumerate(unique_dates)}

    column_map: dict[str, int] = {}
    for tid in topo_order:
        task = task_by_id[tid]
        _, date_text = _date_rank_value(task)
        base_col = date_rank.get(date_text, 0)
        pred_col = max((column_map[pred] + 1 for pred in predecessors[tid]), default=0)
        column_map[tid] = max(base_col, pred_col) + 1

    max_column = max(column_map.values(), default=1)
    band_count = max(1, ((max_column - 1) // MAX_COLUMNS_PER_BAND) + 1)
    column_step = node_w + COLUMN_GAP
    band_inner_w = node_w + (MAX_COLUMNS_PER_BAND - 1) * column_step
    band_total_w = band_inner_w + BAND_SIDE_PAD * 2
    start_lane_x = MARGIN_X
    finish_lane_x = MARGIN_X + band_total_w + 96
    band_left = MARGIN_X + BAND_SIDE_PAD + MILESTONE_W + 28

    band_task_ids: dict[int, list[str]] = defaultdict(list)
    for tid in topo_order:
        band_index = (column_map[tid] - 1) // MAX_COLUMNS_PER_BAND
        band_task_ids[band_index].append(tid)

    band_tops: dict[int, int] = {}
    current_top = MARGIN_Y
    for band_index in range(band_count):
        band_tops[band_index] = current_top
        ids = band_task_ids.get(band_index, [])

        def _band_score(task_id: str) -> tuple[float, int, int]:
            pred_rows = [original_index[pred] for pred in predecessors[task_id] if pred in original_index]
            pred_score = sum(pred_rows) / len(pred_rows) if pred_rows else original_index.get(task_id, 0)
            local_col = (column_map[task_id] - 1) % MAX_COLUMNS_PER_BAND
            return pred_score, local_col, original_index.get(task_id, 0)

        ids.sort(key=_band_score)
        band_height = BAND_HEADER_H + max(1, len(ids)) * ROW_GAP + 24
        current_top += band_height + BAND_GAP_Y

    positions: dict[str, dict] = {}
    column_headers: list[dict] = []
    band_midpoints: dict[int, float] = {}

    for band_index in range(band_count):
        ids = band_task_ids.get(band_index, [])
        top = band_tops[band_index]
        direction = 1 if band_index % 2 == 0 else -1
        if ids:
            y_values: list[float] = []
            for row, tid in enumerate(ids):
                global_col = column_map[tid]
                local_col = (global_col - 1) % MAX_COLUMNS_PER_BAND
                display_col = local_col if direction == 1 else (MAX_COLUMNS_PER_BAND - 1 - local_col)
                x = band_left + display_col * column_step
                y = top + BAND_HEADER_H + row * ROW_GAP
                task = task_by_id[tid]
                positions[tid] = {
                    "id": tid,
                    "x": x,
                    "y": y,
                    "w": node_w,
                    "h": node_h,
                    "label": str(task.get("task") or task.get("task_name") or tid),
                    "kr": str(task.get("kr") or "KR"),
                    "window": _format_window(task),
                    "duration": max(1, _duration_days(task) // 7),
                    "type": "task",
                    "band": band_index,
                    "column": global_col,
                    "local_col": local_col,
                    "direction": direction,
                }
                y_values.append(y + node_h / 2)
            band_midpoints[band_index] = sum(y_values) / len(y_values)
        else:
            band_midpoints[band_index] = top + BAND_HEADER_H + ROW_GAP / 2

        for local_col in range(MAX_COLUMNS_PER_BAND):
            global_col = band_index * MAX_COLUMNS_PER_BAND + local_col + 1
            tasks_in_col = [tid for tid in ids if column_map[tid] == global_col]
            if not tasks_in_col:
                continue
            display_col = local_col if direction == 1 else (MAX_COLUMNS_PER_BAND - 1 - local_col)
            col_x = band_left + display_col * column_step + node_w / 2
            start_dates = [
                _date_rank_value(task_by_id[tid])[1]
                for tid in tasks_in_col
                if _date_rank_value(task_by_id[tid])[1]
            ]
            label = min(start_dates) if start_dates else f"Step {global_col}"
            column_headers.append(
                {
                    "x": col_x,
                    "y": top + 10,
                    "label": label,
                    "line_x": col_x,
                    "band_top": top + BAND_HEADER_H - 6,
                    "band_bottom": top + BAND_HEADER_H + max(1, len(ids)) * ROW_GAP - 18,
                }
            )

    entry_ids = [tid for tid in topo_order if not predecessors[tid]]
    exit_ids = [tid for tid in topo_order if not successors[tid]]
    first_band_mid = band_midpoints.get(0, MARGIN_Y + BAND_HEADER_H)
    last_band_mid = band_midpoints.get(band_count - 1, first_band_mid)
    entry_mid = sum((positions[tid]["y"] + node_h / 2 for tid in entry_ids), 0.0) / max(len(entry_ids), 1)
    exit_mid = sum((positions[tid]["y"] + node_h / 2 for tid in exit_ids), 0.0) / max(len(exit_ids), 1)

    positions["__start__"] = {
        "id": "Start",
        "x": start_lane_x,
        "y": int(round(min(first_band_mid, entry_mid) - MILESTONE_H / 2)),
        "w": MILESTONE_W,
        "h": MILESTONE_H,
        "label": "Start",
        "kr": "",
        "window": "",
        "duration": 0,
        "type": "milestone",
        "band": 0,
    }
    positions["__finish__"] = {
        "id": "Finish",
        "x": finish_lane_x,
        "y": int(round(max(last_band_mid, exit_mid) - MILESTONE_H / 2)),
        "w": MILESTONE_W,
        "h": MILESTONE_H,
        "label": "Finish",
        "kr": "",
        "window": "",
        "duration": 0,
        "type": "milestone",
        "band": band_count - 1,
    }

    canvas_w = finish_lane_x + MILESTONE_W + MARGIN_X
    canvas_h = current_top - BAND_GAP_Y + MARGIN_Y
    return positions, column_headers, entry_ids, exit_ids, has_cycle, canvas_w, canvas_h


def _critical_path(task_by_id: dict[str, dict], dependencies: list[dict]) -> tuple[set[str], set[tuple[str, str]], bool]:
    node_ids = list(task_by_id.keys())
    adjacency: dict[str, list[tuple[str, dict]]] = {nid: [] for nid in node_ids}
    indegree: dict[str, int] = {nid: 0 for nid in node_ids}

    for dep in dependencies:
        source = str(dep.get("from") or "")
        target = str(dep.get("to") or "")
        if source not in task_by_id or target not in task_by_id or source == target:
            continue
        adjacency[source].append((target, dep))
        indegree[target] += 1

    queue: deque[str] = deque(sorted([nid for nid, deg in indegree.items() if deg == 0]))
    topo_order: list[str] = []
    while queue:
        cur = queue.popleft()
        topo_order.append(cur)
        for nxt, _ in adjacency[cur]:
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)

    has_cycle = len(topo_order) != len(node_ids)
    if has_cycle:
        return set(), set(), True

    start_dist: dict[str, int] = {nid: 0 for nid in node_ids}
    predecessor: dict[str, str] = {}

    for nid in topo_order:
        for nxt, dep in adjacency[nid]:
            dur_a = _duration_days(task_by_id[nid])
            dur_b = _duration_days(task_by_id[nxt])
            weight = _constraint_weight(dep, dur_a, dur_b)
            candidate = start_dist[nid] + weight
            if candidate > start_dist[nxt]:
                start_dist[nxt] = candidate
                predecessor[nxt] = nid

    finish_dist = {nid: start_dist[nid] + _duration_days(task_by_id[nid]) for nid in node_ids}
    end_node = max(finish_dist, key=finish_dist.get)

    critical_nodes: set[str] = {end_node}
    critical_edges: set[tuple[str, str]] = set()
    cursor = end_node
    while cursor in predecessor:
        prev = predecessor[cursor]
        critical_edges.add((prev, cursor))
        critical_nodes.add(prev)
        cursor = prev

    return critical_nodes, critical_edges, False


def _compact_edge_label(dep: dict | None) -> str:
    if dep is None:
        return ""
    dep_type = str(dep.get("type") or "FS").upper().strip()
    raw_lag_weeks = dep.get("lag_weeks")
    if raw_lag_weeks is None:
        raw_lag_weeks = _days_to_weeks(dep.get("lag_days", 0), allow_negative=True)
    lag_weeks = _safe_int(raw_lag_weeks, 0)

    raw_overlap_weeks = dep.get("overlap_weeks")
    if raw_overlap_weeks is None:
        raw_overlap_weeks = dep.get("overlap")
    if raw_overlap_weeks is None:
        raw_overlap_weeks = _days_to_weeks(dep.get("overlap_days", 0), allow_negative=False)
    overlap_weeks = max(0, _safe_int(raw_overlap_weeks, 0))

    parts: list[str] = []
    if dep_type != "FS":
        parts.append(dep_type)
    if lag_weeks:
        parts.append(f"lag {lag_weeks:+d}w")
    if overlap_weeks:
        parts.append(f"overlap {overlap_weeks}w")
    return "  ".join(parts)


def _orthogonal_edge(source_node: dict, target_node: dict, lane_index: int) -> dict[str, object]:
    source_right = source_node["x"] + source_node["w"]
    source_y = source_node["y"] + source_node["h"] / 2
    target_left = target_node["x"]
    target_y = target_node["y"] + target_node["h"] / 2

    # Keep arrow semantics stable: always leave predecessor from the right and enter successor from the left.
    start = (source_right, source_y)
    end = (target_left, target_y)
    exit_x = start[0] + 18
    entry_x = end[0] - 18

    if end[0] >= start[0]:
        min_x = start[0]
        max_x = end[0]
        lane_x = min(max_x - 26, max(min_x + 26, (min_x + max_x) / 2 + lane_index * 16))
    else:
        # For wrapped rows, detour to the right-side corridor before coming back to the next band.
        lane_x = start[0] + 120 + lane_index * 18

    points = [
        start,
        (exit_x, start[1]),
        (lane_x, start[1]),
        (lane_x, end[1]),
        (entry_x, end[1]),
        end,
    ]
    label_x = lane_x
    label_y = (start[1] + end[1]) / 2 - 12
    return {
        "points": points,
        "label": (label_x, label_y),
    }


def _polyline_path(points: list[tuple[float, float]]) -> str:
    return " ".join(
        [f"M {points[0][0]:.1f} {points[0][1]:.1f}"]
        + [f"L {x:.1f} {y:.1f}" for x, y in points[1:]]
    )


def create_dependency_graph(tasks: list[dict], dependencies: list[dict]) -> dict:
    if not tasks:
        return {
            "html": "<div style='border:1px solid #e5e7eb;border-radius:12px;padding:16px;color:#6b7280;background:#fff;'>No task data available.</div>",
            "height": 180,
            "svg": "",
            "svg_width": 0,
            "svg_height": 0,
        }

    if not dependencies:
        return {
            "html": "<div style='border:1px solid #e5e7eb;border-radius:12px;padding:16px;color:#6b7280;background:#fff;'>No dependency data available yet. Generate a plan with dependency links to see the graph.</div>",
            "height": 180,
            "svg": "",
            "svg_width": 0,
            "svg_height": 0,
        }

    task_by_id = {
        str(task.get("task_id") or f"T{idx + 1}"): task
        for idx, task in enumerate(tasks)
    }
    positions, column_headers, entry_ids, exit_ids, layout_has_cycle, max_x, max_y = _layout_nodes(
        tasks,
        dependencies,
        NODE_W,
        NODE_H,
    )
    if not positions:
        return {
            "html": "<div style='border:1px solid #e5e7eb;border-radius:12px;padding:16px;color:#6b7280;background:#fff;'>No valid nodes to draw.</div>",
            "height": 180,
            "svg": "",
            "svg_width": 0,
            "svg_height": 0,
        }

    critical_nodes, critical_edges, critical_has_cycle = _critical_path(task_by_id, dependencies)
    has_cycle = layout_has_cycle or critical_has_cycle

    render_edges: list[tuple[str, str, dict | None, bool]] = []
    for dep in dependencies:
        source = str(dep.get("from") or "")
        target = str(dep.get("to") or "")
        if source not in positions or target not in positions:
            continue
        render_edges.append((source, target, dep, False))

    for tid in entry_ids:
        render_edges.append(("__start__", tid, None, True))
    for tid in exit_ids:
        render_edges.append((tid, "__finish__", None, True))

    route_counters: dict[tuple[str, int, int], int] = defaultdict(int)
    edges_svg: list[str] = []
    for source, target, dep, synthetic in render_edges:
        source_node = positions[source]
        target_node = positions[target]
        min_band = min(int(source_node.get("band", 0)), int(target_node.get("band", 0)))
        max_band = max(int(source_node.get("band", 0)), int(target_node.get("band", 0)))
        direction = "forward" if (source_node["x"] + source_node["w"]) <= target_node["x"] else "wrapped"
        route_key = (direction, min_band, max_band)
        lane_index = route_counters[route_key]
        route_counters[route_key] += 1

        edge_geometry = _orthogonal_edge(source_node, target_node, lane_index)
        path_data = _polyline_path(edge_geometry["points"])
        label_x, label_y = edge_geometry["label"]
        is_critical = (source, target) in critical_edges
        stroke = "#d32f2f" if is_critical else "#5b6472"
        width = "2.8" if is_critical else "1.8"
        dash = "6 5" if synthetic else "none"
        marker_id = "arrowhead-critical" if is_critical else "arrowhead"
        label = _compact_edge_label(dep)
        label_html = ""
        if label:
            label_width = max(64, min(170, len(label) * 6 + 18))
            label_html = (
                f"<rect x='{label_x - label_width / 2:.1f}' y='{label_y - 12:.1f}' width='{label_width:.1f}' height='24' rx='12' ry='12' "
                f"fill='#ffffff' stroke='#d5dbe3' stroke-width='1'/>"
                f"<text x='{label_x:.1f}' y='{label_y + 4:.1f}' fill='#344054' font-size='11' font-weight='600' text-anchor='middle'>{html.escape(label)}</text>"
            )

        edges_svg.append(
            f"""
            <path d='{path_data}' stroke='{stroke}' stroke-width='{width}' fill='none' stroke-dasharray='{dash}' marker-end='url(#{marker_id})' stroke-linecap='round' stroke-linejoin='round'/>
            {label_html}
            """
        )

    column_guides = "".join(
        f"<div class='dep-col-guide' style='left:{header['line_x']:.1f}px;top:{header['band_top']:.1f}px;height:{max(12.0, header['band_bottom'] - header['band_top']):.1f}px;'></div>"
        for header in column_headers
    )
    column_badges = "".join(
        f"<div class='dep-col-badge' style='left:{header['x'] - 54:.1f}px;top:{header['y']:.1f}px;'>{html.escape(header['label'])}</div>"
        for header in column_headers
    )

    band_spans: dict[float, tuple[float, float]] = {}
    for header in column_headers:
        top = header["band_top"]
        bottom = header["band_bottom"]
        if top in band_spans:
            band_spans[top] = (min(band_spans[top][0], top), max(band_spans[top][1], bottom))
        else:
            band_spans[top] = (top, bottom)
    band_cards = "".join(
        f"<div class='dep-band' style='top:{top - 18:.1f}px;height:{bottom - top + 52:.1f}px;'></div>"
        for top, bottom in band_spans.values()
    )
    band_regions = [(top, bottom) for top, bottom in band_spans.values()]

    nodes_html: list[str] = []
    for node_id, node in positions.items():
        if node_id in {"__start__", "__finish__"}:
            nodes_html.append(
                f"""
                <div class='milestone' style='left:{node['x']}px;top:{node['y']}px;width:{node['w']}px;height:{node['h']}px;'>
                    {html.escape(node['label'])}
                </div>
                """
            )
            continue

        is_critical = node_id in critical_nodes
        border_color = "#d32f2f" if is_critical else "#d0d5dd"
        shadow = "0 0 0 1px rgba(211,47,47,0.10), 0 10px 20px rgba(15,23,42,0.05)" if is_critical else "0 10px 20px rgba(15,23,42,0.04)"
        nodes_html.append(
            f"""
            <div class='node' style='left:{node['x']}px;top:{node['y']}px;border-color:{border_color};box-shadow:{shadow};'>
                <div class='node-head'>
                    <div class='node-id'>{html.escape(node['id'])}</div>
                    <div class='node-duration'>{node['duration']}w</div>
                </div>
                <div class='node-title'>{html.escape(_short_label(node['label'], 54))}</div>
                <div class='node-meta'>{html.escape(node['window'])}</div>
                <div class='node-kr'>{html.escape(node['kr'])}</div>
            </div>
            """
        )

    legend_items = [
        "<span class='legend-item'><span class='legend-line'></span>Dependency</span>",
        "<span class='legend-item'><span class='legend-line critical'></span>Critical Path</span>",
        "<span class='legend-item'><span class='legend-line dashed'></span>Start / Finish Connector</span>",
    ]

    cycle_note = "Cycle detected in dependencies. Ordering and critical path are approximate." if has_cycle else ""
    note_color = "#b42318" if has_cycle else "#667085"

    export_bands_svg = "".join(
        f"<rect x='{MARGIN_X + MILESTONE_W + 18:.1f}' y='{top - 18:.1f}' width='{max_x - 2 * (MARGIN_X + MILESTONE_W + 18):.1f}' height='{bottom - top + 52:.1f}' rx='18' ry='18' fill='rgba(239,246,255,0.72)' stroke='#dbeafe' stroke-width='1'/>"
        for top, bottom in band_regions
    )
    export_guides_svg = "".join(
        f"<line x1='{header['line_x']:.1f}' y1='{header['band_top']:.1f}' x2='{header['line_x']:.1f}' y2='{header['band_bottom']:.1f}' stroke='#e9edf3' stroke-width='1' stroke-dasharray='4 4'/>"
        for header in column_headers
    )
    export_badges_svg = "".join(
        f"<rect x='{header['x'] - 54:.1f}' y='{header['y']:.1f}' width='108' height='24' rx='12' ry='12' fill='#ffffff' stroke='#e4e7ec' stroke-width='1'/>"
        f"<text x='{header['x']:.1f}' y='{header['y'] + 16:.1f}' text-anchor='middle' font-size='11' font-weight='600' fill='#475467'>{html.escape(header['label'])}</text>"
        for header in column_headers
    )

    export_nodes_svg: list[str] = []
    for node_id, node in positions.items():
        x = float(node["x"])
        y = float(node["y"])
        w = float(node["w"])
        h = float(node["h"])
        if node_id in {"__start__", "__finish__"}:
            export_nodes_svg.append(
                f"<rect x='{x:.1f}' y='{y:.1f}' width='{w:.1f}' height='{h:.1f}' rx='{h / 2:.1f}' ry='{h / 2:.1f}' fill='#f8fafc' stroke='#d0d5dd' stroke-width='1'/>"
                f"<text x='{x + w / 2:.1f}' y='{y + h / 2 + 4:.1f}' text-anchor='middle' font-size='12' font-weight='700' fill='#344054'>{html.escape(node['label'])}</text>"
            )
            continue

        is_critical = node_id in critical_nodes
        border_color = "#d32f2f" if is_critical else "#d0d5dd"
        export_nodes_svg.append(
            f"<rect x='{x:.1f}' y='{y:.1f}' width='{w:.1f}' height='{h:.1f}' rx='12' ry='12' fill='#ffffff' stroke='{border_color}' stroke-width='1.2'/>"
            f"<text x='{x + 12:.1f}' y='{y + 18:.1f}' font-size='11' font-weight='700' fill='#344054'>{html.escape(str(node['id']))}</text>"
            f"<text x='{x + w - 12:.1f}' y='{y + 18:.1f}' text-anchor='end' font-size='11' font-weight='600' fill='#475467'>{int(node['duration'])}w</text>"
            f"<text x='{x + 12:.1f}' y='{y + 38:.1f}' font-size='12' font-weight='600' fill='#101828'>{html.escape(_short_label(node['label'], 42))}</text>"
            f"<text x='{x + 12:.1f}' y='{y + 56:.1f}' font-size='11' fill='#667085'>{html.escape(_short_label(node['window'], 44))}</text>"
            f"<text x='{x + 12:.1f}' y='{y + 74:.1f}' font-size='11' fill='#475467'>{html.escape(_short_label(node['kr'], 44))}</text>"
        )

    export_svg = f"""<?xml version='1.0' encoding='UTF-8'?>
<svg xmlns='http://www.w3.org/2000/svg' width='{max_x}' height='{max_y}' viewBox='0 0 {max_x} {max_y}' preserveAspectRatio='xMinYMin meet'>
    <defs>
        <marker id='arrowhead' markerWidth='10' markerHeight='7' refX='9' refY='3.5' orient='auto'>
            <polygon points='0 0, 10 3.5, 0 7' fill='#5b6472'></polygon>
        </marker>
        <marker id='arrowhead-critical' markerWidth='10' markerHeight='7' refX='9' refY='3.5' orient='auto'>
            <polygon points='0 0, 10 3.5, 0 7' fill='#d32f2f'></polygon>
        </marker>
        <style>
            text {{
                font-family: {EXPORT_FONT_STACK};
                text-rendering: geometricPrecision;
            }}
        </style>
    </defs>
    <rect x='0' y='0' width='{max_x}' height='{max_y}' fill='#ffffff'/>
    {export_bands_svg}
    {export_guides_svg}
    {export_badges_svg}
    {''.join(edges_svg)}
    {''.join(export_nodes_svg)}
</svg>
"""

    svg_markup = f"""
                    <svg viewBox='0 0 {max_x} {max_y}' preserveAspectRatio='xMinYMin meet' xmlns='http://www.w3.org/2000/svg'>
                        <defs>
                            <marker id='arrowhead' markerWidth='10' markerHeight='7' refX='9' refY='3.5' orient='auto'>
                                <polygon points='0 0, 10 3.5, 0 7' fill='#5b6472'></polygon>
                            </marker>
                            <marker id='arrowhead-critical' markerWidth='10' markerHeight='7' refX='9' refY='3.5' orient='auto'>
                                <polygon points='0 0, 10 3.5, 0 7' fill='#d32f2f'></polygon>
                            </marker>
                        </defs>
                        {''.join(edges_svg)}
                    </svg>
    """

    html_content = f"""
    <style>
        .dep-wrap {{
            border: 1px solid #e5e7eb;
            border-radius: 12px;
            background: #ffffff;
            padding: 12px;
        }}
        .dep-title {{
            font-weight: 700;
            color: #111827;
            margin: 2px 0 6px 0;
            font-size: 15px;
        }}
        .dep-subtitle {{
            color: #475467;
            font-size: 12px;
            margin-bottom: 8px;
        }}
        .dep-note {{
            color: {note_color};
            font-size: 12px;
            margin-bottom: 8px;
            min-height: 16px;
        }}
        .dep-legend {{
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
            margin-bottom: 10px;
        }}
        .legend-item {{
            display: inline-flex;
            align-items: center;
            gap: 7px;
            font-size: 12px;
            color: #344054;
        }}
        .legend-line {{
            width: 18px;
            height: 0;
            border-top: 2px solid #5b6472;
        }}
        .legend-line.critical {{
            border-top-color: #d32f2f;
        }}
        .legend-line.dashed {{
            border-top-style: dashed;
        }}
        .dep-toolbar {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 12px;
            margin-bottom: 10px;
            flex-wrap: wrap;
        }}
        .dep-toolbar-note {{
            font-size: 12px;
            color: #475467;
        }}
        .dep-controls {{
            display: inline-flex;
            gap: 8px;
            align-items: center;
            flex-wrap: wrap;
        }}
        .dep-btn {{
            border: 1px solid #d0d5dd;
            background: #ffffff;
            color: #101828;
            border-radius: 999px;
            padding: 5px 10px;
            font-size: 12px;
            cursor: pointer;
        }}
        .dep-scale {{
            font-size: 12px;
            color: #344054;
            min-width: 48px;
            text-align: center;
        }}
        .dep-viewport {{
            position: relative;
            height: {VIEWPORT_HEIGHT}px;
            overflow: auto;
            border: 1px solid #eaecf0;
            border-radius: 12px;
            background: linear-gradient(180deg, #fcfcfd 0%, #ffffff 100%);
        }}
        .dep-stage {{
            position: relative;
            width: {max_x}px;
            height: {max_y}px;
            transform-origin: top left;
        }}
        .dep-band {{
            position: absolute;
            left: {MARGIN_X + MILESTONE_W + 18}px;
            right: {MARGIN_X + MILESTONE_W + 18}px;
            border: 1px solid #dbeafe;
            border-radius: 18px;
            background: rgba(239, 246, 255, 0.72);
        }}
        .dep-col-guide {{
            position: absolute;
            width: 0;
            border-left: 1px dashed #e9edf3;
        }}
        .dep-col-badge {{
            position: absolute;
            width: 108px;
            text-align: center;
            padding: 4px 8px;
            border: 1px solid #e4e7ec;
            background: #ffffff;
            border-radius: 999px;
            font-size: 11px;
            color: #475467;
            font-weight: 600;
        }}
        .dep-canvas {{
            position: relative;
            width: {max_x}px;
            height: {max_y}px;
        }}
        .dep-canvas svg {{
            position: absolute;
            inset: 0;
            width: 100%;
            height: 100%;
            overflow: visible;
        }}
        .node {{
            position: absolute;
            width: {NODE_W}px;
            min-height: {NODE_H}px;
            padding: 10px 12px;
            border-radius: 12px;
            border: 1px solid #d0d5dd;
            background: #ffffff;
        }}
        .node-head {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
        }}
        .node-id {{
            font-size: 11px;
            color: #344054;
            font-weight: 700;
            letter-spacing: 0.02em;
        }}
        .node-duration {{
            font-size: 11px;
            color: #475467;
            padding: 2px 8px;
            border-radius: 999px;
            background: #f2f4f7;
            font-weight: 600;
        }}
        .node-title {{
            font-size: 12px;
            color: #101828;
            line-height: 1.35;
            font-weight: 600;
            margin-bottom: 6px;
        }}
        .node-meta {{
            font-size: 11px;
            color: #667085;
            margin-bottom: 6px;
        }}
        .node-kr {{
            font-size: 11px;
            color: #475467;
        }}
        .milestone {{
            position: absolute;
            display: flex;
            align-items: center;
            justify-content: center;
            border: 1px solid #d0d5dd;
            background: #f8fafc;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 700;
            color: #344054;
        }}
        @media (max-width: 960px) {{
            .dep-viewport {{
                height: 540px;
            }}
            .dep-toolbar {{
                align-items: flex-start;
            }}
        }}
    </style>
    <div class='dep-wrap'>
        <div class='dep-title'>PERT-Style Task Network</div>
        <div class='dep-subtitle'>The network is wrapped into horizontal bands to control width. Arrows use horizontal and vertical orthogonal routes to reduce visual crossings.</div>
        <div class='dep-note'>{html.escape(cycle_note)}</div>
        <div class='dep-legend'>{''.join(legend_items)}</div>
        <div class='dep-toolbar'>
            <div class='dep-toolbar-note'>The view opens in full-network mode first. Use zoom controls or Ctrl + mouse wheel to inspect dense branches.</div>
            <div class='dep-controls'>
                <button class='dep-btn' id='dep-zoom-out' type='button'>-</button>
                <button class='dep-btn' id='dep-fit-all' type='button'>Fit</button>
                <button class='dep-btn' id='dep-zoom-in' type='button'>+</button>
                <span class='dep-scale' id='dep-scale-value'>100%</span>
            </div>
        </div>
        <div class='dep-viewport' id='dep-viewport'>
            <div class='dep-stage' id='dep-stage'>
                {band_cards}
                {column_guides}
                {column_badges}
                <div class='dep-canvas'>
                    {svg_markup}
                    {''.join(nodes_html)}
                </div>
            </div>
        </div>
    </div>
    <script>
        (function () {{
            const viewport = document.getElementById('dep-viewport');
            const stage = document.getElementById('dep-stage');
            const zoomInButton = document.getElementById('dep-zoom-in');
            const zoomOutButton = document.getElementById('dep-zoom-out');
            const fitButton = document.getElementById('dep-fit-all');
            const scaleValue = document.getElementById('dep-scale-value');
            if (!viewport || !stage || !zoomInButton || !zoomOutButton || !fitButton || !scaleValue) {{
                return;
            }}

            let scale = 1;
            const minScale = 0.32;
            const maxScale = 2.3;

            function centerViewport() {{
                const scaledWidth = stage.offsetWidth * scale;
                const scaledHeight = stage.offsetHeight * scale;
                viewport.scrollLeft = Math.max(0, (scaledWidth - viewport.clientWidth) / 2);
                viewport.scrollTop = Math.max(0, (scaledHeight - viewport.clientHeight) / 2);
            }}

            function applyScale(nextScale, recenter) {{
                scale = Math.max(minScale, Math.min(maxScale, nextScale));
                stage.style.transform = `scale(${{scale}})`;
                scaleValue.textContent = `${{Math.round(scale * 100)}}%`;
                if (recenter) {{
                    window.requestAnimationFrame(centerViewport);
                }}
            }}

            function fitOverview() {{
                const fitWidth = (viewport.clientWidth - 36) / stage.offsetWidth;
                const fitHeight = (viewport.clientHeight - 36) / stage.offsetHeight;
                applyScale(Math.min(fitWidth, fitHeight, 1), true);
            }}

            zoomInButton.addEventListener('click', function () {{
                applyScale(scale + 0.14, false);
            }});
            zoomOutButton.addEventListener('click', function () {{
                applyScale(scale - 0.14, false);
            }});
            fitButton.addEventListener('click', fitOverview);
            viewport.addEventListener('wheel', function (event) {{
                if (!event.ctrlKey) {{
                    return;
                }}
                event.preventDefault();
                applyScale(scale + (event.deltaY < 0 ? 0.12 : -0.12), false);
            }}, {{ passive: false }});
            window.addEventListener('resize', fitOverview);
            fitOverview();
        }})();
    </script>
    """

    return {
        "html": html_content,
        "height": 780,
        "svg": export_svg.strip(),
        "svg_width": max_x,
        "svg_height": max_y,
    }
