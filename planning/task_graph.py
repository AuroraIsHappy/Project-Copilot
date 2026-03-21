from collections import deque


def _task_id(task: dict, index: int) -> str:
	return str(task.get("task_id") or f"T{index}").strip() or f"T{index}"


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


def build_task_graph(tasks: list[dict], dependencies: list[dict]) -> dict:
	"""Build a lightweight DAG representation used by planner agents."""
	task_ids = [_task_id(task, idx) for idx, task in enumerate(tasks, start=1)]
	nodes = {tid: {"task_id": tid} for tid in task_ids}

	adjacency: dict[str, list[str]] = {tid: [] for tid in task_ids}
	indegree: dict[str, int] = {tid: 0 for tid in task_ids}
	edges: list[dict] = []

	for item in dependencies or []:
		if not isinstance(item, dict):
			continue
		source = str(item.get("from") or "").strip()
		target = str(item.get("to") or "").strip()
		if not source or not target or source == target:
			continue
		if source not in nodes or target not in nodes:
			continue

		edges.append(
			{
				"from": source,
				"to": target,
				"type": str(item.get("type") or "FS").upper(),
				"lag_weeks": int(item.get("lag_weeks", _days_to_weeks(item.get("lag_days", 0), allow_negative=True)) or 0),
				"overlap_weeks": max(
					0,
					int(
						item.get(
							"overlap_weeks",
							item.get("overlap", _days_to_weeks(item.get("overlap_days", 0), allow_negative=False)),
						)
						or 0
					),
				),
			}
		)
		adjacency[source].append(target)
		indegree[target] += 1

	queue: deque[str] = deque(sorted([tid for tid, deg in indegree.items() if deg == 0]))
	topo_order: list[str] = []

	while queue:
		current = queue.popleft()
		topo_order.append(current)
		for nxt in adjacency[current]:
			indegree[nxt] -= 1
			if indegree[nxt] == 0:
				queue.append(nxt)

	has_cycle = len(topo_order) != len(task_ids)

	return {
		"nodes": nodes,
		"edges": edges,
		"adjacency": adjacency,
		"topological_order": topo_order,
		"has_cycle": has_cycle,
	}
