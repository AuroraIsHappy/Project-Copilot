import json
from datetime import datetime
from pathlib import Path
from typing import Any


LOG_FILE = Path(__file__).resolve().parents[2] / "data" / "debug" / "agent_debug_log.txt"


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except TypeError:
        return json.dumps(str(obj), ensure_ascii=False)


def log_agent_step(agent_name: str, stage: str, payload: Any = None, error: str = "") -> None:
    """Append one line JSON logs for each agent stage."""
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "agent": agent_name,
        "stage": stage,
        "payload": payload,
    }
    if error:
        record["error"] = error

    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(_safe_json(record) + "\n")
