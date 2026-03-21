from utils.llm_client import call_llm
from utils.json_utils import extract_json


def _parse(response: str) -> list[dict]:
    payload = extract_json(response)
    if not isinstance(payload, list):
        return []

    strategies: list[dict] = []
    for index, item in enumerate(payload, start=1):
        if not isinstance(item, dict):
            continue

        sid = str(item.get("strategy_id") or f"S{index}").strip() or f"S{index}"
        name = str(item.get("strategy_name") or item.get("strategy") or item.get("name") or "").strip()
        if not name:
            continue

        strategies.append({"strategy_id": sid, "strategy_name": name})

    # Auto-fill IDs when the model returns empty/duplicate IDs.
    used: set[str] = set()
    for index, strategy in enumerate(strategies, start=1):
        sid = strategy.get("strategy_id", "")
        if not sid or sid in used:
            sid = f"S{index}"
            strategy["strategy_id"] = sid
        used.add(sid)

    return strategies

def strategy_prompt(okr):

    prompt = f"""
You are a senior product and project strategist.

Your task is to analyze an OKR (Objective and Key Results) and produce a set of high-level strategies that would realistically achieve the Key Results.

Follow these reasoning steps internally:

1. Understand the Objective.
2. Identify the Key Results.
3. Determine the main levers that could achieve those results.
4. Convert those levers into clear strategies.

Do NOT output the reasoning process.

STRATEGY GUIDELINES

- Each strategy should represent a meaningful approach to achieving the OKR.
- Strategies should be concrete and actionable.
- Avoid vague statements.
- A typical OKR should result in 3–6 strategies.
- Strategies should cover different aspects of the project if possible.

INPUT FORMAT

- You will receive one OKR text block.

LANGUAGE RULE

- Detect the OKR language from user input.
- If the OKR is Chinese, output all strategy_name values in Chinese.
- If the OKR is English, output all strategy_name values in English.
- Do not mix Chinese and English in strategy_name values.

OKR:
{okr}

Return ONLY JSON array in this exact schema:

[
  {{
    "strategy_id": "S1",
    "strategy_name": "strategy description"
  }}
]

STRICT OUTPUT RULES

- Output ONLY JSON
- No explanations
- No markdown
- No text outside JSON
"""

    return prompt

def run(okr: str) -> list[dict]:

    prompt = strategy_prompt(okr)

    response = call_llm(
        prompt,
        trace_label="strategy_agent",
    )

    return _parse(response)


def strategy_agent(okr):
    return run(okr)