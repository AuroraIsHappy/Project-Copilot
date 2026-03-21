from __future__ import annotations

import json
import math
import os
import re
import socket
import subprocess
import uuid
import xml.etree.ElementTree as ET
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from urllib.parse import quote_plus
from urllib.parse import urlsplit
from urllib.error import HTTPError
from urllib.request import Request
from urllib.request import urlopen

from utils.json_utils import extract_json
from utils.llm_client import call_llm_messages
from utils.llm_client import load_llm_config

_ARXIV_ENDPOINT = "http://export.arxiv.org/api/query"
_GITHUB_SEARCH_ENDPOINT = "https://api.github.com/search/repositories"
_HN_SEARCH_ENDPOINT = "https://hn.algolia.com/api/v1/search"
_REDDIT_SEARCH_ENDPOINTS = (
    "https://api.reddit.com/search.json",
    "https://www.reddit.com/search.json",
    "https://old.reddit.com/search.json",
)
_REDDIT_SKILL_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "reddit_read_only" / "scripts" / "reddit-readonly.mjs"
_MAX_CANDIDATE_RESULTS = 6
_MIN_CANDIDATE_RESULTS = 2
_MAX_CANDIDATE_RESULTS_PER_PROJECT = 16
_MAX_QUERY_TERMS = 4
_MAX_QUERY_VARIANTS = 3
_DEFAULT_CARD_COUNT = 3
_HTTP_TIMEOUT_SECONDS = 12
_REDDIT_SKILL_TIMEOUT_SECONDS = 35
_REDDIT_PUBLIC_TIMEOUT_SECONDS = 8
_REDDIT_CONNECT_TIMEOUT_SECONDS = 2.2
_REDDIT_USER_AGENT = "script:okr-insight-engine-reddit:v1.0.0 (personal)"
_REDDIT_COOLDOWN_MINUTES = 20
_REDDIT_SKILL_ENV_DEFAULTS = {
    "REDDIT_RO_MIN_DELAY_MS": "800",
    "REDDIT_RO_MAX_DELAY_MS": "1800",
    "REDDIT_RO_TIMEOUT_MS": "25000",
    "REDDIT_RO_USER_AGENT": _REDDIT_USER_AGENT,
}
_REDDIT_NETWORK_ERROR_TOKENS = (
    "fetch failed",
    "und_err_connect_timeout",
    "connect timeout",
    "network is unreachable",
    "name or service not known",
    "temporary failure in name resolution",
    "getaddrinfo",
    "connection refused",
    "timed out",
    "no route to host",
    "connection reset",
    "econnrefused",
    "enotfound",
)
_REDDIT_BLOCKED_ERROR_TOKENS = (
    "http 403",
    "http 429",
    "forbidden",
    "too many requests",
    "rate limit",
)
_REDDIT_CONNECT_HOSTS = (
    "www.reddit.com",
    "api.reddit.com",
)
_MAX_RERANKED_CANDIDATES = 10
_MAX_RERANKED_CANDIDATES_PER_PROJECT = 30
_MAX_PAPER_AGE_DAYS = 365
_MAX_RECENT_SOURCE_AGE_DAYS = 90
_SOURCE_PRIORITY = {
    "open_source": 0.92,
    "paper": 0.86,
    "blog": 0.78,
}
_DIVERSITY_SOURCE_ORDER = ("open_source", "paper", "blog")
_INSIGHT_RETRIEVAL_SOURCES = ("arxiv", "github", "blog", "reddit")
_SOURCE_QUERY_VARIANT_LIMITS = {
    "arxiv": _MAX_QUERY_VARIANTS,
    "github": _MAX_QUERY_VARIANTS,
    "blog": _MAX_QUERY_VARIANTS,
    # Reddit requests are slower/less stable in constrained networks.
    "reddit": 1,
}
_BLOG_EXCLUDED_DOMAINS = {
    "news.ycombinator.com",
    "github.com",
    "www.github.com",
    "arxiv.org",
    "www.arxiv.org",
}

_CN_TERM_MAP = {
    "蛋白": "protein",
    "蛋白质": "protein",
    "序列": "sequence",
    "结构": "structure",
    "溶解": "solubility",
    "预测": "prediction",
    "模型": "model",
    "训练": "training",
    "鲁棒": "robustness",
    "泛化": "generalization",
    "机器人": "robotics",
    "双臂": "dual-arm",
    "精细": "dexterity",
    "金融": "finance",
    "交易": "trading",
    "转化": "conversion",
    "推荐": "recommendation",
    "规划": "planning",
    "调度": "scheduling",
    "优化": "optimization",
}

_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "that",
    "this",
    "into",
    "your",
    "ours",
    "project",
    "okr",
    "task",
    "tasks",
    "execution",
    "improve",
    "提升",
    "优化",
    "项目",
    "任务",
    "目标",
    "当前",
}

_reddit_backoff_until: datetime | None = None
_reddit_backoff_reason = ""


def _resolve_insight_model() -> str:
    try:
        cfg = load_llm_config()
    except Exception:
        return ""

    assistant_model = str(cfg.get("assistant_model") or "").strip()
    if assistant_model:
        return assistant_model
    return str(cfg.get("model") or "").strip()


def _project_context_text(project_name: str, tasks: list[dict]) -> str:
    parts: list[str] = [str(project_name or "").strip()]
    for task in tasks[:30]:
        if not isinstance(task, dict):
            continue
        for key in ("objective", "kr", "task"):
            value = str(task.get(key, "") or "").strip()
            if value:
                parts.append(value)
    return "\n".join([p for p in parts if p])


def _extract_keywords(project_name: str, tasks: list[dict], limit: int = 12) -> list[str]:
    context_text = _project_context_text(project_name, tasks)
    if not context_text:
        return ["machine learning", "project planning"]

    keywords: list[str] = []
    seen: set[str] = set()

    english_terms = re.findall(r"[a-zA-Z][a-zA-Z0-9\-]{2,}", context_text)
    for term in english_terms:
        normalized = term.lower().strip()
        if normalized in _STOPWORDS or len(normalized) < 3:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        keywords.append(normalized)
        if len(keywords) >= limit:
            return keywords

    chinese_terms = re.findall(r"[\u4e00-\u9fff]{2,8}", context_text)
    for term in chinese_terms:
        mapped = None
        for cn_key, mapped_en in _CN_TERM_MAP.items():
            if cn_key in term:
                mapped = mapped_en
                break
        normalized = mapped or term
        normalized = normalized.lower().strip()
        if normalized in _STOPWORDS or len(normalized) < 2:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        keywords.append(normalized)
        if len(keywords) >= limit:
            break

    if not keywords:
        return ["machine learning", "optimization"]
    return keywords


def _build_arxiv_query(keywords: list[str]) -> str:
    sanitized = [str(term or "").strip() for term in keywords if str(term or "").strip()]
    if not sanitized:
        sanitized = ["machine learning", "optimization"]

    selected = sanitized[:_MAX_QUERY_TERMS]
    query_parts = [f"all:{term}" for term in selected]
    return " OR ".join(query_parts)


def _build_text_query(keywords: list[str], *, max_terms: int = _MAX_QUERY_TERMS) -> str:
    sanitized = [str(term or "").strip() for term in keywords if str(term or "").strip()]
    if not sanitized:
        return "machine learning optimization"
    return " ".join(sanitized[:max_terms])


def _build_query_keyword_variants(
    keywords: list[str],
    *,
    max_terms: int = _MAX_QUERY_TERMS,
    max_variants: int = _MAX_QUERY_VARIANTS,
) -> list[list[str]]:
    sanitized: list[str] = []
    seen: set[str] = set()
    for raw in keywords:
        term = str(raw or "").strip()
        if not term:
            continue
        normalized = term.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        sanitized.append(term)

    if not sanitized:
        sanitized = ["machine learning", "optimization"]

    effective_terms = max(1, int(max_terms))
    effective_variants = max(1, int(max_variants))

    variants: list[list[str]] = []
    variant_seen: set[str] = set()

    def _add_variant(terms: list[str]) -> None:
        cleaned = [str(item or "").strip() for item in terms if str(item or "").strip()]
        if not cleaned:
            return
        variant_key = "||".join(item.lower() for item in cleaned)
        if variant_key in variant_seen:
            return
        variant_seen.add(variant_key)
        variants.append(cleaned)

    _add_variant(sanitized[:effective_terms])

    if len(sanitized) > effective_terms:
        window_size = max(1, effective_terms - 1)
        start = 1
        while start < len(sanitized) and len(variants) < effective_variants:
            window = [sanitized[0]] + sanitized[start : start + window_size]
            _add_variant(window[:effective_terms])
            start += max(1, window_size)

    if len(variants) < effective_variants and len(sanitized) > 1:
        _add_variant(sanitized[-effective_terms:])

    return variants[:effective_variants]


def _http_get_json(url: str, headers: dict | None = None, timeout_seconds: float | None = None) -> dict | list:
    request_headers = {
        "User-Agent": "OKR-Insight-Engine/1.1 (+https://example.local)",
        "Accept": "application/json",
    }
    if isinstance(headers, dict):
        for key, value in headers.items():
            key_text = str(key or "").strip()
            value_text = str(value or "").strip()
            if key_text and value_text:
                request_headers[key_text] = value_text

    req = Request(url, headers=request_headers)
    effective_timeout = _HTTP_TIMEOUT_SECONDS
    if timeout_seconds is not None:
        try:
            effective_timeout = max(0.5, float(timeout_seconds))
        except (TypeError, ValueError):
            effective_timeout = _HTTP_TIMEOUT_SECONDS

    with urlopen(req, timeout=effective_timeout) as resp:
        payload = resp.read().decode("utf-8", errors="ignore")
    return json.loads(payload)


def _http_get_json_with_fallback(
    urls: list[str],
    headers: dict | None = None,
    timeout_seconds: float | None = None,
) -> dict | list:
    last_error: Exception | None = None
    for url in urls:
        try:
            return _http_get_json(url, headers=headers, timeout_seconds=timeout_seconds)
        except Exception as exc:
            last_error = exc
            continue

    if last_error is not None:
        raise last_error
    raise RuntimeError("No URL candidates provided")


def _search_arxiv(keywords: list[str], max_results: int = _MAX_CANDIDATE_RESULTS) -> tuple[list[dict], str]:
    query = _build_arxiv_query(keywords)
    request_url = (
        f"{_ARXIV_ENDPOINT}?search_query={quote_plus(query)}"
        f"&start=0&max_results={max(1, int(max_results))}&sortBy=relevance&sortOrder=descending"
    )

    req = Request(
        request_url,
        headers={
            "User-Agent": "OKR-Insight-Engine/1.0 (+https://example.local)",
        },
    )

    with urlopen(req, timeout=_HTTP_TIMEOUT_SECONDS) as resp:
        payload = resp.read()

    root = ET.fromstring(payload)
    ns = {"atom": "http://www.w3.org/2005/Atom"}

    candidates: list[dict] = []
    for entry in root.findall("atom:entry", ns):
        title = " ".join((entry.findtext("atom:title", default="", namespaces=ns) or "").split())
        summary = " ".join((entry.findtext("atom:summary", default="", namespaces=ns) or "").split())
        link = (entry.findtext("atom:id", default="", namespaces=ns) or "").strip()
        published = (entry.findtext("atom:published", default="", namespaces=ns) or "")[:10]

        authors: list[str] = []
        for author in entry.findall("atom:author", ns):
            name = str(author.findtext("atom:name", default="", namespaces=ns) or "").strip()
            if name:
                authors.append(name)

        if not title or not link:
            continue

        candidates.append(
            {
                "source": "arxiv",
                "source_type": "paper",
                "title": title,
                "url": link,
                "summary": summary,
                "published": published,
                "authors": authors[:5],
            }
        )

    return candidates, query


def _search_github_repositories(keywords: list[str], max_results: int = _MAX_CANDIDATE_RESULTS) -> tuple[list[dict], str]:
    query = _build_text_query(keywords)
    request_url = (
        f"{_GITHUB_SEARCH_ENDPOINT}?q={quote_plus(query + ' in:name,description,readme archived:false')}&sort=stars"
        f"&order=desc&per_page={max(1, int(max_results))}"
    )

    payload = _http_get_json(request_url)
    if not isinstance(payload, dict):
        return [], query

    items = payload.get("items", [])
    if not isinstance(items, list):
        items = []

    candidates: list[dict] = []
    for item in items:
        if not isinstance(item, dict):
            continue

        full_name = str(item.get("full_name") or "").strip()
        html_url = str(item.get("html_url") or "").strip()
        description = str(item.get("description") or "").strip()
        if not full_name or not html_url:
            continue

        language = str(item.get("language") or "").strip()
        topics = item.get("topics", []) if isinstance(item.get("topics"), list) else []
        stars = int(item.get("stargazers_count", 0) or 0)
        updated_at = str(item.get("updated_at") or "")[:10]

        summary_parts = [description]
        if language:
            summary_parts.append(f"language: {language}")
        if topics:
            summary_parts.append("topics: " + ", ".join(str(topic).strip() for topic in topics[:6] if str(topic).strip()))

        candidates.append(
            {
                "source": "github",
                "source_type": "open_source",
                "title": full_name,
                "url": html_url,
                "summary": " | ".join(part for part in summary_parts if part),
                "published": updated_at,
                "stars": stars,
                "language": language,
                "topics": [str(topic).strip() for topic in topics[:8] if str(topic).strip()],
            }
        )

    return candidates, query


def _search_blog_articles(keywords: list[str], max_results: int = _MAX_CANDIDATE_RESULTS) -> tuple[list[dict], str]:
    query = _build_text_query(keywords)
    request_url = (
        f"{_HN_SEARCH_ENDPOINT}?query={quote_plus(query)}&tags=story&hitsPerPage={max(1, int(max_results) * 2)}"
    )

    payload = _http_get_json(request_url)
    if not isinstance(payload, dict):
        return [], query

    hits = payload.get("hits", [])
    if not isinstance(hits, list):
        hits = []

    candidates: list[dict] = []
    for item in hits:
        if not isinstance(item, dict):
            continue

        title = str(item.get("title") or "").strip()
        url = str(item.get("url") or "").strip()
        if not title or not url:
            continue

        domain = str(urlsplit(url).netloc or "").lower().strip()
        if domain.startswith("www."):
            domain = domain[4:]
        if not domain or domain in _BLOG_EXCLUDED_DOMAINS:
            continue

        points = int(item.get("points", 0) or 0)
        author = str(item.get("author") or "").strip()
        summary_parts = [domain]
        if author:
            summary_parts.append(f"author: {author}")
        if points > 0:
            summary_parts.append(f"points: {points}")

        candidates.append(
            {
                "source": "hn_algolia",
                "source_type": "blog",
                "title": title,
                "url": url,
                "summary": " | ".join(summary_parts),
                "published": str(item.get("created_at") or "")[:10],
                "points": points,
                "domain": domain,
            }
        )

        if len(candidates) >= max(1, int(max_results)):
            break

    return candidates, query


def _has_proxy_env() -> bool:
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        if str(os.environ.get(key, "") or "").strip():
            return True
    return False


def _reddit_backoff_message() -> str | None:
    global _reddit_backoff_until
    global _reddit_backoff_reason

    if _reddit_backoff_until is None:
        return None

    now = datetime.now()
    if now >= _reddit_backoff_until:
        _reddit_backoff_until = None
        _reddit_backoff_reason = ""
        return None

    remain_seconds = int((_reddit_backoff_until - now).total_seconds())
    remain_minutes = max(1, math.ceil(remain_seconds / 60.0))
    reason = _reddit_backoff_reason or "reddit source temporarily unavailable"
    return f"{reason}; skip reddit for {remain_minutes}m"


def _set_reddit_backoff(reason: str, minutes: int = _REDDIT_COOLDOWN_MINUTES) -> None:
    global _reddit_backoff_until
    global _reddit_backoff_reason

    safe_minutes = max(1, int(minutes))
    _reddit_backoff_until = datetime.now() + timedelta(minutes=safe_minutes)
    _reddit_backoff_reason = str(reason or "reddit source temporarily unavailable").strip()


def _looks_like_reddit_network_error(exc: Exception) -> bool:
    if isinstance(exc, (TimeoutError, subprocess.TimeoutExpired)):
        return True

    raw = str(exc or "").strip().lower()
    if not raw:
        return False
    return any(token in raw for token in _REDDIT_NETWORK_ERROR_TOKENS)


def _looks_like_reddit_blocked_error(exc: Exception) -> bool:
    if isinstance(exc, HTTPError) and int(getattr(exc, "code", 0) or 0) in {403, 429}:
        return True

    raw = str(exc or "").strip().lower()
    if not raw:
        return False
    return any(token in raw for token in _REDDIT_BLOCKED_ERROR_TOKENS)


def _reddit_connectivity_precheck() -> bool:
    if _has_proxy_env():
        return True

    for host in _REDDIT_CONNECT_HOSTS:
        try:
            with socket.create_connection((host, 443), timeout=_REDDIT_CONNECT_TIMEOUT_SECONDS):
                return True
        except OSError:
            continue
    return False


def _search_reddit_posts_via_skill(keywords: list[str], max_results: int = _MAX_CANDIDATE_RESULTS) -> tuple[list[dict], str]:
    query = _build_text_query(keywords)
    script_path = _REDDIT_SKILL_SCRIPT_PATH
    if not script_path.exists():
        raise RuntimeError("reddit_read_only script not found")

    command = [
        "node",
        str(script_path),
        "search",
        "all",
        query,
        "--limit",
        str(max(1, int(max_results))),
    ]

    command_env = os.environ.copy()
    for env_key, env_value in _REDDIT_SKILL_ENV_DEFAULTS.items():
        if not str(command_env.get(env_key, "") or "").strip():
            command_env[env_key] = env_value

    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="ignore",
        timeout=_REDDIT_SKILL_TIMEOUT_SECONDS,
        cwd=str(script_path.parents[2]),
        env=command_env,
    )

    stdout = str(completed.stdout or "").strip()
    parsed = extract_json(stdout)
    if not isinstance(parsed, dict):
        stderr = str(completed.stderr or "").strip()
        raise RuntimeError(stderr or stdout or "reddit_read_only returned invalid JSON")

    if not parsed.get("ok"):
        error_obj = parsed.get("error", {}) if isinstance(parsed.get("error"), dict) else {}
        message = str(error_obj.get("message") or "reddit_read_only failed").strip()
        details = str(error_obj.get("details") or "").strip()
        raise RuntimeError(f"{message}{': ' + details if details else ''}")

    data = parsed.get("data", {}) if isinstance(parsed.get("data"), dict) else {}
    posts = data.get("posts", []) if isinstance(data.get("posts"), list) else []

    candidates: list[dict] = []
    for post in posts:
        if not isinstance(post, dict):
            continue

        title = str(post.get("title") or "").strip()
        permalink = str(post.get("permalink") or "").strip()
        if not title or not permalink:
            continue

        subreddit = str(post.get("subreddit") or "").strip()
        author = str(post.get("author") or "").strip()
        score = int(post.get("score", 0) or 0)
        num_comments = int(post.get("num_comments", 0) or 0)
        linked_url = str(post.get("url") or "").strip()
        selftext_snippet = str(post.get("selftext_snippet") or "").strip()
        is_self = bool(post.get("is_self", False))

        summary_parts: list[str] = []
        if subreddit:
            summary_parts.append(f"subreddit: r/{subreddit}")
        if author:
            summary_parts.append(f"author: {author}")
        if score > 0:
            summary_parts.append(f"score: {score}")
        if num_comments > 0:
            summary_parts.append(f"comments: {num_comments}")
        if selftext_snippet:
            summary_parts.append(selftext_snippet[:260])
        elif linked_url:
            summary_parts.append(f"linked: {linked_url}")

        candidates.append(
            {
                "source": "reddit_read_only",
                "source_type": "blog",
                "title": title,
                "url": permalink,
                "summary": " | ".join(part for part in summary_parts if part),
                "published": str(post.get("created_iso") or post.get("created_utc") or "").strip(),
                "points": score,
                "num_comments": num_comments,
                "domain": f"reddit.com/r/{subreddit}" if subreddit else "reddit.com",
                "linked_url": linked_url,
            }
        )

    return candidates, query


def _search_reddit_posts_public_api(keywords: list[str], max_results: int = _MAX_CANDIDATE_RESULTS) -> tuple[list[dict], str]:
    query = _build_text_query(keywords)
    request_urls = [
        (
            f"{endpoint}?q={quote_plus(query)}&sort=relevance&t=year"
            f"&limit={max(1, int(max_results))}&restrict_sr=0&raw_json=1&type=link"
        )
        for endpoint in _REDDIT_SEARCH_ENDPOINTS
    ]

    reddit_headers = {
        "User-Agent": _REDDIT_USER_AGENT,
        "Accept": "application/json",
        "Referer": "https://www.reddit.com/",
    }

    try:
        payload = _http_get_json_with_fallback(
            request_urls,
            headers=reddit_headers,
            timeout_seconds=_REDDIT_PUBLIC_TIMEOUT_SECONDS,
        )
    except HTTPError as exc:
        raise RuntimeError(f"reddit search blocked: HTTP {exc.code}") from exc

    if not isinstance(payload, dict):
        return [], query

    data = payload.get("data", {})
    if not isinstance(data, dict):
        return [], query

    children = data.get("children", [])
    if not isinstance(children, list):
        children = []

    candidates: list[dict] = []
    for item in children:
        if not isinstance(item, dict):
            continue
        post = item.get("data", {})
        if not isinstance(post, dict):
            continue

        title = str(post.get("title") or "").strip()
        permalink = str(post.get("permalink") or "").strip()
        if not title or not permalink:
            continue

        subreddit = str(post.get("subreddit") or "").strip()
        author = str(post.get("author") or "").strip()
        score = int(post.get("score", 0) or 0)
        num_comments = int(post.get("num_comments", 0) or 0)
        selftext = str(post.get("selftext") or "").strip()
        external_url = str(post.get("url") or "").strip()
        is_self = bool(post.get("is_self", False))

        summary_parts: list[str] = []
        if subreddit:
            summary_parts.append(f"subreddit: r/{subreddit}")
        if author:
            summary_parts.append(f"author: {author}")
        if score > 0:
            summary_parts.append(f"score: {score}")
        if num_comments > 0:
            summary_parts.append(f"comments: {num_comments}")
        if is_self and selftext:
            summary_parts.append(selftext[:260])
        elif external_url:
            summary_parts.append(f"linked: {external_url}")

        candidates.append(
            {
                "source": "reddit",
                "source_type": "blog",
                "title": title,
                "url": f"https://www.reddit.com{permalink}",
                "summary": " | ".join(part for part in summary_parts if part),
                "published": str(post.get("created_utc") or "").strip(),
                "points": score,
                "num_comments": num_comments,
                "domain": f"reddit.com/r/{subreddit}" if subreddit else "reddit.com",
                "linked_url": external_url,
            }
        )

        if len(candidates) >= max(1, int(max_results)):
            break

    return candidates, query


def _search_reddit_posts(keywords: list[str], max_results: int = _MAX_CANDIDATE_RESULTS) -> tuple[list[dict], str]:
    backoff_message = _reddit_backoff_message()
    if backoff_message:
        raise RuntimeError(backoff_message)

    if not _reddit_connectivity_precheck():
        reason = "reddit network unreachable from current environment"
        _set_reddit_backoff(reason)
        raise RuntimeError(f"{reason}; source skipped temporarily")

    last_error: Exception | None = None

    try:
        return _search_reddit_posts_via_skill(keywords, max_results=max_results)
    except Exception as exc:
        if _looks_like_reddit_network_error(exc):
            _set_reddit_backoff("reddit skill network error")
            raise RuntimeError("reddit skill network error; source skipped temporarily") from exc
        last_error = exc

    try:
        return _search_reddit_posts_public_api(keywords, max_results=max_results)
    except Exception as exc:
        if _looks_like_reddit_network_error(exc):
            _set_reddit_backoff("reddit public api network error")
            raise RuntimeError("reddit public api network error; source skipped temporarily") from exc
        if _looks_like_reddit_blocked_error(exc):
            _set_reddit_backoff("reddit endpoint blocked or rate-limited", minutes=30)
            raise RuntimeError("reddit endpoint blocked or rate-limited; source skipped temporarily") from exc
        if last_error is not None:
            raise RuntimeError(f"reddit skill failed ({last_error}); public api failed ({exc})") from exc
        raise


def _canonical_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""

    parsed = urlsplit(raw)
    netloc = str(parsed.netloc or "").lower().strip()
    if netloc.startswith("www."):
        netloc = netloc[4:]

    path = str(parsed.path or "").strip().rstrip("/")
    if path.endswith(".git"):
        path = path[:-4]

    return f"{netloc}{path}"


def _infer_source_from_url(url: str) -> str:
    parsed = urlsplit(str(url or "").strip())
    domain = str(parsed.netloc or "").lower().strip()
    if domain.startswith("www."):
        domain = domain[4:]

    if "arxiv.org" in domain:
        return "arxiv"
    if "github.com" in domain:
        return "github"
    if "reddit.com" in domain:
        return "reddit"
    if "news.ycombinator.com" in domain:
        return "hn_algolia"
    return domain or "web"


def _tokenize_text_for_match(text: str) -> set[str]:
    normalized = str(text or "").lower()
    return {
        token
        for token in re.findall(r"[a-z0-9_\-]{2,}|[\u4e00-\u9fff]{2,}", normalized)
        if token and token not in _STOPWORDS
    }


def _pick_related_task_label(tasks: list[dict], hint_text: str) -> str:
    if not tasks:
        return "当前计划任务"

    hint_tokens = _tokenize_text_for_match(hint_text)
    best_task: dict | None = None
    best_score = -1
    for task in tasks:
        if not isinstance(task, dict):
            continue
        task_text = " ".join(
            [
                str(task.get("task", "") or ""),
                str(task.get("kr", "") or ""),
                str(task.get("objective", "") or ""),
            ]
        )
        task_tokens = _tokenize_text_for_match(task_text)
        overlap = len(task_tokens & hint_tokens) if hint_tokens and task_tokens else 0
        if overlap > best_score:
            best_score = overlap
            best_task = task

    if not isinstance(best_task, dict):
        best_task = tasks[0] if tasks and isinstance(tasks[0], dict) else {}

    task_id = str(best_task.get("task_id") or "").strip()
    task_name = str(best_task.get("task") or "").strip() or "未命名任务"
    if task_id:
        return f"任务{task_id}「{task_name}」"
    return f"任务「{task_name}」"


def _pick_related_summary_reference(summary_snippets: list[dict], hint_text: str) -> tuple[str, str]:
    if not summary_snippets:
        return "", ""

    hint_tokens = _tokenize_text_for_match(hint_text)
    best_filename = ""
    best_snippet = ""
    best_score = -1

    for item in summary_snippets[:6]:
        if not isinstance(item, dict):
            continue
        filename = str(item.get("filename") or "").strip()
        snippet = str(item.get("snippet") or "").strip()
        if not filename and not snippet:
            continue

        merged_text = f"{filename} {snippet}"
        merged_tokens = _tokenize_text_for_match(merged_text)
        overlap = len(merged_tokens & hint_tokens) if hint_tokens and merged_tokens else 0
        if overlap > best_score:
            best_score = overlap
            best_filename = filename
            best_snippet = snippet

    if not best_filename and summary_snippets:
        first_item = summary_snippets[0] if isinstance(summary_snippets[0], dict) else {}
        best_filename = str(first_item.get("filename") or "").strip()
        best_snippet = str(first_item.get("snippet") or "").strip()

    return best_filename, best_snippet


def _trim_evidence_quote(text: str, max_chars: int = 180) -> str:
    cleaned = " ".join(str(text or "").replace("<<", "").replace(">>", "").split())
    if not cleaned:
        return ""
    if len(cleaned) <= max_chars:
        return cleaned
    return cleaned[: max_chars - 1].rstrip() + "…"


def _compose_evidence_risk_alert(
    *,
    source_label: str,
    quote: str,
    task_label: str,
    summary_filename: str,
    summary_quote: str,
) -> str:
    relation = f"这与你的{task_label}技术路线相近"
    if summary_filename:
        summary_piece = _trim_evidence_quote(summary_quote, max_chars=110)
        if summary_piece:
            relation += f"，并与工作总结<{summary_filename}>提到的“{summary_piece}”一致"
        else:
            relation += f"，并与工作总结<{summary_filename}>中的当前尝试方向一致"

    return f"引用：{source_label}明确指出“{quote}”；关联：{relation}，建议先做小规模对照验证后再推进。"


def _finalize_cards_with_evidence(
    cards: list[dict],
    candidates: list[dict],
    tasks: list[dict],
    summary_snippets: list[dict],
) -> list[dict]:
    if not cards:
        return []

    candidate_by_url: dict[str, dict] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        key = _canonical_url(candidate.get("url", ""))
        if key and key not in candidate_by_url:
            candidate_by_url[key] = candidate

    finalized: list[dict] = []
    for card in cards:
        if not isinstance(card, dict):
            continue

        merged = dict(card)
        url = str(merged.get("url") or "").strip()
        matched_candidate = candidate_by_url.get(_canonical_url(url), {})

        source_label = str(merged.get("source") or "").strip()
        if not source_label:
            source_label = str(matched_candidate.get("source") or "").strip() or _infer_source_from_url(url)
        merged["source"] = source_label

        task_label = _pick_related_task_label(
            tasks,
            " ".join(
                [
                    str(merged.get("title") or ""),
                    str(merged.get("core_insight") or ""),
                    str(merged.get("relevance_reason") or ""),
                    str(matched_candidate.get("summary") or ""),
                ]
            ),
        )
        summary_filename, summary_quote = _pick_related_summary_reference(
            summary_snippets,
            " ".join(
                [
                    str(merged.get("title") or ""),
                    str(matched_candidate.get("summary") or ""),
                ]
            ),
        )

        quote = _trim_evidence_quote(
            str(merged.get("evidence_snippet") or "")
            or str(matched_candidate.get("summary") or "")
            or str(merged.get("title") or "")
        )
        if quote and not str(merged.get("evidence_snippet") or "").strip():
            merged["evidence_snippet"] = f"[{source_label}] {quote}"

        risk_text = str(merged.get("risk_alert") or "").strip()
        has_evidence = "引用：" in risk_text and "关联：" in risk_text
        if not has_evidence:
            merged["risk_alert"] = _compose_evidence_risk_alert(
                source_label=source_label,
                quote=quote or "该方法在特定边界条件下存在失败风险",
                task_label=task_label,
                summary_filename=summary_filename,
                summary_quote=summary_quote,
            )

        finalized.append(merged)

    return finalized


def _normalize_excluded_url_keys(values) -> set[str]:
    if values is None:
        return set()

    if isinstance(values, (list, tuple, set)):
        iterable = values
    else:
        iterable = [values]

    normalized: set[str] = set()
    for raw in iterable:
        text = str(raw or "").strip()
        if not text:
            continue
        key = _canonical_url(text)
        if key:
            normalized.add(key)

    return normalized


def _filter_candidates_by_excluded_urls(candidates: list[dict], excluded_url_keys: set[str]) -> tuple[list[dict], int]:
    if not excluded_url_keys:
        return list(candidates), 0

    filtered: list[dict] = []
    excluded_count = 0
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue

        key = _canonical_url(candidate.get("url", ""))
        if key and key in excluded_url_keys:
            excluded_count += 1
            continue

        filtered.append(candidate)

    return filtered, excluded_count


def _title_tokens(text: str) -> set[str]:
    normalized = str(text or "").lower()
    tokens = re.findall(r"[a-z0-9]+|[\u4e00-\u9fff]{2,}", normalized)
    return {token for token in tokens if token and token not in _STOPWORDS}


def _title_key(text: str) -> str:
    return " ".join(sorted(_title_tokens(text)))


def _merge_candidate_batches(batches: list[list[dict]]) -> list[dict]:
    merged: list[dict] = []
    seen_keys: set[str] = set()

    for batch in batches:
        if not isinstance(batch, list):
            continue
        for candidate in batch:
            if not isinstance(candidate, dict):
                continue

            key = _canonical_url(candidate.get("url", ""))
            if not key:
                key = _title_key(candidate.get("title", ""))

            if key and key in seen_keys:
                continue

            if key:
                seen_keys.add(key)
            merged.append(candidate)

    return merged


def _looks_like_duplicate(candidate: dict, selected: list[dict]) -> bool:
    candidate_url = _canonical_url(candidate.get("url", ""))
    candidate_title_key = _title_key(candidate.get("title", ""))
    candidate_tokens = _title_tokens(candidate.get("title", ""))

    for existing in selected:
        existing_url = _canonical_url(existing.get("url", ""))
        if candidate_url and existing_url and candidate_url == existing_url:
            return True

        existing_title_key = _title_key(existing.get("title", ""))
        if candidate_title_key and existing_title_key and candidate_title_key == existing_title_key:
            return True

        existing_tokens = _title_tokens(existing.get("title", ""))
        if candidate_tokens and existing_tokens:
            union = candidate_tokens | existing_tokens
            if union:
                jaccard = len(candidate_tokens & existing_tokens) / len(union)
                if jaccard >= 0.88:
                    return True

    return False


def _parse_date_safe(value: str):
    raw = str(value or "").strip()
    if not raw:
        return None

    try:
        if re.fullmatch(r"\d+(?:\.\d+)?", raw):
            return datetime.fromtimestamp(float(raw))
        if len(raw) == 10:
            return datetime.fromisoformat(raw)
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (ValueError, OverflowError, OSError, TypeError):
        return None


def _freshness_score(published: str) -> float:
    parsed = _parse_date_safe(published)
    if parsed is None:
        return 0.55

    days_old = max(0, (datetime.now(parsed.tzinfo) - parsed).days)
    if days_old <= 180:
        return 1.0
    if days_old <= 365:
        return 0.86
    if days_old <= 730:
        return 0.68
    return 0.48


def _max_allowed_age_days(source_type: str) -> int | None:
    normalized = str(source_type or "").strip().lower()
    if normalized == "paper":
        return _MAX_PAPER_AGE_DAYS
    if normalized in {"open_source", "blog"}:
        return _MAX_RECENT_SOURCE_AGE_DAYS
    return None


def _apply_hard_age_filter(candidates: list[dict]) -> tuple[list[dict], dict]:
    kept: list[dict] = []
    excluded_by_source = {
        "paper": 0,
        "open_source": 0,
        "blog": 0,
    }

    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue

        source_type = str(candidate.get("source_type") or "").strip().lower()
        allowed_days = _max_allowed_age_days(source_type)
        if allowed_days is None:
            kept.append(candidate)
            continue

        published = str(candidate.get("published") or "").strip()
        published_dt = _parse_date_safe(published)
        if published_dt is None:
            kept.append(candidate)
            continue

        days_old = max(0, (datetime.now(published_dt.tzinfo) - published_dt).days)
        if days_old > allowed_days:
            if source_type in excluded_by_source:
                excluded_by_source[source_type] += 1
            continue

        kept.append(candidate)

    excluded_total = sum(excluded_by_source.values())
    return kept, {
        "paper_max_age_days": _MAX_PAPER_AGE_DAYS,
        "recent_source_max_age_days": _MAX_RECENT_SOURCE_AGE_DAYS,
        "excluded_total": excluded_total,
        "excluded_by_source": excluded_by_source,
    }


def _authority_score(candidate: dict) -> float:
    source_type = str(candidate.get("source_type") or "").strip().lower()
    if source_type == "open_source":
        stars = max(0, int(candidate.get("stars", 0) or 0))
        return min(1.0, math.log1p(stars) / math.log1p(50000))
    if source_type == "blog":
        points = max(0, int(candidate.get("points", 0) or 0))
        return min(1.0, math.log1p(points) / math.log1p(500))
    return 0.55


def _keyword_hits(text: str, keywords: list[str]) -> tuple[int, list[str]]:
    lower_text = str(text or "").lower()
    hits: list[str] = []
    for keyword in keywords:
        normalized = str(keyword or "").lower().strip()
        if not normalized:
            continue
        if normalized in lower_text:
            hits.append(normalized)
    return len(hits), hits


def _score_candidate(candidate: dict, keywords: list[str], project_context: str) -> dict:
    title = str(candidate.get("title") or "")
    summary = str(candidate.get("summary") or "")
    metadata = " ".join(
        [
            str(candidate.get("language") or ""),
            " ".join(str(topic).strip() for topic in candidate.get("topics", []) if str(topic).strip()) if isinstance(candidate.get("topics"), list) else "",
            str(candidate.get("domain") or ""),
        ]
    )

    title_hit_count, title_hits = _keyword_hits(title, keywords)
    summary_hit_count, summary_hits = _keyword_hits(summary, keywords)
    metadata_hit_count, metadata_hits = _keyword_hits(metadata, keywords)
    project_hit_count, project_hits = _keyword_hits(project_context, title_hits + summary_hits)

    weighted_hits = (title_hit_count * 2.2) + (summary_hit_count * 1.2) + (metadata_hit_count * 0.8)
    if project_hit_count:
        weighted_hits += min(1.5, project_hit_count * 0.25)

    max_reference = max(1.0, min(float(len(keywords)), 6.0) * 2.6)
    match_score = min(1.0, weighted_hits / max_reference)

    freshness = _freshness_score(str(candidate.get("published") or ""))
    authority = _authority_score(candidate)
    source_type = str(candidate.get("source_type") or "paper").strip().lower() or "paper"
    source_prior = _SOURCE_PRIORITY.get(source_type, 0.7)

    relevance_score = (0.52 * match_score) + (0.18 * source_prior) + (0.15 * freshness) + (0.15 * authority)

    enriched = dict(candidate)
    enriched["match_keywords"] = list(dict.fromkeys(title_hits + summary_hits + metadata_hits + project_hits))[:8]
    enriched["relevance_score"] = round(max(0.0, min(1.0, relevance_score)), 4)
    return enriched


def _diversify_candidates(candidates: list[dict], limit: int) -> list[dict]:
    if not candidates:
        return []

    selected: list[dict] = []
    selected_keys: set[str] = set()

    for source_type in _DIVERSITY_SOURCE_ORDER:
        source_candidates = [item for item in candidates if str(item.get("source_type") or "").strip().lower() == source_type]
        for item in source_candidates[:2]:
            key = str(item.get("url") or item.get("title") or "")
            if key in selected_keys:
                continue
            selected.append(item)
            selected_keys.add(key)
            if len(selected) >= limit:
                return selected[:limit]

    for item in candidates:
        key = str(item.get("url") or item.get("title") or "")
        if key in selected_keys:
            continue
        selected.append(item)
        selected_keys.add(key)
        if len(selected) >= limit:
            break

    return selected[:limit]


def _rerank_candidates(project_name: str, tasks: list[dict], candidates: list[dict], limit: int = _MAX_RERANKED_CANDIDATES) -> list[dict]:
    if not candidates:
        return []

    keywords = _extract_keywords(project_name, tasks)
    project_context = _project_context_text(project_name, tasks)
    scored = [_score_candidate(candidate, keywords, project_context) for candidate in candidates if isinstance(candidate, dict)]
    scored.sort(key=lambda item: float(item.get("relevance_score", 0.0) or 0.0), reverse=True)

    deduped: list[dict] = []
    for item in scored:
        if _looks_like_duplicate(item, deduped):
            continue
        deduped.append(item)
        if len(deduped) >= max(limit * 2, limit):
            break

    return _diversify_candidates(deduped, limit)


def _normalize_enabled_sources(value) -> list[str]:
    if value is None:
        return list(_INSIGHT_RETRIEVAL_SOURCES)

    normalized: list[str] = []
    seen: set[str] = set()
    for raw in value:
        source_name = str(raw or "").strip().lower()
        if source_name not in _INSIGHT_RETRIEVAL_SOURCES:
            continue
        if source_name in seen:
            continue
        seen.add(source_name)
        normalized.append(source_name)

    return normalized


def _retrieve_candidates(
    project_name: str,
    tasks: list[dict],
    keywords: list[str],
    *,
    source_max_results: int = _MAX_CANDIDATE_RESULTS,
    rerank_limit: int = _MAX_RERANKED_CANDIDATES,
    enabled_sources: list[str] | set[str] | tuple[str, ...] | None = None,
) -> tuple[list[dict], dict]:
    source_queries: dict[str, str] = {}
    source_errors: list[str] = []
    raw_candidates: list[dict] = []
    effective_source_limit = max(1, int(source_max_results))
    effective_rerank_limit = max(1, int(rerank_limit))
    selected_sources = _normalize_enabled_sources(enabled_sources)
    query_variants = _build_query_keyword_variants(keywords)

    retriever_map = {
        "arxiv": _search_arxiv,
        "github": _search_github_repositories,
        "blog": _search_blog_articles,
        "reddit": _search_reddit_posts,
    }

    for source_name in selected_sources:
        retriever = retriever_map[source_name]
        source_variant_limit = max(1, int(_SOURCE_QUERY_VARIANT_LIMITS.get(source_name, _MAX_QUERY_VARIANTS)))
        source_variants = query_variants[:source_variant_limit]
        source_query_list: list[str] = []
        source_errors_local: list[str] = []
        source_batches: list[list[dict]] = []

        for variant_keywords in source_variants:
            try:
                candidates, query = retriever(variant_keywords, max_results=effective_source_limit)
                source_batches.append(candidates if isinstance(candidates, list) else [])
                query_text = str(query or "").strip()
                if query_text:
                    source_query_list.append(query_text)
            except Exception as exc:
                source_errors_local.append(str(exc))

        merged_source_candidates = _merge_candidate_batches(source_batches)
        raw_candidates.extend(merged_source_candidates)

        if source_query_list:
            unique_queries = [q for q in dict.fromkeys(source_query_list) if str(q or "").strip()]
            if unique_queries:
                source_queries[source_name] = " || ".join(unique_queries)

        if source_errors_local:
            if merged_source_candidates:
                source_errors.append(
                    f"{source_name}: {len(source_errors_local)} query variant(s) failed but recovered"
                )
            else:
                source_errors.append(f"{source_name}: {source_errors_local[0]}")

    filtered_candidates, age_filter_meta = _apply_hard_age_filter(raw_candidates)
    reranked = _rerank_candidates(project_name, tasks, filtered_candidates, limit=effective_rerank_limit)
    retrieval_meta = {
        "source": "multi",
        "enabled_sources": selected_sources,
        "query": " | ".join(f"{name}={query}" for name, query in source_queries.items() if query),
        "candidate_count": len(reranked),
        "raw_candidate_count": len(raw_candidates),
        "post_age_filter_count": len(filtered_candidates),
        "source_max_results": effective_source_limit,
        "query_variant_count": len(query_variants),
        "rerank_limit": effective_rerank_limit,
        "age_filter": age_filter_meta,
        "source_errors": source_errors,
    }
    return reranked, retrieval_meta


def _normalize_card(raw_card: dict, index: int) -> dict | None:
    if not isinstance(raw_card, dict):
        return None

    title = str(raw_card.get("title") or raw_card.get("source_title") or "").strip()
    url = str(raw_card.get("url") or raw_card.get("source_url") or "").strip()
    source_type = str(raw_card.get("source_type") or "paper").strip().lower() or "paper"
    source = str(raw_card.get("source") or raw_card.get("source_name") or "").strip().lower()
    if not source and url:
        source = _infer_source_from_url(url)

    core_insight = str(raw_card.get("core_insight") or raw_card.get("insight") or "").strip()
    risk_alert = str(raw_card.get("risk_alert") or raw_card.get("warning") or "").strip()
    alternative = str(raw_card.get("alternative") or raw_card.get("alternative_direction") or "").strip()
    relevance_reason = str(raw_card.get("relevance_reason") or raw_card.get("why_relevant") or "").strip()

    if not title or not url:
        return None
    if not core_insight:
        core_insight = "这项工作处理了与你当前项目相近的问题，值得作为近期设计参考。"
    if not risk_alert:
        risk_alert = "你当前方案中的关键假设可能在边界场景下失效，建议先做小规模验证。"
    if not alternative:
        alternative = "可以先以小范围替代模块试点，再决定是否整体切换。"

    score = raw_card.get("relevance_score", 0.75)
    try:
        relevance_score = max(0.0, min(1.0, float(score)))
    except (TypeError, ValueError):
        relevance_score = 0.75

    return {
        "card_id": str(raw_card.get("card_id") or f"insight_{index}_{uuid.uuid4().hex[:8]}"),
        "source": source,
        "source_type": source_type,
        "title": title,
        "url": url,
        "core_insight": core_insight,
        "risk_alert": risk_alert,
        "alternative": alternative,
        "relevance_reason": relevance_reason,
        "evidence_snippet": str(raw_card.get("evidence_snippet") or "").strip(),
        "relevance_score": round(relevance_score, 4),
    }


def _fallback_cards(
    project_name: str,
    tasks: list[dict],
    candidates: list[dict],
    count: int,
    project_summary_snippets: list[dict] | None = None,
) -> list[dict]:
    summary_snippets = project_summary_snippets if isinstance(project_summary_snippets, list) else []
    cards: list[dict] = []
    for idx, candidate in enumerate(candidates[: max(1, count)], start=1):
        title = str(candidate.get("title") or "").strip()
        url = str(candidate.get("url") or "").strip()
        summary = str(candidate.get("summary") or "").strip()
        if not title or not url:
            continue
        cards.append(
            {
                "card_id": f"insight_fallback_{idx}_{uuid.uuid4().hex[:8]}",
                "source": str(candidate.get("source") or "").strip() or _infer_source_from_url(url),
                "source_type": str(candidate.get("source_type") or "paper").strip().lower() or "paper",
                "title": title,
                "url": url,
                "core_insight": f"这项工作与{project_name or '当前项目'}的问题域接近，可作为建模与评估基线参考。",
                "risk_alert": "",
                "alternative": "建议先增加一个对照分支，把该方法作为并行实验而非直接替换主链路。",
                "relevance_reason": "基于项目关键词检索得到，主题相关度较高。",
                "evidence_snippet": summary[:240],
                "relevance_score": 0.65,
            }
        )
    return _finalize_cards_with_evidence(cards, candidates, tasks, summary_snippets)


def _generate_cards_with_llm(
    project_name: str,
    tasks: list[dict],
    candidates: list[dict],
    card_count: int,
    project_summary_snippets: list[dict] | None = None,
    recommendation_rules_markdown: str = "",
) -> list[dict]:
    if not candidates:
        return []

    summary_snippets = project_summary_snippets if isinstance(project_summary_snippets, list) else []

    objective = ""
    task_samples: list[str] = []
    for task in tasks[:12]:
        if not isinstance(task, dict):
            continue
        if not objective:
            objective = str(task.get("objective", "") or "").strip()
        task_name = str(task.get("task", "") or "").strip()
        if task_name:
            task_samples.append(task_name)

    summary_sample = [
        {
            "filename": str(item.get("filename") or "").strip(),
            "snippet": _trim_evidence_quote(str(item.get("snippet") or ""), max_chars=180),
        }
        for item in summary_snippets[:3]
        if isinstance(item, dict)
    ]

    prompt = (
        "你是 Insight Engine。请基于候选来源生成 1-3 条高相关 insight 卡片。\n"
        "若提供了全局推送偏好规则，请优先遵循其中的偏好并规避明确反例。\n"
        "卡片必须同时包含 inspiration + warning + critique，且 warning 需要基于来源证据，不要空泛。\n"
        "risk_alert 必须是双子句格式：\n"
        "1) 以“引用：”开头，给出来源中的明确原句/关键短句并点名来源（source+title）。\n"
        "2) 以“关联：”开头，明确说明该引用如何关联到用户的 planned task（task_id/task）或工作总结文件。\n"
        "只输出 JSON，不要 markdown。格式：\n"
        "{\"cards\":[{\"source\":\"arxiv|github|reddit|hn_algolia|...\",\"source_type\":\"paper|blog|open_source\",\"title\":\"...\",\"url\":\"...\","
        "\"core_insight\":\"一句话洞察\",\"risk_alert\":\"证据化风险提醒\",\"alternative\":\"替代方向\","
        "\"relevance_reason\":\"为何与当前项目相关\",\"evidence_snippet\":\"来源中的一句证据\",\"relevance_score\":0.0-1.0}]}\n\n"
        f"当前项目名：{project_name}\n"
        f"当前目标：{objective}\n"
        f"当前任务样例：{json.dumps(task_samples[:8], ensure_ascii=False)}\n"
        f"工作总结片段（可为空）：{json.dumps(summary_sample, ensure_ascii=False)}\n"
        f"全局推送偏好规则（可为空）：{recommendation_rules_markdown or '（空）'}\n"
        f"期望卡片数量：{max(1, min(3, card_count))}\n"
        f"候选来源：{json.dumps(candidates[:8], ensure_ascii=False)}"
    )

    model_name = _resolve_insight_model()
    response = call_llm_messages(
        [
            {
                "role": "system",
                "content": "你只返回 JSON。risk_alert 必须包含“引用：”与“关联：”两个部分，禁止空泛表述，并尽量遵循给定偏好规则。",
            },
            {"role": "user", "content": prompt},
        ],
        inject_system_memory=False,
        model_override=model_name,
        trace_label="insight_engine_v1",
    )

    parsed = extract_json(response)
    if isinstance(parsed, dict):
        raw_cards = parsed.get("cards", [])
    elif isinstance(parsed, list):
        raw_cards = parsed
    else:
        raw_cards = []

    normalized: list[dict] = []
    for idx, raw_card in enumerate(raw_cards, start=1):
        card = _normalize_card(raw_card, idx)
        if card:
            normalized.append(card)
        if len(normalized) >= max(1, min(3, card_count)):
            break

    return normalized


def generate_insight_feed(
    project_name: str,
    tasks: list[dict],
    *,
    card_count: int = _DEFAULT_CARD_COUNT,
    candidate_limit: int | None = None,
    rerank_limit: int | None = None,
    exclude_url_keys: list[str] | set[str] | tuple[str, ...] | None = None,
    project_summary_snippets: list[dict] | None = None,
    recommendation_rules_markdown: str = "",
    enabled_sources: list[str] | set[str] | tuple[str, ...] | None = None,
) -> dict:
    today = date.today().isoformat()
    generated_at = datetime.now().isoformat(timespec="seconds")

    effective_count = max(1, min(3, int(card_count or _DEFAULT_CARD_COUNT)))
    if candidate_limit is None:
        effective_candidate_limit = _MAX_CANDIDATE_RESULTS
    else:
        try:
            effective_candidate_limit = int(candidate_limit)
        except (TypeError, ValueError):
            effective_candidate_limit = _MAX_CANDIDATE_RESULTS
    effective_candidate_limit = max(_MIN_CANDIDATE_RESULTS, min(_MAX_CANDIDATE_RESULTS_PER_PROJECT, effective_candidate_limit))

    if rerank_limit is None:
        inferred_rerank_limit = max(_MAX_RERANKED_CANDIDATES, effective_candidate_limit * 2)
        effective_rerank_limit = min(_MAX_RERANKED_CANDIDATES_PER_PROJECT, inferred_rerank_limit)
    else:
        try:
            effective_rerank_limit = int(rerank_limit)
        except (TypeError, ValueError):
            effective_rerank_limit = _MAX_RERANKED_CANDIDATES
        effective_rerank_limit = max(3, min(_MAX_RERANKED_CANDIDATES_PER_PROJECT, effective_rerank_limit))

    excluded_url_keys = _normalize_excluded_url_keys(exclude_url_keys)
    keywords = _extract_keywords(project_name, tasks)

    try:
        candidates, retrieval_meta = _retrieve_candidates(
            project_name,
            tasks,
            keywords,
            source_max_results=effective_candidate_limit,
            rerank_limit=effective_rerank_limit,
            enabled_sources=enabled_sources,
        )
    except Exception as exc:
        return {
            "status": "error",
            "message": f"insight 检索失败：{exc}",
            "feed": None,
        }

    excluded_by_history = 0
    if excluded_url_keys:
        candidates, excluded_by_history = _filter_candidates_by_excluded_urls(candidates, excluded_url_keys)
        if isinstance(retrieval_meta, dict):
            retrieval_meta["excluded_by_history"] = excluded_by_history
            retrieval_meta["candidate_count"] = len(candidates)

    if not candidates:
        enabled_sources_meta = retrieval_meta.get("enabled_sources", []) if isinstance(retrieval_meta, dict) else []
        if isinstance(enabled_sources_meta, list) and not enabled_sources_meta:
            return {
                "status": "error",
                "message": "未启用任何信息来源，无法生成 insight。",
                "feed": None,
            }
        age_filter = retrieval_meta.get("age_filter", {}) if isinstance(retrieval_meta, dict) else {}
        excluded_total = int(age_filter.get("excluded_total", 0) or 0) if isinstance(age_filter, dict) else 0
        if excluded_by_history > 0:
            return {
                "status": "error",
                "message": "候选内容均已在历史 insight 展示过，暂未生成重复卡片。",
                "feed": None,
            }
        if excluded_total > 0:
            return {
                "status": "error",
                "message": "未检索到满足时效阈值的候选（论文<=1年，开源/博客<=3周）。",
                "feed": None,
            }
        return {
            "status": "error",
            "message": "未检索到候选论文，今日 insight 暂未生成。",
            "feed": None,
        }

    summary_snippets = project_summary_snippets if isinstance(project_summary_snippets, list) else []

    cards: list[dict] = []
    llm_error = ""
    try:
        cards = _generate_cards_with_llm(
            project_name,
            tasks,
            candidates,
            effective_count,
            project_summary_snippets=summary_snippets,
            recommendation_rules_markdown=str(recommendation_rules_markdown or "").strip(),
        )
    except Exception as exc:
        llm_error = str(exc)

    if not cards:
        cards = _fallback_cards(
            project_name,
            tasks,
            candidates,
            effective_count,
            project_summary_snippets=summary_snippets,
        )

    cards = _finalize_cards_with_evidence(cards, candidates, tasks, summary_snippets)

    if not cards:
        return {
            "status": "error",
            "message": "insight 生成失败：未得到有效卡片。",
            "feed": None,
        }

    feed = {
        "feed_id": f"feed_{today}_{uuid.uuid4().hex[:8]}",
        "date": today,
        "generated_at": generated_at,
        "cards": cards,
        "keywords": keywords[:12],
        "retrieval": retrieval_meta,
    }

    message = "今日 insight 已生成。"
    source_errors = retrieval_meta.get("source_errors", []) if isinstance(retrieval_meta, dict) else []
    if isinstance(source_errors, list):
        error_text = "\n".join(str(item).lower() for item in source_errors)
        if "reddit" in error_text:
            message += "（Reddit 源暂不可达，已自动跳过）"
    if llm_error:
        message += "（已启用兜底模板）"

    return {
        "status": "ok",
        "message": message,
        "feed": feed,
    }
