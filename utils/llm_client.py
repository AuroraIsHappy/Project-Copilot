import os
import asyncio
import json
from datetime import datetime
from pathlib import Path

from state.assistant_memory import read_system_memory
from utils.json_utils import read_json
from utils.json_utils import write_json

try:
    import keyring
except ImportError:
    keyring = None


SERVICE_NAME = "project_copilot"
LEGACY_SERVICE_NAMES = ("okr_project_assistant", "okr_project_planner")
ACCOUNT_NAME = "llm_api_key"
CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.json"
LLM_ATTEMPT_LOG_PATH = Path(__file__).resolve().parents[1] / "data" / "debug" / "llm_attempt_log.txt"
DEFAULT_PROVIDER = "openai"
UNCONFIGURED_PROVIDER = ""
DEFAULT_MAX_TOKENS = 3000
MAX_TOKENS_HARD_CAP = 8000
DEFAULT_PROGRESS_MAX_WORKERS = 3
PROGRESS_MAX_WORKERS_HARD_CAP = 16
DEFAULT_PROGRESS_LLM_MAX_OUTPUT_TOKENS = 5000
PROGRESS_LLM_MAX_OUTPUT_TOKENS_HARD_CAP = 8000
PROVIDER_REGISTRY = {
    "custom": {
        "label": "Custom (OpenAI-Compatible)",
        "default_base_url": "https://openrouter.ai/api/v1",
        "default_model": "openai/gpt-4o-mini",
        "env_key": "CUSTOM_API_KEY",
        "api_key_optional": True,
    },
    "azure_openai": {
        "label": "Azure OpenAI",
        "default_base_url": "",
        "default_model": "gpt-4o-mini",
        "env_key": "AZURE_OPENAI_API_KEY",
        "api_key_optional": False,
    },
    "anthropic": {
        "label": "Anthropic",
        "default_base_url": "",
        "default_model": "claude-3-5-sonnet-20241022",
        "env_key": "ANTHROPIC_API_KEY",
        "api_key_optional": False,
    },
    "openai": {
        "label": "OpenAI",
        "default_base_url": "https://api.openai.com/v1",
        "default_model": "gpt-4o-mini",
        "env_key": "OPENAI_API_KEY",
        "api_key_optional": False,
    },
    "openrouter": {
        "label": "OpenRouter",
        "default_base_url": "https://openrouter.ai/api/v1",
        "default_model": "openai/gpt-4o-mini",
        "env_key": "OPENROUTER_API_KEY",
        "api_key_optional": False,
    },
    "deepseek": {
        "label": "DeepSeek",
        "default_base_url": "https://api.deepseek.com/v1",
        "default_model": "deepseek-chat",
        "env_key": "DEEPSEEK_API_KEY",
        "api_key_optional": False,
    },
    "groq": {
        "label": "Groq",
        "default_base_url": "https://api.groq.com/openai/v1",
        "default_model": "llama-3.1-70b-versatile",
        "env_key": "GROQ_API_KEY",
        "api_key_optional": False,
    },
    "zhipu": {
        "label": "Zhipu",
        "default_base_url": "https://open.bigmodel.cn/api/paas/v4",
        "default_model": "glm-4.6",
        "env_key": "ZHIPUAI_API_KEY",
        "api_key_optional": False,
    },
    "dashscope": {
        "label": "DashScope",
        "default_base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
        "default_model": "qwen-max",
        "env_key": "DASHSCOPE_API_KEY",
        "api_key_optional": False,
    },
    "gemini": {
        "label": "Gemini",
        "default_base_url": "",
        "default_model": "gemini-1.5-pro",
        "env_key": "GEMINI_API_KEY",
        "api_key_optional": False,
    },
    "moonshot": {
        "label": "Moonshot",
        "default_base_url": "https://api.moonshot.cn/v1",
        "default_model": "moonshot-v1-8k",
        "env_key": "MOONSHOT_API_KEY",
        "api_key_optional": False,
    },
    "minimax": {
        "label": "MiniMax",
        "default_base_url": "https://api.minimax.io/v1",
        "default_model": "MiniMax-M1",
        "env_key": "MINIMAX_API_KEY",
        "api_key_optional": False,
    },
    "aihubmix": {
        "label": "AiHubMix",
        "default_base_url": "https://aihubmix.com/v1",
        "default_model": "gpt-4o-mini",
        "env_key": "AIHUBMIX_API_KEY",
        "api_key_optional": False,
    },
    "siliconflow": {
        "label": "SiliconFlow",
        "default_base_url": "https://api.siliconflow.cn/v1",
        "default_model": "Qwen/Qwen2.5-7B-Instruct",
        "env_key": "SILICONFLOW_API_KEY",
        "api_key_optional": False,
    },
    "volcengine": {
        "label": "VolcEngine",
        "default_base_url": "https://ark.cn-beijing.volces.com/api/v3",
        "default_model": "doubao-seed-1-6-250615",
        "env_key": "ARK_API_KEY",
        "api_key_optional": False,
    },
    "openai_codex": {
        "label": "OpenAI Codex",
        "default_base_url": "https://chatgpt.com/backend-api",
        "default_model": "gpt-5-codex",
        "env_key": "OPENAI_CODEX_API_KEY",
        "api_key_optional": True,
    },
    "github_copilot": {
        "label": "GitHub Copilot",
        "default_base_url": "",
        "default_model": "github_copilot/gpt-4o-mini",
        "env_key": "GITHUB_COPILOT_API_KEY",
        "api_key_optional": True,
    },
    "ollama": {
        "label": "Ollama (Local)",
        "default_base_url": "http://localhost:11434/v1",
        "default_model": "llama3.2",
        "env_key": "OLLAMA_API_KEY",
        "api_key_optional": True,
    },
    "vllm": {
        "label": "vLLM (Local)",
        "default_base_url": "http://localhost:8000/v1",
        "default_model": "meta-llama/Llama-3.1-8B-Instruct",
        "env_key": "VLLM_API_KEY",
        "api_key_optional": True,
    },
}

_TOKEN_USAGE_TRACKER = {
    "prompt_tokens": 0,
    "completion_tokens": 0,
    "total_tokens": 0,
    "request_count": 0,
    "logical_call_count": 0,
    "empty_response_retry_count": 0,
}


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def reset_token_usage_tracker() -> None:
    _TOKEN_USAGE_TRACKER["prompt_tokens"] = 0
    _TOKEN_USAGE_TRACKER["completion_tokens"] = 0
    _TOKEN_USAGE_TRACKER["total_tokens"] = 0
    _TOKEN_USAGE_TRACKER["request_count"] = 0
    _TOKEN_USAGE_TRACKER["logical_call_count"] = 0
    _TOKEN_USAGE_TRACKER["empty_response_retry_count"] = 0


def get_token_usage_tracker() -> dict:
    return {
        "prompt_tokens": _TOKEN_USAGE_TRACKER["prompt_tokens"],
        "completion_tokens": _TOKEN_USAGE_TRACKER["completion_tokens"],
        "total_tokens": _TOKEN_USAGE_TRACKER["total_tokens"],
        "request_count": _TOKEN_USAGE_TRACKER["request_count"],
        "logical_call_count": _TOKEN_USAGE_TRACKER["logical_call_count"],
        "empty_response_retry_count": _TOKEN_USAGE_TRACKER["empty_response_retry_count"],
    }


def _append_llm_attempt_log(payload: dict) -> None:
    record = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        **payload,
    }
    try:
        LLM_ATTEMPT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LLM_ATTEMPT_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    except Exception:
        # Debug logging must never break the main request flow.
        return


def _record_token_usage(response) -> None:
    usage = getattr(response, "usage", None)
    if usage is None:
        return

    prompt_tokens = _safe_int(getattr(usage, "prompt_tokens", 0), 0)
    completion_tokens = _safe_int(getattr(usage, "completion_tokens", 0), 0)
    total_tokens = _safe_int(getattr(usage, "total_tokens", 0), 0)
    if total_tokens <= 0:
        total_tokens = prompt_tokens + completion_tokens

    _TOKEN_USAGE_TRACKER["prompt_tokens"] += prompt_tokens
    _TOKEN_USAGE_TRACKER["completion_tokens"] += completion_tokens
    _TOKEN_USAGE_TRACKER["total_tokens"] += total_tokens
    _TOKEN_USAGE_TRACKER["request_count"] += 1


def _record_usage_dict(usage: dict | None) -> None:
    if not isinstance(usage, dict):
        return
    prompt_tokens = _safe_int(usage.get("prompt_tokens", 0), 0)
    completion_tokens = _safe_int(usage.get("completion_tokens", 0), 0)
    total_tokens = _safe_int(usage.get("total_tokens", 0), 0)
    if total_tokens <= 0:
        total_tokens = prompt_tokens + completion_tokens
    _TOKEN_USAGE_TRACKER["prompt_tokens"] += prompt_tokens
    _TOKEN_USAGE_TRACKER["completion_tokens"] += completion_tokens
    _TOKEN_USAGE_TRACKER["total_tokens"] += total_tokens
    _TOKEN_USAGE_TRACKER["request_count"] += 1


def _run_async(coro):
    """Run coroutine in sync context, handling environments with active event loops."""
    try:
        return asyncio.run(coro)
    except RuntimeError as exc:
        if "asyncio.run() cannot be called" not in str(exc):
            raise
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


def _call_llm_via_provider(
    messages: list[dict],
    *,
    provider_name: str,
    api_key: str,
    base_url: str,
    model: str,
    max_tokens: int,
    trace_label: str | None = None,
) -> str:
    """Provider abstraction path. Raises on error for caller retry."""
    from providers.litellm_provider import LiteLLMProvider

    impl = LiteLLMProvider(
        api_key=api_key or None,
        api_base=base_url,
        default_model=model,
        provider_name=provider_name,
    )

    max_empty_attempts = 3
    last_empty_reason = "unknown"
    label = (trace_label or "llm_call").strip() or "llm_call"
    for attempt in range(1, max_empty_attempts + 1):
        response = _run_async(
            impl.chat_with_retry(
                messages=messages,
                model=model,
                max_tokens=max_tokens,
                temperature=0.2,
            )
        )

        finish_reason = str(getattr(response, "finish_reason", "") or "unknown")
        if finish_reason == "error":
            err_message = str(getattr(response, "content", "LLM provider returned error"))
            _append_llm_attempt_log(
                {
                    "label": label,
                    "status": "error",
                    "provider": provider_name,
                    "model": model,
                    "attempt": attempt,
                    "max_empty_attempts": max_empty_attempts,
                    "finish_reason": finish_reason,
                    "error": err_message,
                }
            )
            raise RuntimeError(err_message)

        usage = getattr(response, "usage", {})
        _record_usage_dict(usage)

        text = getattr(response, "content", "") or ""
        if not isinstance(text, str):
            text = str(text)
        stripped_text = text.strip()
        content_chars = len(stripped_text)
        if stripped_text:
            _append_llm_attempt_log(
                {
                    "label": label,
                    "status": "ok",
                    "provider": provider_name,
                    "model": model,
                    "attempt": attempt,
                    "max_empty_attempts": max_empty_attempts,
                    "finish_reason": finish_reason,
                    "content_chars": content_chars,
                    "usage": usage,
                }
            )
            return text

        last_empty_reason = finish_reason
        _append_llm_attempt_log(
            {
                "label": label,
                "status": "empty",
                "provider": provider_name,
                "model": model,
                "attempt": attempt,
                "max_empty_attempts": max_empty_attempts,
                "finish_reason": finish_reason,
                "content_chars": content_chars,
                "usage": usage,
            }
        )
        # Some providers occasionally return empty content with finish_reason=stop.
        # Retry the same model a few times before bubbling up an error.
        if attempt < max_empty_attempts:
            _TOKEN_USAGE_TRACKER["empty_response_retry_count"] += 1
            continue

    raise RuntimeError(
        "LLM provider returned empty content "
        f"(provider={provider_name}, model={model}, finish_reason={last_empty_reason})"
    )


def _inject_system_memory(messages: list[dict], enabled: bool = True) -> list[dict]:
    if not enabled:
        return list(messages)

    memory_text = read_system_memory().strip()
    if not memory_text:
        return list(messages)

    memory_block = (
        "[Shared System Memory: apply across all projects unless explicitly overridden]\n"
        + memory_text
    )
    prepared = [dict(msg) for msg in messages]
    if prepared and prepared[0].get("role") == "system":
        existing = str(prepared[0].get("content", "") or "").strip()
        prepared[0]["content"] = memory_block + ("\n\n" + existing if existing else "")
        return prepared

    return [{"role": "system", "content": memory_block}, *prepared]


def _resolve_provider(provider: str | None, *, allow_unconfigured: bool = False) -> str:
    name = (provider or "").strip().lower()
    if name in PROVIDER_REGISTRY:
        return name
    if allow_unconfigured:
        return UNCONFIGURED_PROVIDER
    return DEFAULT_PROVIDER


def _provider_account_name(provider: str) -> str:
    return f"{ACCOUNT_NAME}::{provider}"


def get_provider_specs() -> dict:
    return {
        name: {
            **spec,
            "default_max_tokens": DEFAULT_MAX_TOKENS,
            "default_assistant_model": spec.get("default_model", ""),
            "default_progress_max_workers": DEFAULT_PROGRESS_MAX_WORKERS,
            "default_progress_llm_max_output_tokens": DEFAULT_PROGRESS_LLM_MAX_OUTPUT_TOKENS,
            "default_progress_model": spec.get("default_model", ""),
        }
        for name, spec in PROVIDER_REGISTRY.items()
    }


def _normalize_max_tokens(value: int | str | None) -> int:
    try:
        tokens = int(value) if value is not None else DEFAULT_MAX_TOKENS
    except (TypeError, ValueError):
        tokens = DEFAULT_MAX_TOKENS

    if tokens < 100:
        return 100
    if tokens > MAX_TOKENS_HARD_CAP:
        return MAX_TOKENS_HARD_CAP
    return tokens


def _normalize_progress_max_workers(value: int | str | None) -> int:
    try:
        workers = int(value) if value is not None else DEFAULT_PROGRESS_MAX_WORKERS
    except (TypeError, ValueError):
        workers = DEFAULT_PROGRESS_MAX_WORKERS

    if workers < 1:
        return 1
    if workers > PROGRESS_MAX_WORKERS_HARD_CAP:
        return PROGRESS_MAX_WORKERS_HARD_CAP
    return workers


def _normalize_progress_llm_max_output_tokens(value: int | str | None) -> int:
    try:
        tokens = int(value) if value is not None else DEFAULT_PROGRESS_LLM_MAX_OUTPUT_TOKENS
    except (TypeError, ValueError):
        tokens = DEFAULT_PROGRESS_LLM_MAX_OUTPUT_TOKENS

    if tokens < 100:
        return 100
    if tokens > PROGRESS_LLM_MAX_OUTPUT_TOKENS_HARD_CAP:
        return PROGRESS_LLM_MAX_OUTPUT_TOKENS_HARD_CAP
    return tokens


def load_llm_config() -> dict:
    file_config = read_json(CONFIG_PATH, default={})
    provider = _resolve_provider(file_config.get("provider"), allow_unconfigured=True)

    providers_config = file_config.get("providers", {})
    if not isinstance(providers_config, dict):
        providers_config = {}

    selected_config = providers_config.get(provider, {}) if provider else {}
    if not isinstance(selected_config, dict):
        selected_config = {}

    max_tokens = _normalize_max_tokens(selected_config.get("max_tokens", DEFAULT_MAX_TOKENS))
    progress_max_workers = _normalize_progress_max_workers(
        selected_config.get("progress_max_workers", DEFAULT_PROGRESS_MAX_WORKERS)
    )
    progress_llm_max_output_tokens = _normalize_progress_llm_max_output_tokens(
        selected_config.get(
            "progress_llm_max_output_tokens",
            DEFAULT_PROGRESS_LLM_MAX_OUTPUT_TOKENS,
        )
    )

    if provider:
        provider_spec = PROVIDER_REGISTRY[provider]
        base_url = selected_config.get("base_url", provider_spec["default_base_url"])
        model = selected_config.get("model", provider_spec["default_model"])
        assistant_model = str(selected_config.get("assistant_model", model) or model).strip() or model
        progress_model = str(selected_config.get("progress_model", model) or model).strip() or model
        env_key_name = provider_spec.get("env_key", "")
        env_key_value = (os.getenv(env_key_name, "").strip() if env_key_name else "")
        compat_env_key = os.getenv("OKR_OPENAI_API_KEY", "").strip()
        provider_label = provider_spec["label"]
        api_key_optional = provider_spec["api_key_optional"]
        has_api_key = bool(get_api_key(provider))
        use_env_key = bool(env_key_value or compat_env_key)
    else:
        base_url = ""
        model = ""
        assistant_model = ""
        progress_model = ""
        env_key_name = ""
        provider_label = "未配置"
        api_key_optional = False
        has_api_key = False
        use_env_key = False

    config = {
        "provider": provider,
        "provider_label": provider_label,
        "is_configured": bool(provider),
        "base_url": base_url,
        "model": model,
        "assistant_model": assistant_model,
        "progress_model": progress_model,
        "max_tokens": max_tokens,
        "progress_max_workers": progress_max_workers,
        "progress_llm_max_output_tokens": progress_llm_max_output_tokens,
        "env_key": env_key_name,
        "api_key_optional": api_key_optional,
        "has_api_key": has_api_key,
        "use_env_key": use_env_key,
        "provider_options": [
            {
                "name": name,
                "label": spec["label"],
                "default_base_url": spec["default_base_url"],
                "default_model": spec["default_model"],
                "default_assistant_model": spec["default_model"],
                "default_progress_model": spec["default_model"],
                "default_max_tokens": DEFAULT_MAX_TOKENS,
                "default_progress_max_workers": DEFAULT_PROGRESS_MAX_WORKERS,
                "default_progress_llm_max_output_tokens": DEFAULT_PROGRESS_LLM_MAX_OUTPUT_TOKENS,
                "api_key_optional": spec["api_key_optional"],
            }
            for name, spec in PROVIDER_REGISTRY.items()
        ],
    }
    return config


def save_llm_config(
    provider: str,
    model: str,
    base_url: str | None = None,
    max_tokens: int | None = None,
    progress_max_workers: int | None = None,
    progress_llm_max_output_tokens: int | None = None,
    progress_model: str | None = None,
    assistant_model: str | None = None,
) -> bool:
    raw = read_json(CONFIG_PATH, default={})
    if not isinstance(raw, dict):
        raw = {}

    providers = raw.get("providers", {})
    if not isinstance(providers, dict):
        providers = {}

    provider_name = _resolve_provider(provider, allow_unconfigured=True)
    if not provider_name:
        raise ValueError("请先选择 Provider。")

    provider_spec = PROVIDER_REGISTRY[provider_name]
    selected_base_url = (base_url or "").strip() or provider_spec["default_base_url"]
    selected_model = model.strip() or provider_spec["default_model"]
    existing_selected = providers.get(provider_name, {}) if isinstance(providers.get(provider_name, {}), dict) else {}
    selected_max_tokens = _normalize_max_tokens(existing_selected.get("max_tokens", DEFAULT_MAX_TOKENS))
    selected_progress_max_workers = _normalize_progress_max_workers(
        existing_selected.get("progress_max_workers", DEFAULT_PROGRESS_MAX_WORKERS)
    )
    selected_progress_llm_max_output_tokens = _normalize_progress_llm_max_output_tokens(
        existing_selected.get(
            "progress_llm_max_output_tokens",
            DEFAULT_PROGRESS_LLM_MAX_OUTPUT_TOKENS,
        )
    )
    selected_assistant_model = str(existing_selected.get("assistant_model", selected_model) or selected_model).strip() or selected_model
    selected_progress_model = str(existing_selected.get("progress_model", selected_model) or selected_model).strip() or selected_model
    if max_tokens is not None:
        selected_max_tokens = _normalize_max_tokens(max_tokens)
    if progress_max_workers is not None:
        selected_progress_max_workers = _normalize_progress_max_workers(progress_max_workers)
    if progress_llm_max_output_tokens is not None:
        selected_progress_llm_max_output_tokens = _normalize_progress_llm_max_output_tokens(
            progress_llm_max_output_tokens
        )
    if assistant_model is not None:
        selected_assistant_model = str(assistant_model).strip() or selected_model
    if progress_model is not None:
        selected_progress_model = str(progress_model).strip() or selected_model

    providers[provider_name] = {
        "base_url": selected_base_url,
        "model": selected_model,
        "assistant_model": selected_assistant_model,
        "progress_model": selected_progress_model,
        "max_tokens": selected_max_tokens,
        "progress_max_workers": selected_progress_max_workers,
        "progress_llm_max_output_tokens": selected_progress_llm_max_output_tokens,
    }

    payload = {
        "provider": provider_name,
        "providers": providers,
    }
    return write_json(CONFIG_PATH, payload)


def reset_llm_config() -> bool:
    raw = read_json(CONFIG_PATH, default={})
    if not isinstance(raw, dict):
        raw = {}

    providers = raw.get("providers", {})
    if not isinstance(providers, dict):
        providers = {}

    payload = {
        "provider": UNCONFIGURED_PROVIDER,
        "providers": providers,
    }
    return write_json(CONFIG_PATH, payload)


def set_api_key(api_key: str, provider: str) -> None:
    clean_key = api_key.strip()
    if not clean_key:
        raise ValueError("API Key 不能为空")

    if keyring is None:
        raise RuntimeError("缺少 keyring 依赖，无法安全持久化 API Key")

    provider_name = _resolve_provider(provider, allow_unconfigured=True)
    if not provider_name:
        raise ValueError("请先选择 Provider。")

    keyring.set_password(SERVICE_NAME, _provider_account_name(provider_name), clean_key)


def get_api_key(provider: str) -> str:
    provider_name = _resolve_provider(provider, allow_unconfigured=True)
    if not provider_name:
        return ""

    # 环境变量优先，便于部署时走 CI/CD Secret 管理。
    env_key = os.getenv("OKR_OPENAI_API_KEY", "").strip()
    if env_key:
        return env_key

    provider_env_key = PROVIDER_REGISTRY[provider_name].get("env_key", "")
    if provider_env_key:
        provider_env_value = os.getenv(provider_env_key, "").strip()
        if provider_env_value:
            return provider_env_value

    if keyring is None:
        return ""

    service_candidates = (SERVICE_NAME, *LEGACY_SERVICE_NAMES)

    try:
        for service_name in service_candidates:
            provider_key = keyring.get_password(service_name, _provider_account_name(provider_name))
            if provider_key:
                return provider_key.strip()

        # 兼容历史版本的单账号存储。
        for service_name in service_candidates:
            legacy_key = keyring.get_password(service_name, ACCOUNT_NAME)
            if legacy_key:
                return legacy_key.strip()
        return ""
    except Exception:
        return ""


def clear_api_key(provider: str) -> None:
    if keyring is None:
        return

    provider_name = _resolve_provider(provider, allow_unconfigured=True)
    if not provider_name:
        return

    for service_name in (SERVICE_NAME, *LEGACY_SERVICE_NAMES):
        try:
            keyring.delete_password(service_name, _provider_account_name(provider_name))
        except Exception:
            pass
        try:
            keyring.delete_password(service_name, ACCOUNT_NAME)
        except Exception:
            pass


def _build_model_candidates(provider: str, configured_model: str) -> list[str]:
    spec = PROVIDER_REGISTRY.get(provider, {})
    fallback_models = spec.get("fallback_models", [])
    candidates: list[str] = []

    configured = (configured_model or "").strip()
    if configured:
        candidates.append(configured)

    for model in fallback_models:
        if model and model not in candidates:
            candidates.append(model)

    default_model = spec.get("default_model", "")
    if default_model and default_model not in candidates:
        candidates.append(default_model)

    return candidates


def call_llm_messages(
    messages: list[dict],
    inject_system_memory: bool = True,
    max_tokens_override: int | None = None,
    model_override: str | None = None,
    trace_label: str | None = None,
) -> str:
    _TOKEN_USAGE_TRACKER["logical_call_count"] += 1
    config = load_llm_config()
    provider = str(config.get("provider") or "").strip()
    if not provider:
        raise ValueError("未配置 LLM Provider，请先在侧边栏 LLM 配置中选择 Provider 并保存。")

    api_key = get_api_key(provider)
    if not api_key and not config["api_key_optional"]:
        raise ValueError("未检测到 API Key，请先在 UI 配置或设置 OKR_OPENAI_API_KEY")

    override_model = (model_override or "").strip()
    selected_model = override_model or config["model"]
    candidates = _build_model_candidates(provider, selected_model)
    last_error: Exception | None = None
    base_url = config["base_url"]
    max_tokens = _normalize_max_tokens(config.get("max_tokens"))
    if max_tokens_override is not None:
        max_tokens = min(max_tokens, _normalize_max_tokens(max_tokens_override))
    prepared_messages = _inject_system_memory(messages, enabled=inject_system_memory)

    for model_idx, model in enumerate(candidates):
        try:
            # Provider-only path.
            text = _call_llm_via_provider(
                prepared_messages,
                provider_name=provider,
                api_key=api_key,
                base_url=base_url,
                model=model,
                max_tokens=max_tokens,
                trace_label=trace_label,
            )

            if model_idx > 0:
                if not override_model:
                    save_llm_config(provider, model, base_url, max_tokens)
            return text
        except Exception as err:
            last_error = err
            if model_idx < len(candidates) - 1:
                continue
            raise last_error

    if last_error is not None:
        raise last_error

    raise RuntimeError("LLM 请求失败：未找到可用模型")


def call_llm(
    prompt: str,
    max_tokens_override: int | None = None,
    model_override: str | None = None,
    trace_label: str | None = None,
) -> str:
    return call_llm_messages([
        {"role": "user", "content": prompt},
    ], max_tokens_override=max_tokens_override, model_override=model_override, trace_label=trace_label)