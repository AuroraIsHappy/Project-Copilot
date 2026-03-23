import sys
import re
import importlib
import html
import time
import threading
from datetime import date
from datetime import datetime
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components


@st.cache_resource(show_spinner=False)
def _get_insight_bg_runtime() -> dict:
    # Keep one shared in-memory registry across reruns.
    return {
        "jobs": {},
        "lock": threading.Lock(),
    }


_insight_bg_runtime = _get_insight_bg_runtime()
_insight_bg_jobs: dict = _insight_bg_runtime["jobs"]
_insight_bg_lock = _insight_bg_runtime["lock"]

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from analysis.summary_parser import normalize_summary_folder_path
from controller import generate_plan_by_mode
from controller import load_tasks_from_state
from controller import load_plan_from_state
from controller import render_gantt
from controller import render_graph
from controller import update_tasks_from_summaries
from controller import list_all_projects
from controller import create_new_project
from controller import delete_existing_project
from controller import load_saved_risk_threshold
from controller import load_saved_plan_total_weeks
from controller import save_plan_total_weeks
from controller import load_saved_gantt_theme
from controller import save_gantt_theme
from controller import load_gantt_theme_options
from controller import assistant_chat
from controller import load_insight_state
from controller import sync_project_insight_today_state
from controller import load_insight_settings
from controller import list_saved_insight_cards
from controller import ensure_daily_insight_feed
from controller import generate_project_insight_feed
from controller import mark_insight_notification_seen
from controller import record_insight_feedback
from controller import save_insight_settings
from controller import update_saved_insight_card_annotation
from utils.llm_client import clear_api_key
from utils.llm_client import get_token_usage_tracker
from utils.llm_client import get_provider_specs
from utils.llm_client import load_llm_config
from utils.llm_client import reset_token_usage_tracker
from utils.llm_client import reset_llm_config
from utils.llm_client import save_llm_config
from utils.llm_client import set_api_key
from state.state_manager import load_project_assistant_state
from state.state_manager import load_project_summary_state
from state.state_manager import save_project_summary_state
from state.assistant_memory import project_memory_file
from state.assistant_memory import read_project_memory
from state.assistant_memory import read_system_memory
from state.assistant_memory import system_memory_file
from state.assistant_memory import write_project_memory
from state.assistant_memory import write_system_memory


_INSIGHT_SOURCE_LABELS = {
    "arxiv": "ArXiv 论文",
    "github": "GitHub 项目",
    "blog": "博客 / Hacker News",
    "reddit": "Reddit",
}

_LLM_BASE_URL_CUSTOM_OPTION = "__manual__"


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=IBM+Plex+Sans:wght@400;500;600&display=swap');

        :root {
            /* Base palette */
            --bg: #ffffff;
            --panel: #ffffff;
            --ink: #0d0d0d;
            --muted: #6e6e80;
            --brand: #10a37f;
            --brand-soft: #e9f7f3;
            --accent: #f59e0b;
            --line: #e5e5e5;
            --sidebar-bg: #f9f9f9;
            --assistant-bg: #f6f8fb;
            /* Interactive elements */
            --btn-bg: #ffffff;
            --btn-color: #111827;
            --btn-hover-bg: #f8fafc;
            --line-hover: #d1d5db;
            --input-bg: #ffffff;
            --tab-bg: #ffffff;
            --tab-color: #374151;
            --tab-active-bg: #f3f4f6;
            --tab-active-color: #111827;
            --radio-bg: #ffffff;
            --radio-checked-bg: #eff6ff;
            --radio-checked-border: #bfdbfe;
            /* Chat */
            --chat-msg-border: #d9dee7;
            --chat-user-bg: #dbeafe;
            --chat-user-border: #93c5fd;
            --chat-assistant-bg: #f8fafc;
            --chat-assistant-border: #cbd5e1;
            /* Cards & insight feed */
            --card-bg: #ffffff;
            --card-subtle-bg: #fafafa;
            --insight-feed-bg: #f8fbff;
            --insight-border: #dbe3f0;
            --insight-link: #0f172a;
            --insight-label: #475569;
            --insight-body: #111827;
            --insight-meta: #64748b;
            --insight-annotation-bg: #eff6ff;
            --insight-annotation-border: #93c5fd;
            --insight-annotation-color: #1d4ed8;
            --insight-page-color: #6b7280;
            --insight-ack-bg: #f8fafc;
            --insight-ack-color: #475569;
            --assistant-panel-border: #dde3ec;
        }

        /* ── Dark-mode variable overrides ── */
        @media (prefers-color-scheme: dark) {
            :root {
                --bg: #0e1117;
                --panel: #1e2129;
                --ink: #fafafa;
                --muted: #9ca3af;
                --brand-soft: #0d3027;
                --line: #3d4147;
                --sidebar-bg: #171b22;
                --assistant-bg: #1e2129;
                --btn-bg: #262730;
                --btn-color: #f9fafb;
                --btn-hover-bg: #303340;
                --line-hover: #4b5563;
                --input-bg: #262730;
                --tab-bg: #1e2129;
                --tab-color: #d1d5db;
                --tab-active-bg: #2d3748;
                --tab-active-color: #f9fafb;
                --radio-bg: #1e2129;
                --radio-checked-bg: #1e3a5f;
                --radio-checked-border: #3b82f6;
                --chat-msg-border: #3d4147;
                --chat-user-bg: #1e3a5f;
                --chat-user-border: #3b82f6;
                --chat-assistant-bg: #1e2129;
                --chat-assistant-border: #3d4147;
                --card-bg: #1e2129;
                --card-subtle-bg: #1e2129;
                --insight-feed-bg: #1a1e2b;
                --insight-border: #3d4147;
                --insight-link: #f1f5f9;
                --insight-label: #94a3b8;
                --insight-body: #e2e8f0;
                --insight-meta: #94a3b8;
                --insight-annotation-bg: #1e3a5f;
                --insight-annotation-border: #3b82f6;
                --insight-annotation-color: #93c5fd;
                --insight-page-color: #9ca3af;
                --insight-ack-bg: #1e2129;
                --insight-ack-color: #94a3b8;
                --assistant-panel-border: #3d4147;
            }
        }

        html[data-theme="dark"] {
            --bg: #0e1117;
            --panel: #1e2129;
            --ink: #fafafa;
            --muted: #9ca3af;
            --brand-soft: #0d3027;
            --line: #3d4147;
            --sidebar-bg: #171b22;
            --assistant-bg: #1e2129;
            --btn-bg: #262730;
            --btn-color: #f9fafb;
            --btn-hover-bg: #303340;
            --line-hover: #4b5563;
            --input-bg: #262730;
            --tab-bg: #1e2129;
            --tab-color: #d1d5db;
            --tab-active-bg: #2d3748;
            --tab-active-color: #f9fafb;
            --radio-bg: #1e2129;
            --radio-checked-bg: #1e3a5f;
            --radio-checked-border: #3b82f6;
            --chat-msg-border: #3d4147;
            --chat-user-bg: #1e3a5f;
            --chat-user-border: #3b82f6;
            --chat-assistant-bg: #1e2129;
            --chat-assistant-border: #3d4147;
            --card-bg: #1e2129;
            --card-subtle-bg: #1e2129;
            --insight-feed-bg: #1a1e2b;
            --insight-border: #3d4147;
            --insight-link: #f1f5f9;
            --insight-label: #94a3b8;
            --insight-body: #e2e8f0;
            --insight-meta: #94a3b8;
            --insight-annotation-bg: #1e3a5f;
            --insight-annotation-border: #3b82f6;
            --insight-annotation-color: #93c5fd;
            --insight-page-color: #9ca3af;
            --insight-ack-bg: #1e2129;
            --insight-ack-color: #94a3b8;
            --assistant-panel-border: #3d4147;
        }

        .stApp {
            background: var(--bg);
            color: var(--ink);
        }

        /* Sidebar clean white-grey */
        section[data-testid="stSidebar"] {
            background: var(--sidebar-bg);
            border-right: 1px solid var(--line);
        }

        html, body, [class*="css"] {
            font-family: 'IBM Plex Sans', sans-serif;
        }

        h1, h2, h3 {
            font-family: 'Space Grotesk', sans-serif;
            letter-spacing: -0.02em;
        }

        .hero {
            background: var(--card-bg);
            border: 1px solid var(--line);
            border-radius: 12px;
            padding: 1.2rem 1.4rem;
            box-shadow: none;
            margin-bottom: 1rem;
            animation: rise 0.4s ease-out;
        }

        .metric-card {
            border: 1px solid var(--line);
            background: var(--card-subtle-bg);
            border-radius: 10px;
            padding: 0.8rem 0.9rem;
            margin-bottom: 0.7rem;
        }

        .metric-label {
            color: var(--muted);
            font-size: 0.85rem;
            margin-bottom: 0.2rem;
        }

        .metric-value {
            font-family: 'Space Grotesk', sans-serif;
            font-weight: 700;
            font-size: 1.15rem;
            color: var(--ink);
        }

        .stTextArea textarea {
            border-radius: 8px;
            border: 1px solid var(--line);
            background: var(--input-bg);
            min-height: 210px;
        }

        .stButton button {
            width: 100%;
            border-radius: 8px;
            border: 1px solid var(--line);
            color: var(--btn-color);
            font-weight: 600;
            background: var(--btn-bg);
            box-shadow: none;
            transition: background 0.15s ease, border-color 0.15s ease;
        }

        .stButton button:hover {
            background: var(--btn-hover-bg);
            border-color: var(--line-hover);
            box-shadow: none;
        }

        /* Top assistant launcher: match the hero card height without affecting other buttons */
        [class*="st-key-assistant_open_btn_"] button,
        [class*="st-key-assistant_close_btn_"] button {
            min-height: 112px;
        }

        [class*="st-key-assistant_open_btn_"] button {
            background: var(--btn-bg);
            border-color: var(--line);
        }

        [class*="st-key-assistant_open_btn_"] button:hover {
            background: var(--btn-hover-bg);
            border-color: var(--line-hover);
        }

        [class*="st-key-right_panel_toggle_btn_"] button {
            white-space: nowrap;
        }

        /* Agent mode radio: clean blue-gray selected state */
        div[role="radiogroup"] > label {
            border: 1px solid var(--line);
            border-radius: 999px;
            padding: 0.25rem 0.7rem;
            background: var(--radio-bg);
            transition: background 0.15s ease, border-color 0.15s ease;
        }

        div[role="radiogroup"] > label:has(input:checked) {
            background: var(--radio-checked-bg);
            border-color: var(--radio-checked-border);
        }

        /* Tabs (Timeline / Dependency Graph / Task Table): avoid red active color */
        .stTabs [data-baseweb="tab-list"] {
            gap: 0.35rem;
        }

        .stTabs [data-baseweb="tab"] {
            border: 1px solid var(--line);
            border-radius: 8px;
            background: var(--tab-bg);
            color: var(--tab-color);
            padding: 0.3rem 0.75rem;
            box-shadow: none;
        }

        .stTabs [aria-selected="true"] {
            background: var(--tab-active-bg);
            border-color: var(--line);
            color: var(--tab-active-color);
            box-shadow: none;
        }

        .stTabs [data-baseweb="tab-highlight"] {
            background: transparent !important;
            height: 0 !important;
        }

        .helper-note {
            color: var(--muted);
            font-size: 0.92rem;
            margin-top: 0.35rem;
        }

        .theme-swatch-row {
            display: flex;
            align-items: center;
            gap: 0.45rem;
            margin: 0.2rem 0 0.45rem 0;
            flex-wrap: nowrap;
        }

        .theme-swatch-dot {
            width: 16px;
            height: 16px;
            border-radius: 999px;
            border: 1px solid var(--line);
            box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.42);
            flex: 0 0 auto;
        }

        [class*="st-key-assistant_panel_wrap_"] {
            background: var(--assistant-bg);
            border: 1px solid var(--assistant-panel-border);
            border-radius: 12px;
            padding: 0.9rem;
        }

        /* Chat contrast: make user/assistant cards easier to distinguish */
        [data-testid="stChatMessage"] {
            border-radius: 10px;
            padding: 0.45rem 0.55rem;
            margin-bottom: 0.45rem;
            border: 1px solid var(--chat-msg-border);
            background: var(--card-bg);
        }

        [data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-user"]) {
            background: var(--chat-user-bg) !important;
            border-color: var(--chat-user-border) !important;
        }

        [data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) {
            background: var(--chat-assistant-bg) !important;
            border-color: var(--chat-assistant-border) !important;
        }

        /* Fallback selectors for Streamlit DOM variants */
        [data-testid="stChatMessage"]:has([aria-label*="user" i]),
        [data-testid="stChatMessage"]:has(svg[aria-label*="user" i]) {
            background: var(--chat-user-bg) !important;
            border-color: var(--chat-user-border) !important;
        }

        [data-testid="stChatMessage"]:has([aria-label*="assistant" i]),
        [data-testid="stChatMessage"]:has(svg[aria-label*="assistant" i]) {
            background: var(--chat-assistant-bg) !important;
            border-color: var(--chat-assistant-border) !important;
        }

        /* Streamlit chat input draws border on the inner wrapper, not on this root. */
        div[data-testid="stChatInput"] {
            border: none !important;
            border-radius: 0 !important;
            background: transparent !important;
            margin: 0 !important;
            padding: 0 !important;
            box-shadow: none !important;
            outline: none !important;
        }

        div[data-testid="stChatInput"] > div {
            border: 1.5px solid #60a5fa !important;
            border-radius: 10px !important;
            background: var(--input-bg) !important;
            box-shadow: 0 0 0 2px rgba(96, 165, 250, 0.12) !important;
            outline: none !important;
        }

        div[data-testid="stChatInput"] > div:focus-within {
            border: 1.5px solid #60a5fa !important;
            box-shadow: 0 0 0 2px rgba(96, 165, 250, 0.12) !important;
            outline: none !important;
        }

        div[data-testid="stChatInput"] textarea {
            border: none !important;
            box-shadow: none !important;
            background: transparent !important;
            outline: none !important;
        }

        div[data-testid="stChatInput"] [data-baseweb="textarea"],
        div[data-testid="stChatInput"] [data-baseweb="base-input"],
        div[data-testid="stChatInput"] [data-baseweb="textarea"] > div,
        div[data-testid="stChatInput"] [data-baseweb="base-input"] > div {
            border: none !important;
            box-shadow: none !important;
            background: transparent !important;
            outline: none !important;
        }

        div[data-testid="stChatInput"] [data-baseweb="textarea"]:focus-within,
        div[data-testid="stChatInput"] [data-baseweb="base-input"]:focus-within,
        div[data-testid="stChatInput"] [data-baseweb="textarea"] > div:focus-within,
        div[data-testid="stChatInput"] [data-baseweb="base-input"] > div:focus-within,
        div[data-testid="stChatInput"] textarea:focus,
        div[data-testid="stChatInput"] textarea:focus-visible {
            border: none !important;
            outline: none !important;
            box-shadow: none !important;
        }

        .insight-feed-wrap {
            border: 1px solid var(--insight-border);
            border-radius: 12px;
            background: var(--insight-feed-bg);
            padding: 0.85rem 0.95rem;
            margin: 0.35rem 0 0.8rem 0;
        }

        .insight-card {
            border: 1px solid var(--insight-border);
            border-radius: 12px;
            background: var(--card-bg);
            padding: 0.8rem 0.9rem;
            margin-bottom: 0.65rem;
        }

        .insight-card-title {
            margin: 0;
            font-weight: 700;
            font-size: 1rem;
            line-height: 1.35;
        }

        .insight-card-title a {
            color: var(--insight-link);
            text-decoration: none;
        }

        .insight-card-title a:hover {
            text-decoration: underline;
        }

        .insight-label {
            color: var(--insight-label);
            font-size: 0.82rem;
            margin: 0.35rem 0 0.1rem 0;
            font-weight: 600;
        }

        .insight-body {
            color: var(--insight-body);
            margin: 0;
            font-size: 0.95rem;
            line-height: 1.45;
        }

        .saved-insight-directory-item {
            border: 1px solid var(--insight-border);
            border-radius: 12px;
            background: var(--card-bg);
            padding: 0.7rem 0.8rem;
            margin-bottom: 0.65rem;
        }

        .saved-insight-directory-line {
            margin: 0;
            color: var(--insight-link);
            font-size: 0.92rem;
            line-height: 1.45;
            font-weight: 600;
        }

        .saved-insight-directory-meta {
            margin: 0.22rem 0 0 0;
            color: var(--insight-meta);
            font-size: 0.8rem;
            line-height: 1.35;
        }

        .saved-insight-annotation {
            margin-top: 0.5rem;
            padding: 0.52rem 0.68rem;
            border-radius: 10px;
            border: 1px solid var(--insight-annotation-border);
            background: var(--insight-annotation-bg);
            color: var(--insight-annotation-color);
            font-size: 0.88rem;
            line-height: 1.45;
            font-weight: 600;
        }

        .insight-modal-backdrop {
            position: fixed;
            inset: 0;
            z-index: 10010;
            background: rgba(15, 23, 42, 0.22);
            backdrop-filter: blur(1.5px);
        }

        [class*="st-key-insight_modal_"] {
            position: fixed;
            left: 50%;
            top: 50%;
            transform: translate(-50%, -50%);
            width: min(860px, 92vw);
            max-height: 86vh;
            overflow-y: auto;
            z-index: 10011;
            background: var(--card-bg);
            border: 1px solid var(--line);
            border-radius: 16px;
            box-shadow: 0 24px 58px rgba(15, 23, 42, 0.34);
            padding: 0.85rem 0.95rem;
        }

        .insight-modal-card {
            background: var(--card-bg);
            border: none;
            border-radius: 12px;
            padding: 1.05rem 1.1rem;
            margin-top: 0.25rem;
            min-height: 28rem;
            box-shadow: none;
        }

        .insight-page-indicator {
            color: var(--insight-page-color);
            font-weight: 600;
            text-align: center;
            padding-top: 0.7rem;
            padding-bottom: 0.15rem;
            font-size: 0.88rem;
        }

        .insight-feedback-ack {
            border: 1px solid var(--line);
            background: var(--insight-ack-bg);
            color: var(--insight-ack-color);
            border-radius: 0;
            padding: 0.55rem 0.75rem;
            margin: 0.35rem 0 0.5rem 0;
            font-size: 0.9rem;
            font-weight: 500;
            line-height: 1.35;
            animation: insightAckFadeOut 2.6s ease forwards;
            transform-origin: top;
            overflow: hidden;
        }

        @keyframes insightAckFadeOut {
            0% {
                opacity: 1;
                transform: translateY(0);
                max-height: 90px;
                margin-top: 0.35rem;
                margin-bottom: 0.5rem;
                padding-top: 0.55rem;
                padding-bottom: 0.55rem;
            }
            78% {
                opacity: 1;
                transform: translateY(0);
                max-height: 90px;
            }
            100% {
                opacity: 0;
                transform: translateY(-4px);
                max-height: 0;
                margin-top: 0;
                margin-bottom: 0;
                padding-top: 0;
                padding-bottom: 0;
                border-width: 0;
            }
        }

        [class*="st-key-insight_side_prev_"],
        [class*="st-key-insight_side_next_"] {
            min-height: 28rem;
            display: flex;
            align-items: center;
            justify-content: center;
        }

        [class*="st-key-insight_side_prev_"] > div,
        [class*="st-key-insight_side_next_"] > div {
            width: 100%;
        }

        [class*="st-key-insight_hide_feed_"] button {
            border-radius: 999px;
            border: 1px solid var(--line);
            background: var(--btn-bg);
            color: var(--tab-color);
            font-size: 1rem;
            font-weight: 700;
            min-height: 2.05rem;
        }

        [class*="st-key-insight_prev_"] button,
        [class*="st-key-insight_next_"] button {
            min-height: 3rem;
            border-radius: 999px;
            border: 1px solid var(--line);
            background: var(--btn-bg);
            color: var(--tab-color);
            font-size: 1.2rem;
            font-weight: 700;
        }

        [class*="st-key-insight_prev_"] button:disabled,
        [class*="st-key-insight_next_"] button:disabled {
            opacity: 0.45;
        }

        [class*="st-key-dlg_view_"] button,
        [class*="st-key-dlg_later_"] button {
            border: 1px solid var(--line) !important;
            background: var(--btn-bg) !important;
            color: var(--btn-color) !important;
            box-shadow: none !important;
        }

        [class*="st-key-dlg_view_"] button:focus,
        [class*="st-key-dlg_view_"] button:focus-visible,
        [class*="st-key-dlg_later_"] button:focus,
        [class*="st-key-dlg_later_"] button:focus-visible {
            outline: none !important;
            box-shadow: none !important;
        }

        [class*="st-key-insight_fab_btn_"] {
            position: fixed;
            right: 1.2rem;
            bottom: 1.15rem;
            width: 250px;
            z-index: 9999;
        }

        [class*="st-key-insight_fab_btn_"] button {
            border-radius: 999px;
            border: 1px solid #0f766e;
            background: #0f766e;
            color: #ffffff;
            box-shadow: 0 8px 24px rgba(15, 118, 110, 0.28);
            font-weight: 700;
        }

        [class*="st-key-insight_fab_btn_"] button:hover {
            border-color: #115e59;
            background: #115e59;
        }

        @keyframes rise {
            from { opacity: 0; transform: translateY(6px); }
            to { opacity: 1; transform: translateY(0); }
        }

        @media (max-width: 900px) {
            .hero {
                padding: 1rem 0.9rem;
                border-radius: 10px;
            }

            .metric-card {
                padding: 0.65rem 0.7rem;
                border-radius: 8px;
            }

            .metric-value {
                font-size: 1rem;
            }

            [class*="st-key-insight_fab_btn_"] {
                width: calc(100vw - 2rem);
                right: 1rem;
                bottom: 1rem;
            }

            [class*="st-key-insight_modal_"] {
                width: calc(100vw - 1.2rem);
                left: 0.6rem;
                top: 7vh;
                transform: none;
                max-height: 84vh;
            }

            .insight-modal-card {
                min-height: 22rem;
                padding: 0.95rem 0.9rem;
            }

            [class*="st-key-insight_side_prev_"],
            [class*="st-key-insight_side_next_"] {
                min-height: 22rem;
            }

            [class*="st-key-insight_prev_"] button,
            [class*="st-key-insight_next_"] button {
                min-height: 2.45rem;
                font-size: 1.05rem;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_header(project_name: str = "") -> None:
    subtitle = f"当前项目：<strong>{project_name}</strong>" if project_name else "把 OKR 文本快速转成可执行任务时间线，辅助你做节奏管理与风险识别。"
    st.markdown(
        f"""
        <div class="hero">
                                        <h1 style="margin:0;">Project Copilot</h1>
          <p style="margin:0.5rem 0 0 0;color:var(--muted);">
            {subtitle}
          </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _assistant_open_key(project_id: str) -> str:
    return f"assistant_open_{project_id}"


def _right_panel_open_key(project_id: str) -> str:
    return f"right_panel_open_{project_id}"


def _render_right_panel_toggle(project_id: str) -> None:
    open_key = _right_panel_open_key(project_id)
    st.session_state.setdefault(open_key, True)
    is_open = bool(st.session_state.get(open_key, True))

    label = "⇥" if is_open else "⇤"
    btn_type = "secondary" if is_open else "primary"
    if st.button(
        label,
        key=f"right_panel_toggle_btn_{project_id}",
        use_container_width=True,
        type=btn_type,
    ):
        st.session_state[open_key] = not is_open
        st.rerun()


def _render_assistant_launcher(project_id: str, *, show_hint: bool = True, compact: bool = False) -> None:
    open_key = _assistant_open_key(project_id)
    st.session_state.setdefault(open_key, False)

    is_open = bool(st.session_state.get(open_key, False))
    if show_hint:
        hint = "Assistant 已展开：右侧栏为聊天窗口。" if is_open else "Assistant 已折叠：点击按钮可展开右侧聊天窗口。"
        st.info(hint)

    if is_open:
        if st.button(
            "关闭聊天窗口",
            key=f"assistant_close_btn_{project_id}",
            use_container_width=not compact,
            type="secondary",
        ):
            st.session_state[open_key] = False
            st.rerun()
    else:
        if st.button(
            "💬 打开聊天窗口",
            key=f"assistant_open_btn_{project_id}",
            use_container_width=not compact,
            type="primary",
        ):
            st.session_state[open_key] = True
            st.rerun()


def render_metrics(tasks: list[dict]) -> None:
    total = len(tasks)
    done = sum(1 for t in tasks if t.get("status") == "Done")
    in_progress = sum(1 for t in tasks if t.get("status") == "In Progress")
    risk = sum(1 for t in tasks if t.get("status") == "At Risk")

    cards = [
        ("任务总数", str(total)),
        ("进行中", str(in_progress)),
        ("已完成", str(done)),
        ("风险项", str(risk)),
    ]

    for label, value in cards:
        st.markdown(
            f"""
            <div class="metric-card">
              <div class="metric-label">{label}</div>
              <div class="metric-value">{value}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _extract_objective(tasks: list[dict]) -> str:
    for task in tasks:
        objective = str(task.get("objective", "")).strip()
        if objective:
            return objective
    return ""


def _coerce_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value:
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return date.max
    return date.max


def _format_date(value) -> str:
    parsed = _coerce_date(value)
    return "" if parsed == date.max else parsed.isoformat()


def _parse_kr_index(kr_name: str) -> int:
    match = re.match(r"\s*kr\s*(\d+)", kr_name, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return 10**6


def _task_sort_key(task: dict) -> tuple:
    kr_name = str(task.get("kr", "KR1: Execution")).strip() or "KR1: Execution"
    kr_index = task.get("kr_index")
    try:
        kr_index = int(kr_index) if kr_index is not None else _parse_kr_index(kr_name)
    except (TypeError, ValueError):
        kr_index = _parse_kr_index(kr_name)

    subtask_index = task.get("subtask_index")
    try:
        subtask_index = int(subtask_index) if subtask_index is not None else 10**6
    except (TypeError, ValueError):
        subtask_index = 10**6

    return (
        _coerce_date(task.get("start", "")),
        _coerce_date(task.get("end", "")),
        kr_index,
        subtask_index,
        str(task.get("task", "")),
    )


def _group_sort_key(kr_name: str, kr_tasks: list[dict]) -> tuple:
    explicit_indices = []
    for task in kr_tasks:
        kr_index = task.get("kr_index")
        try:
            if kr_index is not None:
                explicit_indices.append(int(kr_index))
        except (TypeError, ValueError):
            continue

    resolved_index = min(explicit_indices) if explicit_indices else _parse_kr_index(kr_name)
    earliest_start = min((_coerce_date(task.get("start", "")) for task in kr_tasks), default=date.max)
    return (resolved_index, earliest_start, kr_name)


def _group_tasks_by_kr(tasks: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for task in sorted(tasks, key=_task_sort_key):
        kr = str(task.get("kr", "KR1: Execution")).strip() or "KR1: Execution"
        grouped.setdefault(kr, []).append(task)

    sorted_groups = sorted(grouped.items(), key=lambda item: _group_sort_key(item[0], item[1]))
    return {kr: kr_tasks for kr, kr_tasks in sorted_groups}


def _resolve_kr_index(kr_name: str, kr_tasks: list[dict]) -> int | None:
    for task in kr_tasks:
        kr_index = task.get("kr_index")
        try:
            if kr_index is not None:
                return int(kr_index)
        except (TypeError, ValueError):
            continue

    parsed_index = _parse_kr_index(kr_name)
    return None if parsed_index == 10**6 else parsed_index


def _summarize_kr_row(kr_name: str, kr_tasks: list[dict]) -> dict:
    start_dates = [_coerce_date(task.get("start", "")) for task in kr_tasks]
    end_dates = [_coerce_date(task.get("end", "")) for task in kr_tasks]
    valid_starts = [value for value in start_dates if value != date.max]
    valid_ends = [value for value in end_dates if value != date.max]
    progress_values = []
    for task in kr_tasks:
        try:
            progress_values.append(int(task.get("progress", 0)))
        except (TypeError, ValueError):
            progress_values.append(0)

    return {
        "task": kr_name,
        "start": min(valid_starts).isoformat() if valid_starts else "",
        "end": max(valid_ends).isoformat() if valid_ends else "",
        "duration": sum(int(task.get("duration", 1)) for task in kr_tasks),
        "owner": "KR",
        "status": "KR Summary",
        "progress": round(sum(progress_values) / len(progress_values)) if progress_values else 0,
        "kr": kr_name,
        "objective": str(kr_tasks[0].get("objective", "")) if kr_tasks else "",
        "kr_index": _resolve_kr_index(kr_name, kr_tasks),
    }


def _build_subtask_label(task: dict, kr_name: str, sequence_number: int) -> str:
    kr_index = task.get("kr_index")
    try:
        kr_index = int(kr_index) if kr_index is not None else _resolve_kr_index(kr_name, [task])
    except (TypeError, ValueError):
        kr_index = _resolve_kr_index(kr_name, [task])

    if kr_index is None:
        return f"{sequence_number}. {task.get('task', '')}"
    return f"{kr_index}.{sequence_number} {task.get('task', '')}"


def _build_timeline_rows(tasks: list[dict]) -> list[dict]:
    grouped = _group_tasks_by_kr(tasks)
    rows: list[dict] = []

    for kr, kr_tasks in grouped.items():
        sorted_tasks = sorted(kr_tasks, key=_task_sort_key)
        rows.append(_summarize_kr_row(kr, sorted_tasks))

        for sequence_number, task in enumerate(sorted_tasks, start=1):
            task_row = dict(task)
            task_row["task"] = _build_subtask_label(task, kr, sequence_number)
            rows.append(task_row)

    return rows


def _build_task_display_name_map(tasks: list[dict]) -> dict[str, str]:
    grouped = _group_tasks_by_kr(tasks)
    display_name_map: dict[str, str] = {}

    for kr, kr_tasks in grouped.items():
        sorted_tasks = sorted(kr_tasks, key=_task_sort_key)
        for sequence_number, task in enumerate(sorted_tasks, start=1):
            task_id = str(task.get("task_id") or "").strip()
            if not task_id:
                continue
            display_name_map[task_id] = _build_subtask_label(task, kr, sequence_number)

    return display_name_map


def _build_table_rows(tasks: list[dict]) -> list[dict]:
    grouped = _group_tasks_by_kr(tasks)
    rows: list[dict] = []

    for kr, kr_tasks in grouped.items():
        sorted_tasks = sorted(kr_tasks, key=_task_sort_key)
        summary_row = _summarize_kr_row(kr, sorted_tasks)
        rows.append(
            {
                "任务": summary_row["task"],
                "开始": _format_date(summary_row["start"]),
                "结束": _format_date(summary_row["end"]),
                "负责人": summary_row["owner"],
                "状态": summary_row["status"],
                "进度": f"{summary_row['progress']}%",
            }
        )

        for sequence_number, task in enumerate(sorted_tasks, start=1):
            rows.append(
                {
                    "任务": _build_subtask_label(task, kr, sequence_number),
                    "开始": _format_date(task.get("start", "")),
                    "结束": _format_date(task.get("end", "")),
                    "负责人": task.get("owner", "Unassigned"),
                    "状态": task.get("status", "Planned"),
                    "进度": f"{int(task.get('progress', 0))}%",
                }
            )

    return rows


def _safe_file_stem(raw: str, fallback: str = "okr_project") -> str:
    text = str(raw or "").strip().lower()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_\-]", "", text)
    text = text.strip("._-")
    return text or fallback


def _wrap_export_html(fragment: str, title: str) -> str:
    safe_title = str(title or "OKR Export")
    return (
        "<!doctype html>\n"
        "<html lang='en'>\n"
        "<head>\n"
        "  <meta charset='utf-8'/>\n"
        "  <meta name='viewport' content='width=device-width, initial-scale=1'/>\n"
        f"  <title>{safe_title}</title>\n"
        "</head>\n"
        "<body style='margin:0;padding:12px;background:#ffffff;'>\n"
        f"{fragment}\n"
        "</body>\n"
        "</html>\n"
    )


def _dependency_svg_to_png_bytes(svg_markup: str, width: int, height: int) -> bytes | None:
    if not svg_markup:
        return None
    try:
        cairosvg = importlib.import_module("cairosvg")
        return cairosvg.svg2png(
            bytestring=svg_markup.encode("utf-8"),
            output_width=max(1, int(width)),
            output_height=max(1, int(height)),
        )
    except Exception:
        return None


def _has_cairosvg() -> bool:
    try:
        importlib.import_module("cairosvg")
        return True
    except Exception:
        return False


def render_kr_toolbar(tasks: list[dict]) -> dict[str, bool]:
    grouped = _group_tasks_by_kr(tasks)
    kr_names = list(grouped.keys())

    if "kr_expand_state" not in st.session_state:
        st.session_state.kr_expand_state = {name: True for name in kr_names}
    else:
        st.session_state.kr_expand_state = {
            name: st.session_state.kr_expand_state.get(name, True) for name in kr_names
        }

    for name in kr_names:
        st.session_state.kr_expand_state.setdefault(name, True)

    col_expand, col_collapse = st.columns([1, 1], gap="small")
    with col_expand:
        if st.button("全部展开", use_container_width=True):
            st.session_state.kr_expand_state = {name: True for name in kr_names}
            st.rerun()
    with col_collapse:
        if st.button("全部折叠", use_container_width=True):
            st.session_state.kr_expand_state = {name: False for name in kr_names}
            st.rerun()

    st.caption("点击 KR 标题可折叠/展开子任务")
    for idx, kr in enumerate(kr_names, start=1):
        is_open = bool(st.session_state.kr_expand_state.get(kr, True))
        arrow = "▼" if is_open else "▶"
        label = f"{arrow} {kr} ({len(grouped[kr])})"
        if st.button(label, key=f"kr-toggle-{idx}", use_container_width=True):
            st.session_state.kr_expand_state[kr] = not is_open
            st.rerun()

    return dict(st.session_state.kr_expand_state)


def _llm_base_url_preset_options(current_spec: dict | None, current_value: str = "") -> list[str]:
    options: list[str] = []

    if isinstance(current_spec, dict):
        raw_options = current_spec.get("base_url_options", [])
        if isinstance(raw_options, list):
            options.extend(raw_options)

        default_base_url = str(current_spec.get("default_base_url") or "").strip()
        if default_base_url:
            options.insert(0, default_base_url)

    configured_value = str(current_value or "").strip()
    if configured_value:
        options.append(configured_value)

    normalized: list[str] = []
    seen: set[str] = set()
    for raw_option in options:
        option = str(raw_option or "").strip()
        if not option or option in seen:
            continue
        seen.add(option)
        normalized.append(option)

    return normalized


def _sync_llm_base_url_state(provider_name: str, current_value: str, preset_options: list[str]) -> None:
    normalized_provider = str(provider_name or "").strip()
    normalized_value = str(current_value or "").strip()
    signature = (normalized_provider, normalized_value, tuple(preset_options))

    if st.session_state.get("_llm_base_url_signature") == signature:
        return

    st.session_state["_llm_base_url_signature"] = signature
    st.session_state["llm_base_url_value"] = normalized_value
    st.session_state["llm_base_url_preset"] = (
        normalized_value if normalized_value in preset_options else _LLM_BASE_URL_CUSTOM_OPTION
    )


def _apply_llm_base_url_preset() -> None:
    preset = str(st.session_state.get("llm_base_url_preset") or "").strip()
    if preset and preset != _LLM_BASE_URL_CUSTOM_OPTION:
        st.session_state["llm_base_url_value"] = preset


def _apply_llm_base_url_value(preset_options: list[str]) -> None:
    current_value = str(st.session_state.get("llm_base_url_value") or "").strip()
    st.session_state["llm_base_url_preset"] = (
        current_value if current_value in preset_options else _LLM_BASE_URL_CUSTOM_OPTION
    )


def render_llm_settings() -> None:
    with st.sidebar.expander("LLM 配置", expanded=False):
        cfg = load_llm_config()
        provider_specs = get_provider_specs()
        provider_names = [""] + list(provider_specs.keys())
        provider_index = provider_names.index(cfg["provider"]) if cfg["provider"] in provider_names else 0

        provider = st.selectbox(
            "Provider",
            options=provider_names,
            index=provider_index,
            format_func=lambda x: "请选择 Provider" if not x else provider_specs[x]["label"],
            key="llm_provider_select",
        )

        if provider != cfg["provider"]:
            if provider:
                switched_spec = provider_specs[provider]
                save_llm_config(
                    provider,
                    switched_spec["default_model"],
                    switched_spec["default_base_url"],
                    switched_spec["default_max_tokens"],
                    switched_spec["default_progress_max_workers"],
                    switched_spec["default_progress_llm_max_output_tokens"],
                    switched_spec["default_progress_model"],
                    switched_spec["default_assistant_model"],
                )
                st.success("已切换 Provider，请确认 Base URL 与模型配置。")
            else:
                reset_llm_config()
                st.success("已重置为未配置状态。")
            st.rerun()

        selected_provider = provider or cfg["provider"]
        current_spec = provider_specs.get(selected_provider)
        if current_spec:
            base_url_presets = _llm_base_url_preset_options(current_spec, cfg.get("base_url", ""))
            _sync_llm_base_url_state(selected_provider, cfg.get("base_url", ""), base_url_presets)

            if base_url_presets:
                preset_options = [_LLM_BASE_URL_CUSTOM_OPTION] + base_url_presets
                if st.session_state.get("llm_base_url_preset") not in preset_options:
                    st.session_state["llm_base_url_preset"] = _LLM_BASE_URL_CUSTOM_OPTION

                st.selectbox(
                    "Base URL 预设",
                    options=preset_options,
                    format_func=lambda value: "手动输入 / 自定义 URL" if value == _LLM_BASE_URL_CUSTOM_OPTION else value,
                    key="llm_base_url_preset",
                    on_change=_apply_llm_base_url_preset,
                )
            else:
                st.caption("该 Provider 没有通用 Base URL 预设，请手动填写。")

            st.text_input(
                "Base URL",
                key="llm_base_url_value",
                placeholder="https://...",
                help="可直接修改；如果先选择上面的预设，输入框会自动带入该值。",
                on_change=_apply_llm_base_url_value,
                args=(base_url_presets,),
            )
        else:
            st.caption("Base URL 预设：选择 Provider 后显示")
            st.info("当前还没有选择 Provider。首次使用请先选择 Provider，再填写模型与 API Key。")

        with st.form("llm_config_form"):
            task_generation_model = st.text_input(
                "任务生成模型（Generate Task）",
                value=cfg["model"],
                help="仅用于 OKR 任务生成流程（Generate Plan），不用于 Assistant 聊天或总结进度更新。",
                disabled=not current_spec,
            )
            assistant_model = st.text_input(
                "Assistant 模型（聊天助手）",
                value=cfg.get("assistant_model", cfg["model"]),
                help="用于右侧 Assistant 聊天能力（含意图识别、重规划建议、记忆提取）的专用模型。",
                disabled=not current_spec,
            )
            progress_model = st.text_input(
                "Progress 模型（读取总结/更新任务）",
                value=cfg.get("progress_model", cfg["model"]),
                help="用于读取工作总结并更新任务状态的专用模型，可与主模型不同。",
                disabled=not current_spec,
            )
            max_tokens = st.number_input(
                "Max Tokens",
                min_value=100,
                max_value=7000,
                value=int(cfg["max_tokens"]),
                step=100,
                help="限制单次模型输出上限，避免异常消耗过多 token。",
                disabled=not current_spec,
            )
            progress_max_workers = st.number_input(
                "Progress 并发数",
                min_value=1,
                max_value=16,
                value=int(cfg.get("progress_max_workers", 3)),
                step=1,
                help="用于工作总结进度估计流程（progress_estimator）的并发请求数。",
                disabled=not current_spec,
            )
            progress_llm_max_output_tokens = st.number_input(
                "Progress 输出上限 Tokens",
                min_value=100,
                max_value=8000,
                value=int(cfg.get("progress_llm_max_output_tokens", 5000)),
                step=100,
                help="用于工作总结进度估计流程（progress_estimator）的单次输出 token 上限。",
                disabled=not current_spec,
            )
            api_key = st.text_input(
                "API Key",
                type="password",
                placeholder="仅首次输入，后续自动读取",
                help="API Key 会按 provider 分开保存在系统凭据库，不写入 config.json。",
                disabled=not current_spec,
            )
            submitted = st.form_submit_button("保存配置", disabled=not current_spec)

        if submitted:
            save_error = ""
            try:
                selected_base_url = str(st.session_state.get("llm_base_url_value") or "").strip()
                save_ok = save_llm_config(
                    selected_provider,
                    task_generation_model,
                    selected_base_url,
                    int(max_tokens),
                    int(progress_max_workers),
                    int(progress_llm_max_output_tokens),
                    progress_model,
                    assistant_model,
                )
            except ValueError as exc:
                save_error = str(exc)
                save_ok = False
            if not save_ok:
                st.error(save_error or "配置保存失败，请检查文件权限。")
            else:
                if api_key.strip():
                    try:
                        set_api_key(api_key, selected_provider)
                    except RuntimeError:
                        st.error("未安装 keyring，无法安全保存 API Key。请安装: pip install keyring")
                    except ValueError as exc:
                        st.error(str(exc))
                    else:
                        st.success("配置已保存。")
                        st.rerun()
                else:
                    st.success("配置已保存。")
                    st.rerun()

        st.caption(f"当前 Provider: {cfg['provider_label']}")
        if cfg.get("is_configured"):
            state_label = "已保存" if cfg["has_api_key"] else "未保存"
            st.caption(f"API Key 状态: {state_label}")
            st.caption(f"Base URL: {cfg['base_url'] or '未设置'}")
            st.caption(f"任务生成模型: {cfg['model']}")
            st.caption(f"Max Tokens: {cfg['max_tokens']}")
            st.caption(f"Assistant 模型: {cfg.get('assistant_model', cfg['model'])}")
            st.caption(f"Progress 模型: {cfg.get('progress_model', cfg['model'])}")
            st.caption(f"Progress 并发数: {cfg.get('progress_max_workers', 3)}")
            st.caption(f"Progress 输出上限 Tokens: {cfg.get('progress_llm_max_output_tokens', 5000)}")
            if cfg["use_env_key"]:
                if cfg["env_key"]:
                    st.caption(f"当前使用环境变量 {cfg['env_key']} 或 OKR_OPENAI_API_KEY。")
                else:
                    st.caption("当前使用环境变量 OKR_OPENAI_API_KEY。")

            if cfg["api_key_optional"]:
                st.caption("该 Provider 可不填 API Key（本地服务常见）。")
        else:
            st.caption("API Key 状态: 未配置")

        if cfg["has_api_key"] and cfg.get("is_configured") and st.button("清除已保存 API Key", key="clear_api_key_btn"):
            clear_api_key(cfg["provider"])
            st.success("已清除本地保存的 API Key。")
            st.rerun()


def render_project_nav() -> str | None:
    """Render project list in sidebar. Returns the active project_id."""
    st.sidebar.markdown("## 我的项目")

    projects = list_all_projects()

    # ── New project ──────────────────────────────────────────────────────────
    with st.sidebar.expander("新建项目", expanded=False):
        new_name = st.text_input("项目名称", key="new_project_name_input", placeholder="输入项目名称…")
        if st.button("创建", key="create_project_btn", use_container_width=True):
            name = new_name.strip()
            if not name:
                st.warning("请输入项目名称。")
            else:
                record = create_new_project(name)
                st.session_state.active_project_id = record["id"]
                st.session_state.kr_expand_state = {}
                st.rerun()

    if not projects:
        st.sidebar.info("还没有项目，先新建一个吧。")
        return None

    # ── Ensure a valid active project ────────────────────────────────────────
    project_ids = [p["id"] for p in projects]
    if st.session_state.get("active_project_id") not in project_ids:
        st.session_state.active_project_id = project_ids[0]

    active_id = st.session_state.active_project_id

    # ── Project list ─────────────────────────────────────────────────────────
    st.sidebar.markdown("---")
    for project in projects:
        pid = project["id"]
        pname = project["name"]
        is_active = pid == active_id

        col_btn, col_del = st.sidebar.columns([4, 1], gap="small")
        with col_btn:
            label = f"**{pname}**" if is_active else pname
            if st.button(label, key=f"proj_select_{pid}", use_container_width=True):
                if pid != active_id:
                    st.session_state.active_project_id = pid
                    st.session_state.kr_expand_state = {}
                    st.rerun()
        with col_del:
            if st.button("🗑", key=f"proj_del_{pid}", help=f"删除「{pname}」"):
                st.session_state[f"confirm_delete_{pid}"] = True
                st.rerun()

        # Delete confirmation
        if st.session_state.get(f"confirm_delete_{pid}"):
            st.sidebar.warning(f"确认删除「{pname}」？此操作不可恢复。")
            c1, c2 = st.sidebar.columns(2)
            with c1:
                if st.button("确认删除", key=f"proj_del_confirm_{pid}", use_container_width=True):
                    delete_existing_project(pid)
                    st.session_state.pop(f"confirm_delete_{pid}", None)
                    remaining = [p for p in projects if p["id"] != pid]
                    st.session_state.active_project_id = remaining[0]["id"] if remaining else None
                    st.session_state.kr_expand_state = {}
                    st.rerun()
            with c2:
                if st.button("取消", key=f"proj_del_cancel_{pid}", use_container_width=True):
                    st.session_state.pop(f"confirm_delete_{pid}", None)
                    st.rerun()

    # ── Personalized Memory --------------------------------------------------
    st.sidebar.markdown("---")
    with st.sidebar.expander("记忆管理", expanded=False):
        memory_targets = ["系统级 MEMORY"] + [f"项目 MEMORY: {p['name']}" for p in projects]
        default_idx = 0
        for idx, project in enumerate(projects, start=1):
            if project["id"] == active_id:
                default_idx = idx
                break

        target_label = st.selectbox(
            "编辑目标",
            options=memory_targets,
            index=default_idx,
            key="memory_editor_target",
        )

        if target_label == "系统级 MEMORY":
            target_path = system_memory_file()
            current_text = read_system_memory()
            text_key = "memory_editor_text_system"
            if st.session_state.get("memory_editor_target_prev") != target_label:
                st.session_state[text_key] = current_text
            edited = st.text_area("内容", key=text_key, height=240)
            st.caption(f"文件路径：{target_path}")
            if st.button("保存系统级 MEMORY", key="save_system_memory_btn", use_container_width=True):
                write_system_memory(edited)
                st.success("系统级 MEMORY 已保存。")
                st.rerun()
        else:
            selected_name = target_label.replace("项目 MEMORY: ", "", 1)
            selected_project = next((p for p in projects if p["name"] == selected_name), None)
            if selected_project:
                pid = selected_project["id"]
                target_path = project_memory_file(pid)
                current_text = read_project_memory(pid)
                text_key = f"memory_editor_text_project_{pid}"
                if st.session_state.get("memory_editor_target_prev") != target_label:
                    st.session_state[text_key] = current_text
                edited = st.text_area("内容", key=text_key, height=240)
                st.caption(f"文件路径：{target_path}")
                if st.button("保存项目 MEMORY", key=f"save_project_memory_btn_{pid}", use_container_width=True):
                    write_project_memory(pid, edited)
                    st.success("项目 MEMORY 已保存。")
                    st.rerun()

        st.session_state["memory_editor_target_prev"] = target_label

    return active_id


def _render_theme_swatches(colors: list[str]) -> None:
    dots = "".join(
        f"<span class='theme-swatch-dot' style='background:{color};'></span>"
        for color in colors
    )
    st.markdown(f"<div class='theme-swatch-row'>{dots}</div>", unsafe_allow_html=True)


def render_personalization_nav(project_id: str) -> str:
    theme_options = load_gantt_theme_options()
    ordered_theme_keys = [key for key in ["default", "minimal", "retro"] if key in theme_options]
    if not ordered_theme_keys:
        ordered_theme_keys = list(theme_options.keys())

    saved_theme, _ = load_saved_gantt_theme(project_id)
    session_key = f"gantt_theme_{project_id}"
    if session_key not in st.session_state:
        st.session_state[session_key] = saved_theme

    current_theme = str(st.session_state.get(session_key, saved_theme) or "default").strip().lower()
    if current_theme not in theme_options:
        current_theme = "default"
        st.session_state[session_key] = current_theme

    with st.sidebar.expander("个性化", expanded=False):
        current_label = str(theme_options.get(current_theme, {}).get("label", "默认"))
        st.caption(f"当前甘特图主题：{current_label}")

        tab_labels = [str(theme_options[key].get("label", key)) for key in ordered_theme_keys]
        theme_tabs = st.tabs(tab_labels)

        for idx, theme_key in enumerate(ordered_theme_keys):
            option = theme_options.get(theme_key, {})
            with theme_tabs[idx]:
                description = str(option.get("description", "")).strip()
                if description:
                    st.caption(description)

                swatches = [str(color) for color in option.get("swatches", []) if str(color).strip()]
                if swatches:
                    _render_theme_swatches(swatches)

                is_current = theme_key == current_theme
                button_label = "当前使用中" if is_current else f"应用{option.get('label', theme_key)}"
                if st.button(
                    button_label,
                    key=f"apply_gantt_theme_{project_id}_{theme_key}",
                    use_container_width=True,
                    disabled=is_current,
                ):
                    normalized_theme = save_gantt_theme(project_id, theme_key)
                    st.session_state[session_key] = normalized_theme
                    st.success(f"已切换为 {option.get('label', theme_key)} 主题。")
                    st.rerun()

    return current_theme


def render_insight_management_nav(project_id: str) -> dict:
    saved_settings = load_insight_settings(project_id)
    active_labels = _active_insight_source_labels(saved_settings)

    with st.sidebar.expander("Insight 管理", expanded=False):
        st.caption("以下设置为工作区全局设置，对所有项目同时生效。")
        if bool(saved_settings.get("enabled", True)):
            if active_labels:
                st.caption(f"当前状态：已开启 · 已启用 {len(active_labels)}/4 个来源")
            else:
                st.caption("当前状态：已开启，但暂未启用任何信息来源。")
        else:
            st.caption("当前状态：已关闭，不会生成新的 insight card。")

        with st.form(f"insight_management_form_{project_id}"):
            enabled = st.checkbox(
                "启用 Insight 功能（全局总开关）",
                value=bool(saved_settings.get("enabled", True)),
            )
            st.caption("关闭后会停止生成新的 insight card，但已保存的 insight card 和偏好规则文档会继续保留。")

            source_values: dict[str, bool] = {}
            for source_name, label in _INSIGHT_SOURCE_LABELS.items():
                source_values[source_name] = st.checkbox(
                    label,
                    value=bool(saved_settings.get("source_enabled", {}).get(source_name, True)),
                    disabled=not enabled,
                )

            submitted = st.form_submit_button("保存 Insight 设置", use_container_width=True)

        if submitted:
            updated_settings = save_insight_settings(
                project_id,
                enabled=enabled,
                source_enabled=source_values,
            )
            updated_labels = _active_insight_source_labels(updated_settings)
            if not bool(updated_settings.get("enabled", True)):
                st.success("Insight 已关闭；历史已保存卡片和规则文档已保留。")
            elif not updated_labels:
                st.success("Insight 设置已保存。当前未启用任何来源，因此不会生成新的卡片。")
            else:
                st.success(f"Insight 设置已保存。当前启用 {len(updated_labels)} 个来源。")
            st.rerun()

        if bool(saved_settings.get("enabled", True)) and not active_labels:
            st.warning("当前总开关已开启，但四个来源都已关闭，因此新的 insight card 不会生成。")

    return saved_settings


def _render_generate_plan_form(project_id: str, is_regen: bool = False) -> None:
    label = "重新输入 OKR（Regenerate）" if is_regen else "输入你的 OKR"
    okr_text = st.text_area(
        label=label,
        placeholder="例如:\nO: 提升用户留存\nKR1: 次日留存提升到 35%\nKR2: 关键功能使用率提升 20%\nKR3: 新手引导完成率达到 70%",
    )

    planning_mode_label = st.radio(
        "任务生成模式",
        options=["单Agent", "多Agent"],
        horizontal=True,
    )
    planning_mode = "multi" if planning_mode_label.startswith("多Agent") else "single"

    saved_total_weeks, _ = load_saved_plan_total_weeks(project_id)
    total_weeks = st.number_input(
        "项目完成周数",
        min_value=1,
        max_value=520,
        value=int(saved_total_weeks),
        step=1,
        key=f"plan_total_weeks_input_{project_id}",
        help="例如输入 12，表示计划总时长固定为 12 周左右。",
    )

    btn_label = "重新生成计划" if is_regen else "Generate Plan"
    if st.button(btn_label, key="gen_plan_form_btn"):
        if not okr_text.strip():
            st.warning("请先输入 OKR，再生成计划。")
        else:
            try:
                normalized_total_weeks = max(1, int(total_weeks))
                save_plan_total_weeks(project_id, normalized_total_weeks)
                reset_token_usage_tracker()
                if planning_mode == "single":
                    with st.spinner("正在生成任务计划...  这大概需要1分钟左右✈️..."):
                        generated = generate_plan_by_mode(
                            okr_text,
                            project_id,
                            mode=planning_mode,
                            target_total_weeks=normalized_total_weeks,
                        )
                else:
                    status_placeholder = st.empty()
                    detail_placeholder = st.empty()
                    progress_placeholder = st.empty()
                    progress_bar = progress_placeholder.progress(0)
                    spinner_frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
                    frame_state = {"idx": 0}

                    def _progress_callback(event: dict) -> None:
                        total_steps = int(event.get("total_steps", 3) or 3)
                        completed_steps = int(event.get("completed_steps", 0) or 0)
                        completed_steps = max(0, min(total_steps, completed_steps))
                        percent = int(round((completed_steps * 100) / max(1, total_steps)))

                        phase = str(event.get("phase") or "working").strip().lower()
                        label = str(event.get("label") or "Agent").strip()
                        message = str(event.get("message") or "").strip()

                        frame = spinner_frames[frame_state["idx"] % len(spinner_frames)]
                        frame_state["idx"] += 1

                        if phase == "done":
                            status_placeholder.markdown(f"✅ {label} completed")
                        else:
                            status_placeholder.markdown(f"{frame} {label} working...")

                        if message:
                            detail_placeholder.caption(message)
                        progress_bar.progress(percent)

                    with st.spinner("多Agent 正在生成任务计划... 正在依次调用各个 agent"):
                        generated = generate_plan_by_mode(
                            okr_text,
                            project_id,
                            mode=planning_mode,
                            progress_callback=_progress_callback,
                            target_total_weeks=normalized_total_weeks,
                        )

                    progress_bar.progress(100)
                    status_placeholder.markdown("✅ Multi-Agent planning completed")
                    detail_placeholder.caption("所有 agent 已完成，正在进入结果展示。")
                usage = get_token_usage_tracker()
                st.session_state._gen_result = {
                    "count": len(generated),
                    "total_tokens": int(usage.get("total_tokens", 0) or 0),
                    "prompt_tokens": int(usage.get("prompt_tokens", 0) or 0),
                    "completion_tokens": int(usage.get("completion_tokens", 0) or 0),
                    "request_count": int(usage.get("request_count", 0) or 0),
                    "logical_call_count": int(usage.get("logical_call_count", 0) or 0),
                    "empty_response_retry_count": int(usage.get("empty_response_retry_count", 0) or 0),
                }
                st.session_state.main_view_mode = "dashboard"
                st.rerun()
            except Exception as exc:
                err_text = str(exc)
                if "finish_reason=length" in err_text or "手动提高 Max Tokens" in err_text:
                    st.error("模型输出被长度限制截断。请到 LLM 配置里手动调高 Max Tokens 后重试。")
                else:
                    st.error(f"计划生成失败：{err_text}")

    st.markdown('<div class="helper-note">支持多行文本，建议按 O / KR 分行输入。</div>', unsafe_allow_html=True)


def _saved_summary_folder_key(project_id: str) -> str:
    return f"saved_summary_folder_{project_id}"


def _seen_files_key(project_id: str) -> str:
    """session_state key: dict[folder_str, list[filename]] for processed files."""
    return f"seen_summary_files_{project_id}"


def _show_summary_on_dashboard_key(project_id: str) -> str:
    """Session-only flag: show summary result on dashboard after a fresh update."""
    return f"show_summary_on_dashboard_{project_id}"


def _normalize_summary_seen_map(raw_seen_map) -> dict[str, list[str]]:
    if not isinstance(raw_seen_map, dict):
        return {}

    normalized: dict[str, list[str]] = {}
    for raw_folder, raw_names in raw_seen_map.items():
        folder = normalize_summary_folder_path(raw_folder)
        if not folder:
            continue

        if isinstance(raw_names, list):
            iterable = raw_names
        elif isinstance(raw_names, (set, tuple)):
            iterable = list(raw_names)
        else:
            continue

        names: list[str] = []
        seen_names: set[str] = set()
        for raw_name in iterable:
            name = str(raw_name or "").replace("\\", "/").strip()
            if not name or name in seen_names:
                continue
            seen_names.add(name)
            names.append(name)

        existing = normalized.setdefault(folder, [])
        for name in names:
            if name not in existing:
                existing.append(name)

    return normalized


def _hydrate_summary_state(project_id: str, force: bool = False) -> None:
    """Ensure summary path + seen-files cache are available in session_state."""
    saved_key = _saved_summary_folder_key(project_id)
    seen_key = _seen_files_key(project_id)

    if not force:
        cached_seen = st.session_state.get(seen_key)
        if saved_key in st.session_state and isinstance(cached_seen, dict):
            return

    persisted = load_project_summary_state(project_id)
    st.session_state[saved_key] = normalize_summary_folder_path(persisted.get("saved_summary_folder", ""))
    seen_summary_files = _normalize_summary_seen_map(persisted.get("seen_summary_files", {}))
    st.session_state[seen_key] = seen_summary_files


def _hydrate_summary_update_result(project_id: str, force: bool = False) -> None:
    """Restore the latest successful summary table from project state."""
    current = st.session_state.get("summary_update_result")
    if not force and isinstance(current, dict):
        if current.get("project_id") == project_id and current.get("status") == "ok":
            return

    persisted = load_project_summary_state(project_id)
    latest = persisted.get("latest_summary_update_result")
    if isinstance(latest, dict):
        st.session_state.summary_update_result = latest
    elif force:
        st.session_state.summary_update_result = None


def _insight_feed_open_key(project_id: str) -> str:
    return f"insight_feed_open_{project_id}"


def _insight_auto_checked_key(project_id: str) -> str:
    return f"insight_auto_checked_{project_id}"


def _mark_insight_auto_checked(project_id: str, checked_on: str | None = None) -> None:
    normalized_project_id = str(project_id or "").strip()
    if not normalized_project_id:
        return

    normalized_date = str(checked_on or date.today().isoformat()).strip() or date.today().isoformat()
    st.session_state[_insight_auto_checked_key(normalized_project_id)] = normalized_date


def _insight_toast_seen_key(project_id: str) -> str:
    return f"insight_toast_seen_{project_id}"


def _insight_feed_page_key(project_id: str) -> str:
    return f"insight_feed_page_{project_id}"


def _insight_feedback_ack_token_key(project_id: str) -> str:
    return f"insight_feedback_ack_token_{project_id}"


def _insight_feedback_ack_msg_key(project_id: str) -> str:
    return f"insight_feedback_ack_msg_{project_id}"


def _saved_insight_selected_card_key(project_id: str) -> str:
    return f"saved_insight_selected_card_{project_id}"


def _saved_insight_pending_unsave_key(project_id: str) -> str:
    return f"saved_insight_pending_unsave_{project_id}"


def _saved_insight_status_msg_key(project_id: str) -> str:
    return f"saved_insight_status_msg_{project_id}"


def _saved_insight_edit_annotation_key(project_id: str) -> str:
    return f"saved_insight_edit_annotation_{project_id}"


def _saved_insight_note_mode_key(project_id: str, card_id: str) -> str:
    return f"saved_insight_note_mode_{project_id}_{card_id}"


def _saved_insight_note_text_key(project_id: str, card_id: str) -> str:
    return f"saved_insight_note_text_{project_id}_{card_id}"


def _clear_saved_insight_annotation_draft(project_id: str, card_id: str = "") -> None:
    editing_key = _saved_insight_edit_annotation_key(project_id)
    normalized_card_id = str(card_id or st.session_state.get(editing_key) or "").strip()
    st.session_state[editing_key] = ""
    if not normalized_card_id:
        return

    st.session_state.pop(_saved_insight_note_mode_key(project_id, normalized_card_id), None)
    st.session_state.pop(_saved_insight_note_text_key(project_id, normalized_card_id), None)


def _insight_pending_save_card_key(project_id: str) -> str:
    return f"insight_pending_save_card_{project_id}"


def _insight_save_note_mode_key(project_id: str, card_id: str) -> str:
    return f"insight_save_note_mode_{project_id}_{card_id}"


def _insight_save_note_text_key(project_id: str, card_id: str) -> str:
    return f"insight_save_note_text_{project_id}_{card_id}"


def _clear_insight_save_draft(project_id: str, card_id: str = "") -> None:
    pending_key = _insight_pending_save_card_key(project_id)
    normalized_card_id = str(card_id or st.session_state.get(pending_key) or "").strip()
    st.session_state[pending_key] = ""
    if not normalized_card_id:
        return

    st.session_state.pop(_insight_save_note_mode_key(project_id, normalized_card_id), None)
    st.session_state.pop(_insight_save_note_text_key(project_id, normalized_card_id), None)


def _active_insight_source_labels(insight_settings: dict) -> list[str]:
    source_enabled = (
        insight_settings.get("source_enabled", {})
        if isinstance(insight_settings, dict)
        else {}
    )
    return [
        label
        for source_name, label in _INSIGHT_SOURCE_LABELS.items()
        if bool(source_enabled.get(source_name, True))
    ]


def _insight_generation_ready(insight_settings: dict) -> bool:
    if not bool((insight_settings or {}).get("enabled", True)):
        return False
    return bool(_active_insight_source_labels(insight_settings))


def _insight_generation_disabled_message(insight_settings: dict) -> str:
    if not bool((insight_settings or {}).get("enabled", True)):
        return "Insight 总开关已关闭，不会生成新的 insight card；历史卡片和规则文档仍会保留。"
    return "当前四个信息来源都已关闭，请先在左侧“Insight 管理”中至少开启一个来源。"


def _saved_annotation_html(annotation: str) -> str:
    normalized = str(annotation or "").strip() or "无"
    content = html.escape(normalized).replace("\n", "<br/>")
    return f"<div class='saved-insight-annotation'>批注内容：{content}</div>"


def _set_insight_feedback_ack(project_id: str, message: str) -> None:
    st.session_state[_insight_feedback_ack_token_key(project_id)] = datetime.now().isoformat(timespec="milliseconds")
    st.session_state[_insight_feedback_ack_msg_key(project_id)] = str(message or "").strip()


# ---------------------------------------------------------------------------
# Background-job helpers
# ---------------------------------------------------------------------------

def _insight_is_running(project_id: str) -> bool:
    job = _insight_bg_jobs.get(project_id)
    return bool(job and job.get("running"))


def _insight_pop_done(project_id: str) -> dict | None:
    """If the job is done, atomically clear the flag and return the job snapshot."""
    with _insight_bg_lock:
        job = _insight_bg_jobs.get(project_id)
        if job and job.get("done"):
            job["done"] = False
            return dict(job)
    return None


def _launch_insight_bg(project_id: str, fn, *args, open_after: bool = False, **kwargs) -> None:
    """Run *fn* in a daemon thread; records running/done/result in _insight_bg_jobs."""
    if _insight_is_running(project_id):
        return
    with _insight_bg_lock:
        _insight_bg_jobs[project_id] = {
            "running": True,
            "done": False,
            "result": None,
            "error": "",
            "open_after": open_after,
        }

    def _worker() -> None:
        try:
            result = fn(*args, **kwargs)
            with _insight_bg_lock:
                job = _insight_bg_jobs.get(project_id)
                if isinstance(job, dict):
                    job.update({"running": False, "done": True, "result": result})
                else:
                    _insight_bg_jobs[project_id] = {
                        "running": False,
                        "done": True,
                        "result": result,
                        "error": "",
                        "open_after": open_after,
                    }
        except Exception as exc:  # noqa: BLE001
            with _insight_bg_lock:
                job = _insight_bg_jobs.get(project_id)
                if isinstance(job, dict):
                    job.update({"running": False, "done": True, "error": str(exc)})
                else:
                    _insight_bg_jobs[project_id] = {
                        "running": False,
                        "done": True,
                        "result": None,
                        "error": str(exc),
                        "open_after": open_after,
                    }

    threading.Thread(target=_worker, daemon=True).start()


@st.dialog("💡 今日 Insight 已生成")
def _insight_done_dialog(project_id: str) -> None:
    st.markdown("今日 Insight 已生成，可点击右下角按钮查看。")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("立即查看", key=f"dlg_view_{project_id}", use_container_width=True, type="secondary"):
            st.session_state[_insight_feed_open_key(project_id)] = True
            mark_insight_notification_seen(project_id)
            st.rerun()
    with col2:
        if st.button("关闭", key=f"dlg_later_{project_id}", use_container_width=True, type="secondary"):
            st.rerun()


def _run_daily_insight_auto_trigger(
    project_id: str,
    insight_settings: dict | None = None,
    insight_state: dict | None = None,
) -> None:
    settings = insight_settings if isinstance(insight_settings, dict) else load_insight_settings(project_id)
    if not _insight_generation_ready(settings):
        return

    check_key = _insight_auto_checked_key(project_id)
    today = date.today().isoformat()
    if st.session_state.get(check_key) == today:
        return

    state = insight_state if isinstance(insight_state, dict) else load_insight_state(project_id)
    latest_feed = state.get("latest_feed") if isinstance(state, dict) else None
    latest_feed_date = str(latest_feed.get("date") or "").strip() if isinstance(latest_feed, dict) else ""
    if str(state.get("last_generated_on") or "").strip() == today or latest_feed_date == today:
        st.session_state[check_key] = today
        return

    if _insight_is_running(project_id):
        return

    st.session_state[check_key] = today
    _launch_insight_bg(project_id, ensure_daily_insight_feed, project_id, open_after=False)


def _maybe_show_insight_toast(project_id: str, insight_state: dict | None = None) -> dict:
    state = insight_state if isinstance(insight_state, dict) else load_insight_state(project_id)
    latest_feed = state.get("latest_feed")
    if not state.get("pending_notification"):
        return state
    if not isinstance(latest_feed, dict):
        return state

    feed_id = str(latest_feed.get("feed_id") or "").strip()
    toast_seen_key = _insight_toast_seen_key(project_id)
    if st.session_state.get(toast_seen_key) != feed_id:
        st.toast("今日 insight 已生成，可点击右下角按钮查看。", icon="💡")
        st.session_state[toast_seen_key] = feed_id

    return state


def _generate_insight_and_refresh(project_id: str, *, open_after_generate: bool = True) -> None:
    insight_settings = load_insight_settings(project_id)
    if not _insight_generation_ready(insight_settings):
        st.info(_insight_generation_disabled_message(insight_settings))
        return

    if _insight_is_running(project_id):
        st.info("Insight 正在生成中，请稍候…")
        return

    st.session_state[f"insight_manual_requested_{project_id}"] = datetime.now().isoformat(timespec="seconds")
    st.toast("已触发手动生成，正在检索并生成 Insight…", icon="⏳")
    _launch_insight_bg(
        project_id,
        generate_project_insight_feed,
        project_id,
        open_after=open_after_generate,
        trigger_mode="manual",
    )
    st.rerun()


def _shift_insight_page(page_key: str, delta: int, max_index: int) -> None:
    current = int(st.session_state.get(page_key, 0) or 0)
    target = current + int(delta)
    target = max(0, min(target, max(0, int(max_index))))
    st.session_state[page_key] = target


def _format_insight_source_label(source: str) -> str:
    normalized = str(source or "").strip().lower()
    if not normalized:
        return "Unknown"

    mapping = {
        "arxiv": "ArXiv",
        "github": "GitHub",
        "reddit": "Reddit",
        "reddit_read_only": "Reddit",
        "hn_algolia": "Hacker News",
        "open_source": "Open Source",
        "paper": "Paper",
        "blog": "Blog",
    }
    return mapping.get(normalized, normalized)


def _truncate_display_text(value: str, max_chars: int = 88) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if len(text) <= max(8, int(max_chars)):
        return text
    return f"{text[: max(8, int(max_chars)) - 1].rstrip()}..."


def _format_saved_insight_kind(card: dict) -> str:
    source_type = str(card.get("source_type") or "").strip().lower()
    source = str(card.get("source") or "").strip().lower()

    if source_type in {"paper"} or source in {"arxiv"}:
        return "论文"
    if source_type in {"blog"}:
        return "博客"
    if source_type in {"open_source", "project", "github", "repo"} or source in {"github", "open_source"}:
        return "项目"
    return "内容"


def _format_saved_insight_catalog_line(card: dict) -> str:
    project_name = str(card.get("project_name") or "").strip() or "当前项目"
    item_kind = _format_saved_insight_kind(card)
    title = _truncate_display_text(str(card.get("title") or "").strip(), max_chars=66)
    source_name = _truncate_display_text(str(card.get("source") or "").strip(), max_chars=30)
    item_name = title or source_name or "未命名条目"
    insight_content = _truncate_display_text(str(card.get("core_insight") or "").strip(), max_chars=110)

    if insight_content:
        return f"{project_name} —— {item_kind}：{item_name} —— {insight_content}"
    return f"{project_name} —— {item_kind}：{item_name}"


@st.fragment
def _render_insight_feed(project_id: str) -> None:
    if not bool(st.session_state.get(_insight_feed_open_key(project_id), False)):
        return

    insight_state = load_insight_state(project_id)
    insight_settings = load_insight_settings(project_id)
    generation_ready = _insight_generation_ready(insight_settings)
    latest_feed = insight_state.get("latest_feed")
    saved_card_ids_raw = insight_state.get("saved_card_ids", [])
    saved_card_ids = {
        str(item).strip()
        for item in (saved_card_ids_raw if isinstance(saved_card_ids_raw, list) else [])
        if str(item).strip()
    }

    page_key = _insight_feed_page_key(project_id)
    pending_save_key = _insight_pending_save_card_key(project_id)
    is_running = _insight_is_running(project_id)
    manual_requested_key = f"insight_manual_requested_{project_id}"
    run_error_key = f"insight_auto_error_{project_id}"

    # Fixed backdrop + modal container to emulate a floating popup window.
    st.markdown("<div class='insight-modal-backdrop'></div>", unsafe_allow_html=True)
    with st.container(key=f"insight_modal_{project_id}"):
        head_left, head_mid, head_right = st.columns([5.6, 2.2, 1], gap="small")
        with head_left:
            st.markdown("### 🧠 Insight Engine")
        with head_mid:
            if is_running:
                manual_btn_label = "⏳ 生成中…"
            elif generation_ready:
                manual_btn_label = "手动生成"
            else:
                manual_btn_label = "Insight 已关闭"
            if st.button(
                manual_btn_label,
                key=f"insight_manual_generate_{project_id}",
                use_container_width=True,
                disabled=is_running or not generation_ready,
            ):
                _generate_insight_and_refresh(project_id, open_after_generate=True)
        with head_right:
            if st.button("✕", key=f"insight_hide_feed_{project_id}", use_container_width=True):
                st.session_state[_insight_feed_open_key(project_id)] = False
                st.session_state[page_key] = 0
                st.rerun(scope="fragment")

        if is_running:
            st.info("正在生成新的 Insight，当前卡片仍为上次结果，完成后会自动刷新。")
        elif st.session_state.get(manual_requested_key):
            st.session_state.pop(manual_requested_key, None)

        run_error = str(st.session_state.get(run_error_key) or "").strip()
        if run_error:
            st.error(f"上次生成失败：{run_error}")

        if not generation_ready:
            st.info(_insight_generation_disabled_message(insight_settings))

        ack_token_key = _insight_feedback_ack_token_key(project_id)
        ack_msg_key = _insight_feedback_ack_msg_key(project_id)
        ack_token = str(st.session_state.get(ack_token_key) or "").strip()
        if ack_token:
            ack_msg = html.escape(str(st.session_state.get(ack_msg_key) or "").strip() or "assistant 已收到你的意见，后续会优化推荐结果。")
            st.markdown(f"<div class='insight-feedback-ack'>{ack_msg}</div>", unsafe_allow_html=True)
            st.session_state.pop(ack_token_key, None)
            st.session_state.pop(ack_msg_key, None)

        if not isinstance(latest_feed, dict):
            if is_running:
                st.info("正在生成今日 Insight，请稍候…")
            else:
                if generation_ready:
                    st.info("今日暂无 insight。可点击“手动生成”创建。")
                else:
                    st.info("当前没有可展示的 insight card。")
            return

        mark_insight_notification_seen(project_id)

        feed_date = str(latest_feed.get("date") or "").strip()
        feed_is_today = feed_date == date.today().isoformat()
        retrieval = latest_feed.get("retrieval", {}) if isinstance(latest_feed.get("retrieval"), dict) else {}
        retrieval_mode = str(retrieval.get("source") or "").strip() or "unknown"
        candidate_count = int(retrieval.get("candidate_count", 0) or 0)
        st.caption(f"生成日期：{feed_date} | 检索模式：{retrieval_mode} | 候选：{candidate_count}")
        if not feed_is_today and generation_ready and not is_running:
            stale_label = feed_date or "历史"
            st.warning(f"当前展示的是 {stale_label} 的历史 Insight。可点击上方“手动生成”刷新今日卡片。")

        raw_cards = latest_feed.get("cards", []) if isinstance(latest_feed.get("cards"), list) else []
        valid_cards = [card for card in raw_cards if isinstance(card, dict)]
        cards = valid_cards[:3]
        feed_id = str(latest_feed.get("feed_id") or "").strip()

        if not cards:
            st.info("今日 insight 为空。可点击“手动生成”重试。")
            return

        if len(valid_cards) > 3:
            st.caption("仅展示前三张 Insight 卡片。")

        max_index = len(cards) - 1
        page_idx = int(st.session_state.get(page_key, 0) or 0)
        page_idx = min(max(page_idx, 0), max_index)
        st.session_state[page_key] = page_idx

        card = cards[page_idx]
        card_id = str(card.get("card_id") or f"card_{page_idx + 1}").strip()
        pending_save_card_id = str(st.session_state.get(pending_save_key) or "").strip()
        title = html.escape(str(card.get("title") or "(Untitled)").strip())
        url = html.escape(str(card.get("url") or "").strip())
        source_label = html.escape(_format_insight_source_label(str(card.get("source") or card.get("source_type") or "").strip()))
        project_name = html.escape(str(card.get("project_name") or "").strip() or "当前项目")
        core_insight = html.escape(str(card.get("core_insight") or "").strip())
        risk_alert = html.escape(str(card.get("risk_alert") or "").strip())
        alternative = html.escape(str(card.get("alternative") or "").strip())
        relevance_reason = html.escape(str(card.get("relevance_reason") or "").strip())
        evidence = html.escape(str(card.get("evidence_snippet") or "").strip())
        is_saved = card_id in saved_card_ids
        note_mode_key = _insight_save_note_mode_key(project_id, card_id)
        note_text_key = _insight_save_note_text_key(project_id, card_id)
        if pending_save_card_id == card_id and not is_saved:
            st.session_state.setdefault(note_mode_key, "添加批注")
            st.session_state.setdefault(note_text_key, "")

        title_markup = f'<a href="{url}" target="_blank">{title}</a>' if url else title

        side_left, card_col, side_right = st.columns([1, 8, 1], gap="small", vertical_alignment="center")
        with side_left:
            with st.container(key=f"insight_side_prev_{project_id}"):
                st.button(
                    "◀",
                    key=f"insight_prev_{project_id}",
                    use_container_width=True,
                    disabled=page_idx <= 0,
                    on_click=_shift_insight_page,
                    args=(page_key, -1, max_index),
                )
        with card_col:
            st.markdown(
                f"""
                <div class="insight-modal-card">
                    <p class="insight-card-title">📄 {title_markup}</p>
                    <p class="insight-label">🔎 来源</p>
                    <p class="insight-body">{source_label}</p>
                    <p class="insight-label">🗂 关联项目</p>
                    <p class="insight-body">{project_name}</p>
                    <p class="insight-label">💡 Insight</p>
                    <p class="insight-body">{core_insight or '暂无'}</p>
                    <p class="insight-label">⚠️ 风险提醒</p>
                    <p class="insight-body">{risk_alert or '暂无'}</p>
                    <p class="insight-label">🔀 新方向建议</p>
                    <p class="insight-body">{alternative or '暂无'}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div class='insight-page-indicator'>第 {page_idx + 1} / {len(cards)} 张</div>",
                unsafe_allow_html=True,
            )
        with side_right:
            with st.container(key=f"insight_side_next_{project_id}"):
                st.button(
                    "▶",
                    key=f"insight_next_{project_id}",
                    use_container_width=True,
                    disabled=page_idx >= max_index,
                    on_click=_shift_insight_page,
                    args=(page_key, 1, max_index),
                )

        if pending_save_card_id == card_id and not is_saved:
            st.info("你刚刚点击了“保存”。请在下面填写批注，或切换到“不批注”后再确认保存。")
            note_mode = st.radio(
                "保存时是否添加批注？",
                options=["添加批注", "不批注"],
                horizontal=True,
                key=note_mode_key,
            )
            st.markdown("**批注输入框**")
            st.text_input(
                "批注内容",
                key=note_text_key,
                disabled=note_mode == "不批注",
                placeholder=" ",
            )
            if note_mode == "不批注":
                st.caption("将直接保存，不附带批注。")

        btn_cols = st.columns(4, gap="small")
        with btn_cols[0]:
            if st.button("👍 有用", key=f"insight_useful_{project_id}_{card_id}", use_container_width=True):
                record_insight_feedback(project_id, card_id, "useful", feed_id=feed_id)
                _set_insight_feedback_ack(project_id, "assistant 已收到你的意见，后续会优化推荐结果。")
                st.rerun(scope="fragment")
        with btn_cols[1]:
            if st.button("👎 不相关", key=f"insight_not_relevant_{project_id}_{card_id}", use_container_width=True):
                record_insight_feedback(project_id, card_id, "not_relevant", feed_id=feed_id)
                _set_insight_feedback_ack(project_id, "assistant 已收到你的意见，后续会优化推荐结果。")
                st.rerun(scope="fragment")
        with btn_cols[2]:
            if is_saved:
                st.button(
                    "📌 已保存",
                    key=f"insight_saved_{project_id}_{card_id}",
                    use_container_width=True,
                    disabled=True,
                )
            elif pending_save_card_id == card_id:
                if st.button(
                    "✅ 确认保存",
                    key=f"insight_save_confirm_{project_id}_{card_id}",
                    use_container_width=True,
                ):
                    note_mode = str(st.session_state.get(note_mode_key) or "添加批注")
                    annotation = ""
                    if note_mode == "添加批注":
                        annotation = str(st.session_state.get(note_text_key) or "").strip()
                        if not annotation:
                            st.warning("请输入批注内容，或选择“不批注”。")
                            return

                    feedback = record_insight_feedback(
                        project_id,
                        card_id,
                        "save",
                        feed_id=feed_id,
                        annotation=annotation,
                    )
                    if str(feedback.get("status") or "").strip().lower() == "ok":
                        _clear_insight_save_draft(project_id, card_id)
                        if annotation:
                            _set_insight_feedback_ack(project_id, "已保存 insight card，并记录了你的批注。")
                        else:
                            _set_insight_feedback_ack(project_id, "已保存 insight card。")
                        st.rerun(scope="fragment")
                    st.error(str(feedback.get("message") or "保存 insight card 失败。"))
            elif st.button("📌 保存", key=f"insight_save_{project_id}_{card_id}", use_container_width=True):
                st.session_state[pending_save_key] = card_id
                st.session_state[note_mode_key] = "添加批注"
                st.rerun(scope="fragment")
        with btn_cols[3]:
            if pending_save_card_id == card_id and not is_saved:
                if st.button(
                    "取消保存",
                    key=f"insight_save_cancel_{project_id}_{card_id}",
                    use_container_width=True,
                    type="secondary",
                ):
                    _clear_insight_save_draft(project_id, card_id)
                    st.rerun(scope="fragment")
            else:
                deep_key = f"insight_deep_open_{project_id}_{card_id}"
                if st.button("🔍 深入", key=f"insight_deep_{project_id}_{card_id}", use_container_width=True):
                    record_insight_feedback(project_id, card_id, "deep_dive", feed_id=feed_id)
                    st.session_state[deep_key] = not bool(st.session_state.get(deep_key, False))

        deep_key = f"insight_deep_open_{project_id}_{card_id}"
        if st.session_state.get(deep_key):
            st.caption(f"相关性说明：{relevance_reason or '暂无'}")
            if evidence:
                st.caption(f"证据片段：{evidence}")


def _render_saved_insight_cards_view(project_id: str) -> None:
    st.markdown("### 已保存的 Insight Card")

    selected_key = _saved_insight_selected_card_key(project_id)
    pending_unsave_key = _saved_insight_pending_unsave_key(project_id)
    status_key = _saved_insight_status_msg_key(project_id)
    editing_annotation_key = _saved_insight_edit_annotation_key(project_id)

    status_msg = str(st.session_state.get(status_key) or "").strip()
    if status_msg:
        st.success(status_msg)
        st.session_state.pop(status_key, None)

    payload = list_saved_insight_cards(project_id)
    cards_raw = payload.get("saved_cards", []) if isinstance(payload, dict) else []
    cards = [card for card in cards_raw if isinstance(card, dict)]

    if not cards:
        st.session_state[selected_key] = ""
        st.session_state[pending_unsave_key] = ""
        _clear_saved_insight_annotation_draft(project_id)
        st.info("当前还没有已保存的 insight card。先在“查看今日 Insight”里点击“保存”。")
        return

    cards_by_id: dict[str, dict] = {}
    for card in cards:
        card_id = str(card.get("card_id") or "").strip()
        if card_id and card_id not in cards_by_id:
            cards_by_id[card_id] = card

    selected_card_id = str(st.session_state.get(selected_key) or "").strip()
    if selected_card_id and selected_card_id not in cards_by_id:
        st.session_state[selected_key] = ""
        st.session_state[pending_unsave_key] = ""
        _clear_saved_insight_annotation_draft(project_id)
        selected_card_id = ""

    editing_annotation_card_id = str(st.session_state.get(editing_annotation_key) or "").strip()
    if selected_card_id and editing_annotation_card_id and editing_annotation_card_id != selected_card_id:
        _clear_saved_insight_annotation_draft(project_id, editing_annotation_card_id)
        editing_annotation_card_id = ""

    st.caption(f"已保存 {len(cards_by_id)} 张卡片。")

    if not selected_card_id:
        if editing_annotation_card_id:
            _clear_saved_insight_annotation_draft(project_id, editing_annotation_card_id)
        st.caption("目录：点击任意条目可进入卡片详情。")
        for idx, card in enumerate(cards, start=1):
            card_id = str(card.get("card_id") or "").strip()
            if not card_id:
                continue

            line_text = html.escape(_format_saved_insight_catalog_line(card))
            source_label = html.escape(
                _format_insight_source_label(str(card.get("source") or card.get("source_type") or "").strip())
            )
            saved_at = str(card.get("saved_at") or "").strip().replace("T", " ") or "未知"
            st.markdown(
                (
                    "<div class='saved-insight-directory-item'>"
                    f"<p class='saved-insight-directory-line'>{idx}. {line_text}</p>"
                    f"<p class='saved-insight-directory-meta'>来源：{source_label} · 收藏时间：{html.escape(saved_at)}</p>"
                    f"{_saved_annotation_html(str(card.get('annotation') or ''))}"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )
            if st.button(
                "查看卡片",
                key=f"saved_insight_open_{project_id}_{card_id}_{idx}",
                use_container_width=True,
            ):
                st.session_state[selected_key] = card_id
                st.session_state[pending_unsave_key] = ""
                _clear_saved_insight_annotation_draft(project_id)
                st.rerun()
        return

    card = cards_by_id[selected_card_id]
    if st.button("← 返回目录", key=f"saved_insight_back_{project_id}", type="secondary"):
        st.session_state[selected_key] = ""
        st.session_state[pending_unsave_key] = ""
        _clear_saved_insight_annotation_draft(project_id, selected_card_id)
        st.rerun()

    title = html.escape(str(card.get("title") or "(Untitled)").strip())
    url = html.escape(str(card.get("url") or "").strip())
    title_markup = f'<a href="{url}" target="_blank">{title}</a>' if url else title
    source_label = html.escape(
        _format_insight_source_label(str(card.get("source") or card.get("source_type") or "").strip())
    )
    project_name = html.escape(str(card.get("project_name") or "").strip() or "当前项目")
    core_insight = html.escape(str(card.get("core_insight") or "").strip())
    risk_alert = html.escape(str(card.get("risk_alert") or "").strip())
    alternative = html.escape(str(card.get("alternative") or "").strip())
    relevance_reason = html.escape(str(card.get("relevance_reason") or "").strip())
    evidence = html.escape(str(card.get("evidence_snippet") or "").strip())
    current_annotation = str(card.get("annotation") or "").strip()

    st.markdown(
        f"""
        <div class="insight-modal-card">
            <p class="insight-card-title">📄 {title_markup}</p>
            <p class="insight-label">🔎 来源</p>
            <p class="insight-body">{source_label}</p>
            <p class="insight-label">🗂 关联项目</p>
            <p class="insight-body">{project_name}</p>
            <p class="insight-label">💡 Insight</p>
            <p class="insight-body">{core_insight or '暂无'}</p>
            <p class="insight-label">⚠️ 风险提醒</p>
            <p class="insight-body">{risk_alert or '暂无'}</p>
            <p class="insight-label">🔀 新方向建议</p>
            <p class="insight-body">{alternative or '暂无'}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(_saved_annotation_html(str(card.get("annotation") or "")), unsafe_allow_html=True)
    if relevance_reason:
        st.caption(f"相关性说明：{relevance_reason}")
    if evidence:
        st.caption(f"证据片段：{evidence}")

    pending_unsave_card_id = str(st.session_state.get(pending_unsave_key) or "").strip()
    editing_annotation_card_id = str(st.session_state.get(editing_annotation_key) or "").strip()

    if editing_annotation_card_id == selected_card_id:
        note_mode_key = _saved_insight_note_mode_key(project_id, selected_card_id)
        note_text_key = _saved_insight_note_text_key(project_id, selected_card_id)
        st.session_state.setdefault(note_mode_key, "修改批注")
        st.session_state.setdefault(note_text_key, current_annotation)

        st.markdown("#### 编辑批注")
        note_mode = st.radio(
            "批注操作",
            options=["修改批注", "清空批注"],
            horizontal=True,
            key=note_mode_key,
        )

        st.markdown("**批注输入框**")
        st.text_input(
            "批注内容",
            key=note_text_key,
            disabled=note_mode == "清空批注",
            placeholder="例如：这张卡片适合作为我当前项目下一轮实验的参考。",
        )
        if note_mode == "清空批注":
            st.caption("保存后会清空当前批注，卡片本身仍会保留在已保存目录中。")

        edit_save_col, edit_cancel_col = st.columns(2)
        with edit_save_col:
            if st.button(
                "保存批注",
                key=f"saved_insight_annotation_save_{project_id}_{selected_card_id}",
                use_container_width=True,
            ):
                updated_annotation = ""
                if note_mode == "修改批注":
                    updated_annotation = str(st.session_state.get(note_text_key) or "").strip()
                    if not updated_annotation:
                        st.warning("请输入批注内容，或切换到“清空批注”。")
                        return

                result = update_saved_insight_card_annotation(
                    project_id,
                    selected_card_id,
                    updated_annotation,
                )
                if str(result.get("status") or "").strip().lower() == "ok":
                    _clear_saved_insight_annotation_draft(project_id, selected_card_id)
                    st.session_state[status_key] = str(result.get("message") or "批注已更新。")
                    st.rerun()
                st.error(str(result.get("message") or "保存批注失败。"))
        with edit_cancel_col:
            if st.button(
                "取消编辑",
                key=f"saved_insight_annotation_cancel_{project_id}_{selected_card_id}",
                use_container_width=True,
                type="secondary",
            ):
                _clear_saved_insight_annotation_draft(project_id, selected_card_id)
                st.rerun()
        return

    if pending_unsave_card_id != selected_card_id:
        action_col, unsave_col = st.columns(2)
        with action_col:
            if st.button(
                "编辑批注",
                key=f"saved_insight_annotation_edit_{project_id}_{selected_card_id}",
                type="secondary",
                use_container_width=True,
            ):
                st.session_state[editing_annotation_key] = selected_card_id
                st.session_state[_saved_insight_note_mode_key(project_id, selected_card_id)] = "修改批注"
                st.session_state[_saved_insight_note_text_key(project_id, selected_card_id)] = current_annotation
                st.rerun()
        with unsave_col:
            if st.button(
                "取消收藏",
                key=f"saved_insight_unsave_{project_id}_{selected_card_id}",
                type="secondary",
                use_container_width=True,
            ):
                st.session_state[pending_unsave_key] = selected_card_id
                _clear_saved_insight_annotation_draft(project_id, selected_card_id)
                st.rerun()
        return

    st.warning("是否确定取消收藏？")
    confirm_col, cancel_col = st.columns(2)
    with confirm_col:
        if st.button(
            "确定取消收藏",
            key=f"saved_insight_unsave_confirm_{project_id}_{selected_card_id}",
            use_container_width=True,
        ):
            feedback = record_insight_feedback(
                project_id,
                selected_card_id,
                "unsave",
                feed_id=str(card.get("feed_id") or "").strip(),
            )
            if str(feedback.get("status") or "").strip().lower() == "ok":
                st.session_state[pending_unsave_key] = ""
                st.session_state[selected_key] = ""
                _clear_saved_insight_annotation_draft(project_id, selected_card_id)
                st.session_state[status_key] = "已取消收藏，这张卡片不会再出现在已保存目录中。"
                st.rerun()
            st.error(str(feedback.get("message") or "取消收藏失败。"))
    with cancel_col:
        if st.button(
            "再想想",
            key=f"saved_insight_unsave_cancel_{project_id}_{selected_card_id}",
            use_container_width=True,
            type="secondary",
        ):
            st.session_state[pending_unsave_key] = ""
            st.rerun()


def _render_insight_fab(project_id: str, insight_state: dict | None = None) -> None:
    is_running = _insight_is_running(project_id)
    insight_settings = load_insight_settings(project_id)
    generation_ready = _insight_generation_ready(insight_settings)

    # Floating status badge above the FAB button when generation is in progress
    if is_running:
        st.markdown(
            '<div style="position:fixed;right:1.2rem;bottom:4.6rem;width:250px;z-index:9999;'
            'background:rgba(15,118,110,0.12);border:1px solid #0f766e;border-radius:999px;'
            'padding:5px 14px;font-size:0.82rem;color:#0f766e;text-align:center;font-weight:600;">'
            '⏳ Insight 正在生成…</div>',
            unsafe_allow_html=True,
        )

    state = insight_state if isinstance(insight_state, dict) else load_insight_state(project_id)
    latest_feed = state.get("latest_feed")
    has_feed = isinstance(latest_feed, dict) and isinstance(latest_feed.get("cards"), list) and bool(latest_feed.get("cards"))
    today = date.today().isoformat()
    latest_feed_date = str(latest_feed.get("date") or "").strip() if isinstance(latest_feed, dict) else ""
    has_today_feed = has_feed and latest_feed_date == today

    open_key = _insight_feed_open_key(project_id)
    st.session_state.setdefault(open_key, False)
    is_open = bool(st.session_state.get(open_key, False))

    if is_running:
        button_label = "⏳ 正在生成…"
    elif has_today_feed:
        button_label = "查看今日 Insight"
    elif has_feed and generation_ready:
        stale_label = latest_feed_date or "历史"
        button_label = f"刷新今日 Insight（当前 {stale_label}）"
    elif has_feed:
        button_label = "查看历史 Insight"
    elif not generation_ready:
        button_label = "Insight 已关闭"
    else:
        button_label = "生成今日 Insight"

    if st.button(
        button_label,
        key=f"insight_fab_btn_{project_id}",
        use_container_width=True,
        disabled=is_running or (not generation_ready and not has_feed),
    ):
        if has_feed and (has_today_feed or not generation_ready):
            mark_insight_notification_seen(project_id)
            if not is_open:
                st.session_state[open_key] = True
                st.rerun()
        elif has_feed and generation_ready and not has_today_feed:
            _generate_insight_and_refresh(project_id, open_after_generate=True)
        else:
            _generate_insight_and_refresh(project_id, open_after_generate=True)



def _render_update_summary_form(project_id: str) -> None:
    st.markdown("### 根据团队工作总结，更新任务状态")

    _hydrate_summary_state(project_id)

    saved_key = _saved_summary_folder_key(project_id)
    seen_key = _seen_files_key(project_id)
    saved_path = normalize_summary_folder_path(st.session_state.get(saved_key, ""))
    # seen_map: {folder_str: [list of already-processed relative filenames]}
    raw_seen_map = st.session_state.get(seen_key, {})
    seen_map = _normalize_summary_seen_map(raw_seen_map)
    st.session_state[seen_key] = seen_map
    saved_threshold, has_saved_threshold = load_saved_risk_threshold(project_id)

    # Path input now supports dropdown quick-pick from historical folders.
    summary_folder_key = f"summary_folder_input_{project_id}"
    if summary_folder_key not in st.session_state:
        st.session_state[summary_folder_key] = saved_path
    else:
        raw_folder_input = str(st.session_state.get(summary_folder_key, "") or "").strip()
        normalized_folder_input = normalize_summary_folder_path(raw_folder_input)
        if raw_folder_input and normalized_folder_input and normalized_folder_input != raw_folder_input:
            st.session_state[summary_folder_key] = normalized_folder_input

    current_folder_input = normalize_summary_folder_path(st.session_state.get(summary_folder_key, ""))

    path_options: list[str] = []
    for candidate in [current_folder_input, saved_path, *seen_map.keys()]:
        candidate_text = normalize_summary_folder_path(candidate)
        if candidate_text and candidate_text not in path_options:
            path_options.append(candidate_text)

    if saved_path:
        st.caption(f"上次使用的路径：`{saved_path}`")

    summary_folder = st.selectbox(
        label="工作总结文件夹路径",
        options=path_options,
        index=None,
        placeholder="例如: D:/data/summaries 或 C:/Users/you/project_copilot/data/summaries",
        key=summary_folder_key,
        accept_new_options=True,
        help="点击输入框可从历史路径下拉选择，也可以直接输入新路径。",
    )

    keep_previous_threshold = False
    if has_saved_threshold:
        keep_previous_threshold = st.checkbox(
            f"保持之前的风险报告阈值（{saved_threshold}%）",
            value=True,
            help="勾选后将沿用上次设置的风险阈值，不需要重新输入。",
        )

    threshold_input_key = f"risk_threshold_input_{project_id}"
    default_threshold = saved_threshold if has_saved_threshold else 30
    selected_threshold = saved_threshold if keep_previous_threshold else st.number_input(
        "风险预警阈值（%）",
        min_value=0,
        max_value=100,
        value=int(default_threshold),
        step=1,
        key=threshold_input_key,
        help="当任务进度落后于预期进度达到该百分比时，系统会将该任务标记为 At Risk。",
    )
    st.caption("阈值越小越敏感，更容易触发预警；阈值越大越宽松，只提示明显落后的任务。")

    # ── Incremental-read hint ───────────────────────────────────────────────
    folder_preview = normalize_summary_folder_path((summary_folder or "").strip() or saved_path)
    if folder_preview and folder_preview in seen_map and seen_map[folder_preview]:
        prev_files = seen_map[folder_preview]
        st.caption(
            f"增量更新说明：上次已读取该文件夹中的 {len(prev_files)} 个文件，"
            "本次只会读取新增的文件，跳过已处理的旧文件。"
        )
        st.caption("已处理过的文件：" + "、".join(prev_files))
    else:
        st.caption("首次读取该文件夹时会处理全部文件；之后再次读取同一文件夹时，只会处理新增文件，跳过上次已处理的文件。")

    if st.button("更新任务状态", key="do_update_summary_btn"):
        folder = normalize_summary_folder_path(summary_folder)
        if not folder:
            st.warning("请先输入总结文件夹路径。")
        else:
            try:
                already_seen: set[str] = set(seen_map.get(folder, []))
                with st.spinner("正在分析工作总结..."):
                    update_result = update_tasks_from_summaries(
                        project_id,
                        folder,
                        seen_filenames=already_seen,
                        risk_lag_threshold=int(selected_threshold),
                    )
                status = update_result.get("status")
                if status == "ok":
                    update_result["project_id"] = project_id
                    new_files = update_result.get("new_filenames", [])
                    updated_seen = list(already_seen | set(new_files))
                    seen_map[folder] = updated_seen

                    # Persist first: even if later UI/session mutations fail,
                    # the summary path + seen files + latest parsed table survive restarts.
                    save_project_summary_state(
                        project_id,
                        saved_summary_folder=folder,
                        seen_summary_files=seen_map,
                        latest_summary_update_result=update_result,
                    )

                    # Keep the latest successful parsed table in session until next successful parse.
                    st.session_state.summary_update_result = update_result
                    st.session_state[_show_summary_on_dashboard_key(project_id)] = True
                    # Mirror persisted state into current session.
                    st.session_state[saved_key] = folder
                    st.session_state[seen_key] = seen_map
                    st.rerun()
                elif status == "no_new_summaries":
                    st.session_state[saved_key] = folder
                    latest_result = st.session_state.get("summary_update_result")
                    latest_result_for_project = (
                        latest_result
                        if isinstance(latest_result, dict)
                        and latest_result.get("project_id") == project_id
                        and latest_result.get("status") == "ok"
                        else None
                    )
                    save_project_summary_state(
                        project_id,
                        saved_summary_folder=folder,
                        seen_summary_files=seen_map,
                        latest_summary_update_result=latest_result_for_project,
                    )
                    st.warning(str(update_result.get("message") or "没有新的总结文件。"))
                    skipped = update_result.get("skipped_filenames", [])
                    if skipped:
                        st.caption("已跳过（上次已处理）：" + "、".join(skipped))
                else:
                    st.warning(str(update_result.get("message") or "总结更新未生效。"))
            except Exception as exc:
                st.error(f"根据工作总结更新任务状态失败：{exc}")


def _render_summary_update_result(project_id: str, tasks: list[dict], expanded: bool = True) -> None:
    summary_update_result = st.session_state.get("summary_update_result")
    if (
        not isinstance(summary_update_result, dict)
        or summary_update_result.get("project_id") != project_id
        or summary_update_result.get("status") != "ok"
    ):
        _hydrate_summary_update_result(project_id)
        summary_update_result = st.session_state.get("summary_update_result")
    if not isinstance(summary_update_result, dict):
        return
    if summary_update_result.get("project_id") != project_id:
        return
    if summary_update_result.get("status") != "ok":
        return

    with st.expander("工作总结解析汇总", expanded=expanded):
        summaries_count = int(summary_update_result.get("summaries_count", 0) or 0)
        read_at_raw = str(summary_update_result.get("read_at") or "").strip()
        read_at_display = read_at_raw.replace("T", " ") if read_at_raw else "未知"
        read_event_text = "已读取一份工作总结" if summaries_count == 1 else f"已读取 {summaries_count} 份工作总结"

        st.caption(
            f"已读取 {summaries_count} 份总结。"
        )
        st.dataframe(
            [{"读取记录": read_event_text, "读取时间": read_at_display}],
            use_container_width=True,
            hide_index=True,
        )

        aggregated_updates = summary_update_result.get("aggregated_updates", [])
        if aggregated_updates:
            task_display_name_map = _build_task_display_name_map(tasks)
            table_rows = [
                {
                    "task": task_display_name_map.get(str(item.get("task_id") or ""), str(item.get("task_id") or "")),
                    "progress_delta": item.get("progress_delta", 0),
                    "confidence": item.get("confidence", 0),
                    "evidence": item.get("evidence", ""),
                    "evidence_files": item.get("evidence_files", ""),
                    "summary_count": item.get("summary_count", 0),
                }
                for item in aggregated_updates
            ]
            st.dataframe(table_rows, use_container_width=True, hide_index=True)
        else:
            st.info("未识别到可更新的任务。")

        risk_items = summary_update_result.get("risk_items", [])
        if risk_items:
            st.markdown("#### 风险任务提示")
            for item in risk_items:
                st.warning(str(item.get("message") or "检测到风险任务。"))

            risk_table_rows = [
                {
                    "task": item.get("task", ""),
                    "expected_progress": f"{int(item.get('expected_progress', 0))}%",
                    "actual_progress": f"{int(item.get('actual_progress', 0))}%",
                    "lag_percent": f"{int(item.get('lag_percent', 0))}%",
                }
                for item in risk_items
            ]
            st.dataframe(risk_table_rows, use_container_width=True, hide_index=True)


def _format_chat_timestamp(raw_ts: str) -> str:
    text = str(raw_ts or "").strip()
    if not text:
        return ""

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return text.replace("T", " ")


def _render_assistant_panel(project_id: str) -> None:
    _hydrate_summary_state(project_id)

    assistant_state = load_project_assistant_state(project_id)
    history = assistant_state.get("chat_history", [])
    pending_change = assistant_state.get("pending_change")
    memory_updates_key = f"assistant_memory_updates_{project_id}"
    pending_user_msg_key = f"assistant_pending_user_msg_{project_id}"
    pending_user_msg_ts_key = f"assistant_pending_user_msg_ts_{project_id}"
    pending_user_msg = str(st.session_state.get(pending_user_msg_key, "") or "").strip()
    pending_user_msg_ts_raw = str(st.session_state.get(pending_user_msg_ts_key, "") or "").strip()
    pending_user_msg_ts = _format_chat_timestamp(pending_user_msg_ts_raw)

    if st.button(
        "关闭聊天窗口",
        key=f"assistant_close_inline_btn_{project_id}",
        use_container_width=False,
        type="secondary",
    ):
        st.session_state[_assistant_open_key(project_id)] = False
        st.rerun()

    st.markdown("### Assistant")
    st.caption("可自然语言提问、提出任务修改。涉及修改的操作需要你确认后才会执行。")

    if isinstance(pending_change, dict):
        pending_kind = str(pending_change.get("kind", "task_update")).strip().lower()
        if pending_kind == "batch_update":
            operations = pending_change.get("operations", []) if isinstance(pending_change.get("operations"), list) else []
            st.warning(
                "存在待执行批量修改："
                f"共 {len(operations)} 步。回复“确认执行”后才会真正落盘。"
            )
            if operations:
                with st.expander("查看批量步骤", expanded=False):
                    for idx, op in enumerate(operations, start=1):
                        st.markdown(f"{idx}. {str(op)}")
        else:
            st.warning(
                "存在待执行修改："
                f"目标={pending_change.get('task_id') or pending_change.get('task_name') or '未识别'}，"
                f"更新={pending_change.get('updates', {})}。"
                "回复“确认执行”后才会真正落盘。"
            )

    chat_box = st.container(height=460)
    with chat_box:
        for msg in history[-12:]:
            role = "assistant" if msg.get("role") == "assistant" else "user"
            with st.chat_message(role):
                st.caption("Assistant" if role == "assistant" else "You")
                st.markdown(str(msg.get("content", "")))
                ts_text = _format_chat_timestamp(msg.get("ts", ""))
                if ts_text:
                    st.caption(ts_text)
        if pending_user_msg:
            with st.chat_message("user"):
                st.caption("You")
                st.markdown(pending_user_msg)
                if pending_user_msg_ts:
                    st.caption(pending_user_msg_ts)

    summary_folder = st.session_state.get(_saved_summary_folder_key(project_id), "")
    user_msg = st.chat_input(
        "例如：把任务1.3进度改为 70%，并重排期；或：总结目前KR1的完成状态",
        key=f"assistant_chat_input_{project_id}",
    )
    if user_msg:
        st.session_state[pending_user_msg_key] = str(user_msg).strip()
        st.session_state[pending_user_msg_ts_key] = datetime.now().isoformat(timespec="seconds")
        st.rerun()

    pending_user_msg = str(st.session_state.get(pending_user_msg_key, "") or "").strip()
    if pending_user_msg:
        try:
            with st.spinner("Assistant 正在处理..."):
                result = assistant_chat(
                    project_id,
                    pending_user_msg,
                    summaries_folder=summary_folder,
                    user_message_ts=pending_user_msg_ts_raw,
                )
            st.session_state.pop(pending_user_msg_key, None)
            st.session_state.pop(pending_user_msg_ts_key, None)
            memory_updates = result.get("memory_updates", {}) if isinstance(result, dict) else {}
            if isinstance(memory_updates, dict):
                st.session_state[memory_updates_key] = {
                    "added_system": [
                        str(v).strip()
                        for v in memory_updates.get("added_system", [])
                        if str(v).strip()
                    ],
                    "added_project": [
                        str(v).strip()
                        for v in memory_updates.get("added_project", [])
                        if str(v).strip()
                    ],
                }
            snippets = result.get("summary_snippets", []) if isinstance(result, dict) else []
            if snippets:
                st.session_state[f"assistant_snippets_{project_id}"] = snippets
            st.rerun()
        except Exception as exc:
            st.session_state.pop(pending_user_msg_key, None)
            st.session_state.pop(pending_user_msg_ts_key, None)
            st.error(f"Assistant 处理失败：{exc}")
            st.exception(exc)


def main() -> None:
    st.set_page_config(page_title="Project Copilot", page_icon="🎯", layout="wide")
    inject_styles()

    # ── Sidebar ───────────────────────────────────────────────────────────────
    active_project_id = render_project_nav()
    render_llm_settings()

    # ── Guard: no projects yet ────────────────────────────────────────────────
    if active_project_id is None:
        render_header()
        st.info("左侧新建一个项目，即可开始生成 OKR 计划。")
        return

    gantt_theme = render_personalization_nav(active_project_id)
    insight_settings = render_insight_management_nav(active_project_id)

    # ── Resolve active project name ───────────────────────────────────────────
    all_projects = list_all_projects()
    active_project = next((p for p in all_projects if p["id"] == active_project_id), None)
    project_name = active_project["name"] if active_project else "项目"
    assistant_open = bool(st.session_state.get(_assistant_open_key(active_project_id), False))

    if not assistant_open:
        header_col, assistant_btn_col = st.columns([2.1, 1], gap="large")
        with header_col:
            render_header(project_name)
        with assistant_btn_col:
            _render_assistant_launcher(active_project_id, show_hint=False)

    # ── Reset session state when switching projects ───────────────────────────
    previous_project_id = st.session_state.get("_last_project_id")
    project_switched = previous_project_id is not None and previous_project_id != active_project_id

    if previous_project_id != active_project_id:
        st.session_state._last_project_id = active_project_id
        st.session_state.main_view_mode = "dashboard"
        st.session_state.summary_update_result = None
        st.session_state.pop("_gen_result", None)
        _hydrate_summary_state(active_project_id, force=True)
        _hydrate_summary_update_result(active_project_id, force=True)

    st.session_state.setdefault("summary_update_result", None)
    st.session_state.setdefault("main_view_mode", "dashboard")
    st.session_state.setdefault(_show_summary_on_dashboard_key(active_project_id), False)
    st.session_state.setdefault(_insight_feed_open_key(active_project_id), False)
    st.session_state.setdefault(_saved_insight_selected_card_key(active_project_id), "")
    st.session_state.setdefault(_saved_insight_pending_unsave_key(active_project_id), "")
    st.session_state.setdefault(_insight_pending_save_card_key(active_project_id), "")

    plan_state = load_plan_from_state(active_project_id)
    tasks = plan_state.get("tasks", [])
    dependencies = plan_state.get("dependencies", [])
    has_plan = bool(tasks)

    insight_state = load_insight_state(active_project_id)
    if has_plan:
        insight_state = sync_project_insight_today_state(active_project_id)
        if project_switched:
            _mark_insight_auto_checked(active_project_id)
        else:
            _run_daily_insight_auto_trigger(
                active_project_id,
                insight_settings,
                insight_state=insight_state,
            )

    insight_state = _maybe_show_insight_toast(active_project_id, insight_state)

    # ── Handle completed background insight job ───────────────────────────────
    done_job = _insight_pop_done(active_project_id)
    if done_job is not None:
        error = str(done_job.get("error") or "").strip()
        result = done_job.get("result")
        if error:
            st.session_state[f"insight_auto_error_{active_project_id}"] = error
            st.toast(f"Insight 生成失败：{error}", icon="⚠️")
        elif isinstance(result, dict):
            status = str(result.get("status") or "").strip().lower()
            if status == "error":
                message = str(result.get("message") or "Insight 生成失败。")
                st.session_state[f"insight_auto_error_{active_project_id}"] = message
                st.toast(f"Insight 生成失败：{message}", icon="⚠️")
            elif status == "ok":
                st.session_state[f"insight_auto_error_{active_project_id}"] = ""
                if done_job.get("open_after"):
                    st.session_state[_insight_feed_open_key(active_project_id)] = True
                    mark_insight_notification_seen(active_project_id)
                done_message = str(result.get("message") or "今日 insight 已生成。")
                st.toast(done_message, icon="✅")
                _insight_done_dialog(active_project_id)
            else:
                st.session_state[f"insight_auto_error_{active_project_id}"] = ""

            if status != "error":
                insight_state = load_insight_state(active_project_id)

    # ── Load plan state ───────────────────────────────────────────────────────
    # ── NEW PROJECT: no plan yet ──────────────────────────────────────────────
    if not has_plan:
        if assistant_open:
            main_col, assistant_col = st.columns([1, 1], gap="large")
            with main_col:
                render_header(project_name)
                _render_generate_plan_form(active_project_id)
                if st.session_state.get(_insight_feed_open_key(active_project_id), False):
                    _render_insight_feed(active_project_id)
                st.markdown("---")
                render_metrics([])
            with assistant_col:
                with st.container(key=f"assistant_panel_wrap_{active_project_id}"):
                    _render_assistant_panel(active_project_id)
        else:
            left_col, right_col = st.columns([2.1, 1], gap="large")
            with left_col:
                _render_generate_plan_form(active_project_id)
                if st.session_state.get(_insight_feed_open_key(active_project_id), False):
                    _render_insight_feed(active_project_id)
            with right_col:
                render_metrics([])
        _render_insight_fab(active_project_id, insight_state)
        return

    # ── PROJECT HAS A PLAN: route by view mode ────────────────────────────────
    view_mode = st.session_state.get("main_view_mode", "dashboard")

    # -- Regenerate mode -------------------------------------------------------
    if view_mode == "regenerate":
        if assistant_open:
            main_col, assistant_col = st.columns([1, 1], gap="large")
            with main_col:
                render_header(project_name)
                _render_generate_plan_form(active_project_id, is_regen=True)
                if st.session_state.get(_insight_feed_open_key(active_project_id), False):
                    _render_insight_feed(active_project_id)
                st.markdown("---")
                render_metrics(tasks)
                if st.button("← 返回查看计划", key="back_from_regen", use_container_width=True):
                    st.session_state.main_view_mode = "dashboard"
                    st.rerun()
            with assistant_col:
                with st.container(key=f"assistant_panel_wrap_{active_project_id}"):
                    _render_assistant_panel(active_project_id)
        else:
            left_col, right_col = st.columns([2.1, 1], gap="large")
            with left_col:
                _render_generate_plan_form(active_project_id, is_regen=True)
                if st.session_state.get(_insight_feed_open_key(active_project_id), False):
                    _render_insight_feed(active_project_id)
            with right_col:
                render_metrics(tasks)
                st.markdown("---")
                if st.button("← 返回查看计划", key="back_from_regen", use_container_width=True):
                    st.session_state.main_view_mode = "dashboard"
                    st.rerun()
        _render_insight_fab(active_project_id, insight_state)
        return

    # -- Update summary mode ---------------------------------------------------
    if view_mode == "update_summary":
        if assistant_open:
            main_col, assistant_col = st.columns([1, 1], gap="large")
            with main_col:
                render_header(project_name)
                _render_update_summary_form(active_project_id)
                _render_summary_update_result(active_project_id, tasks, expanded=True)
                if st.session_state.get(_insight_feed_open_key(active_project_id), False):
                    _render_insight_feed(active_project_id)
                st.markdown("---")
                render_metrics(tasks)
                if st.button("← 返回查看计划", key="back_from_summary", use_container_width=True):
                    st.session_state.main_view_mode = "dashboard"
                    st.rerun()
            with assistant_col:
                with st.container(key=f"assistant_panel_wrap_{active_project_id}"):
                    _render_assistant_panel(active_project_id)
        else:
            left_col, right_col = st.columns([2.1, 1], gap="large")
            with left_col:
                _render_update_summary_form(active_project_id)
                _render_summary_update_result(active_project_id, tasks, expanded=True)
                if st.session_state.get(_insight_feed_open_key(active_project_id), False):
                    _render_insight_feed(active_project_id)
            with right_col:
                render_metrics(tasks)
                st.markdown("---")
                if st.button("← 返回查看计划", key="back_from_summary", use_container_width=True):
                    st.session_state.main_view_mode = "dashboard"
                    st.rerun()
        _render_insight_fab(active_project_id, insight_state)
        return

    # -- Saved insight cards mode ---------------------------------------------
    if view_mode == "saved_insight_cards":
        if assistant_open:
            main_col, assistant_col = st.columns([1, 1], gap="large")
            with main_col:
                render_header(project_name)
                _render_saved_insight_cards_view(active_project_id)
                if st.session_state.get(_insight_feed_open_key(active_project_id), False):
                    _render_insight_feed(active_project_id)
                st.markdown("---")
                render_metrics(tasks)
                if st.button("← 返回查看计划", key="back_from_saved_insight", use_container_width=True):
                    st.session_state.main_view_mode = "dashboard"
                    st.rerun()
            with assistant_col:
                with st.container(key=f"assistant_panel_wrap_{active_project_id}"):
                    _render_assistant_panel(active_project_id)
        else:
            left_col, right_col = st.columns([2.1, 1], gap="large")
            with left_col:
                _render_saved_insight_cards_view(active_project_id)
                if st.session_state.get(_insight_feed_open_key(active_project_id), False):
                    _render_insight_feed(active_project_id)
            with right_col:
                render_metrics(tasks)
                st.markdown("---")
                if st.button("← 返回查看计划", key="back_from_saved_insight", use_container_width=True):
                    st.session_state.main_view_mode = "dashboard"
                    st.rerun()
        _render_insight_fab(active_project_id, insight_state)
        return

    # -- Dashboard mode --------------------------------------------------------
    show_summary_on_dashboard = bool(st.session_state.get(_show_summary_on_dashboard_key(active_project_id), False))

    right_panel_open = bool(st.session_state.get(_right_panel_open_key(active_project_id), True))
    if right_panel_open:
        if assistant_open:
            left_col, right_col = st.columns([1, 1], gap="large")
        else:
            left_col, right_col = st.columns([2.1, 1], gap="large")
    else:
        left_col = st.container()
        right_col = None

    with left_col:
        toggle_spacer_col, toggle_btn_col = st.columns([6, 1], gap="small")
        with toggle_btn_col:
            _render_right_panel_toggle(active_project_id)

        if assistant_open:
            render_header(project_name)
            render_metrics(tasks)

            gen_result = st.session_state.get("_gen_result")
            if gen_result:
                st.success(f"已生成 {gen_result['count']} 个任务")
                total = gen_result.get("total_tokens", 0)
                logical_calls = int(gen_result.get("logical_call_count", gen_result.get("request_count", 0)) or 0)
                provider_responses = int(gen_result.get("request_count", 0) or 0)
                empty_retries = int(
                    gen_result.get(
                        "empty_response_retry_count",
                        max(0, provider_responses - logical_calls),
                    )
                    or 0
                )
                calls_text = f"calls={logical_calls}"
                if empty_retries > 0 or provider_responses != logical_calls:
                    calls_text += f", retries={empty_retries}, provider_responses={provider_responses}"
                if total > 0:
                    st.caption(
                        f"Token 消耗: total={total:,} "
                        f"(prompt={gen_result['prompt_tokens']:,}, "
                        f"completion={gen_result['completion_tokens']:,}, "
                        f"{calls_text})"
                    )
                else:
                    st.caption("Token 消耗: 当前 Provider 未返回 usage 统计。")

            summary_result = st.session_state.get("summary_update_result")
            if show_summary_on_dashboard and isinstance(summary_result, dict) and summary_result.get("project_id") == active_project_id:
                if summary_result.get("status") == "ok":
                    st.success(
                        f"总结已更新：{int(summary_result.get('summaries_count', 0))} 份总结，"
                        f"{int(summary_result.get('changed_tasks_count', 0))} 个任务变更"
                    )
                    risk_count = int(summary_result.get("risk_count", 0) or 0)
                    if risk_count > 0:
                        st.warning(f"发现 {risk_count} 个风险任务（进度落后 >= {int(summary_result.get('risk_lag_threshold', 30))}%）。")

        if show_summary_on_dashboard:
            _render_summary_update_result(active_project_id, tasks, expanded=True)

        if st.session_state.get(_insight_feed_open_key(active_project_id), False):
            _render_insight_feed(active_project_id)

        tab_timeline, tab_graph, tab_table = st.tabs(["甘特图", "依赖图", "任务表"])

        with tab_timeline:
            objective = _extract_objective(tasks)
            timeline_rows = _build_timeline_rows(tasks)
            gantt_payload = render_gantt(timeline_rows, objective=objective, theme=gantt_theme)
            gantt_export_html = _wrap_export_html(gantt_payload["html"], f"{project_name} - Timeline")
            gantt_file = f"{_safe_file_stem(project_name)}_timeline.html"
            st.download_button(
                "下载甘特图 (HTML)",
                data=gantt_export_html.encode("utf-8"),
                file_name=gantt_file,
                mime="text/html",
                key=f"download_gantt_html_{active_project_id}",
            )
            components.html(gantt_payload["html"], height=gantt_payload["height"], scrolling=True)

        with tab_graph:
            graph_payload = render_graph(tasks, dependencies)
            graph_png = _dependency_svg_to_png_bytes(
                str(graph_payload.get("svg", "")),
                int(graph_payload.get("svg_width", 0) or 0),
                int(graph_payload.get("svg_height", 0) or 0),
            )
            graph_file = f"{_safe_file_stem(project_name)}_dependency_graph.png"
            if graph_png:
                st.download_button(
                    "下载依赖图 (PNG)",
                    data=graph_png,
                    file_name=graph_file,
                    mime="image/png",
                    key=f"download_graph_png_{active_project_id}",
                )
            elif not _has_cairosvg():
                st.info("当前环境未安装 cairosvg，暂无法导出依赖图 PNG。")
            else:
                st.info("依赖图 PNG 正在生成中或当前图无可导出的 SVG 内容。")
            components.html(graph_payload["html"], height=graph_payload["height"], scrolling=True)

        with tab_table:
            table_rows = _build_table_rows(tasks)
            st.dataframe(table_rows, use_container_width=True, hide_index=True)

    if right_col is not None:
        with right_col:
            if assistant_open:
                with st.container(key=f"assistant_panel_wrap_{active_project_id}"):
                    _render_assistant_panel(active_project_id)
            else:
                render_metrics(tasks)
                st.markdown("---")
                if st.button("改变本项目的OKR，重新生成计划", key="btn_goto_regen", use_container_width=True):
                    st.session_state.main_view_mode = "regenerate"
                    st.rerun()
                st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
                if st.button("根据团队工作总结，更新任务状态", key="btn_goto_summary", use_container_width=True):
                    st.session_state.main_view_mode = "update_summary"
                    st.rerun()
                st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
                if st.button("已保存的 Insight Card", key="btn_goto_saved_insight", use_container_width=True):
                    st.session_state.main_view_mode = "saved_insight_cards"
                    st.rerun()
                st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
                manual_generation_ready = _insight_generation_ready(insight_settings)
                insight_running = _insight_is_running(active_project_id)
                manual_btn_label = "⏳ Insight 生成中..." if insight_running else "手动生成今日 Insight"
                if st.button(
                    manual_btn_label,
                    key="btn_manual_insight",
                    use_container_width=True,
                    disabled=(not manual_generation_ready) or insight_running,
                ):
                    _generate_insight_and_refresh(active_project_id, open_after_generate=True)
                if not manual_generation_ready:
                    st.caption(_insight_generation_disabled_message(insight_settings))

                gen_result = st.session_state.get("_gen_result")
                if gen_result:
                    st.success(f"已生成 {gen_result['count']} 个任务")
                    total = gen_result.get("total_tokens", 0)
                    logical_calls = int(gen_result.get("logical_call_count", gen_result.get("request_count", 0)) or 0)
                    provider_responses = int(gen_result.get("request_count", 0) or 0)
                    empty_retries = int(
                        gen_result.get(
                            "empty_response_retry_count",
                            max(0, provider_responses - logical_calls),
                        )
                        or 0
                    )
                    calls_text = f"calls={logical_calls}"
                    if empty_retries > 0 or provider_responses != logical_calls:
                        calls_text += f", retries={empty_retries}, provider_responses={provider_responses}"
                    if total > 0:
                        st.caption(
                            f"Token 消耗: total={total:,} "
                            f"(prompt={gen_result['prompt_tokens']:,}, "
                            f"completion={gen_result['completion_tokens']:,}, "
                            f"{calls_text})"
                        )
                    else:
                        st.caption("Token 消耗: 当前 Provider 未返回 usage 统计。")

                summary_result = st.session_state.get("summary_update_result")
                if show_summary_on_dashboard and isinstance(summary_result, dict) and summary_result.get("project_id") == active_project_id:
                    if summary_result.get("status") == "ok":
                        st.success(
                            f"总结已更新：{int(summary_result.get('summaries_count', 0))} 份总结，"
                            f"{int(summary_result.get('changed_tasks_count', 0))} 个任务变更"
                        )
                        risk_count = int(summary_result.get("risk_count", 0) or 0)
                        if risk_count > 0:
                            st.warning(f"发现 {risk_count} 个风险任务（进度落后 >= {int(summary_result.get('risk_lag_threshold', 30))}%）。")

    _render_insight_fab(active_project_id, insight_state)

    # ── Auto-poll while background insight generation is in progress ──────────
    # The page has already been rendered above; a 2-second sleep + rerun keeps
    # the status indicator fresh without blocking any UI element.
    if _insight_is_running(active_project_id):
        time.sleep(2)
        st.rerun()


if __name__ == "__main__":
    main()