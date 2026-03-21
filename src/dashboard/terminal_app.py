"""
terminal_app.py — Streamlit dashboard for Maldini Stats.

Reads from BigQuery mart tables (dbt-managed).

Run with:
    streamlit run src/dashboard/terminal_app.py
"""

import html as _html
import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
from report_core import load, build_translations, build_sections

st.set_page_config(
    page_title="maldini-stats",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── toggles (top-right, flat 3-column) ───────────────────────────────────────
_, _c_lang, _c_dark = st.columns([8, 1, 1])
with _c_lang:
    _lang = st.radio("lang", ["EN", "ES"], horizontal=True, label_visibility="collapsed")
with _c_dark:
    _dark_icon = "𖤓" if st.session_state.get("_dark", False) else "☾"
    dark = st.toggle(_dark_icon, value=False, key="_dark")

lang  = "es" if _lang == "ES" else "en"
BG    = "#111111" if dark else "#ffffff"
FG    = "#dddddd" if dark else "#111111"
BLUE  = "#4499ff" if dark else "#0033cc"
GREEN = "#44AAFF" if dark else "#0077BB"   # blue    (CB-safe)
AMBER = "#FFAA44" if dark else "#EE7733"   # orange  (CB-safe)
RED   = "#EE66AA" if dark else "#AA3377"   # magenta (CB-safe)

st.markdown(
    f"""
    <style>
    html, body, .stApp,
    [data-testid="stAppViewContainer"],
    [data-testid="stHeader"],
    [data-testid="stToolbar"],
    [data-testid="stDecoration"],
    [data-testid="stBottom"],
    section[data-testid="stSidebar"] {{
        background-color: {BG} !important;
    }}
    * {{
        font-family: 'Courier New', Courier, monospace !important;
        color: {FG};
    }}
    #MainMenu, footer, header {{ visibility: hidden; }}
    .block-container {{
        padding-top: 0.5rem !important;
        padding-bottom: 2rem !important;
        padding-left: 3rem !important;
        padding-right: 3rem !important;
    }}
    [data-testid="stVerticalBlock"] {{ gap: 0 !important; }}
    section[data-testid="stSidebar"] * {{ color: {FG}; }}
    .doc {{
        font-family: 'Courier New', Courier, monospace;
        font-size: 13px;
        line-height: 1.4;
        color: {FG};
        white-space: pre;
        display: block;
        width: fit-content;
        margin: 0;
        padding: 0 0 1.15em 0;
    }}
    .doc b  {{ font-weight: bold; color: {FG}; }}
    .muted  {{ color: {BLUE}; }}
    .hl {{
        display: inline-block;
        border-left: 3px solid {GREEN};
        background-color: {"rgba(68,170,255,0.08)" if dark else "rgba(0,119,187,0.07)"};
        padding-left: 6px;
        margin-left: -6px;
    }}
    [data-testid="stToggle"] label,
    [data-testid="stToggle"] p {{
        font-family: 'Courier New', Courier, monospace !important;
        font-size: 25px !important;
        color: {BLUE} !important;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)


# ── HTML formatting adapter ──────────────────────────────────────────────────
_COLORS = {"blue": GREEN, "magenta": RED, "orange": AMBER}


class _HtmlFmt:
    lt = "&lt;"
    gt = "&gt;"

    def bold(self, text):
        return f"<b>{text}</b>"

    def underline(self, text):
        return f"<u>{text}</u>"

    def color(self, text, name):
        return f'<span style="color:{_COLORS[name]}">{text}</span>'

    def muted(self, text):
        return f'<span class="muted">{text}</span>'

    def highlight(self, text):
        return f'<span class="hl">{text}</span>'

    def escape(self, text):
        return _html.escape(str(text))


# ── render ────────────────────────────────────────────────────────────────────
def render(lines: list[str]) -> None:
    st.markdown(
        '<div class="doc">' + "<br>".join(lines) + "</div>",
        unsafe_allow_html=True,
    )


# ── BigQuery credentials ──────────────────────────────────────────────────────
def _bq_credentials():
    """Return service account credentials from st.secrets, or None (→ ADC locally)."""
    try:
        from google.oauth2 import service_account
        info = dict(st.secrets["gcp_service_account"])
        return service_account.Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
    except (KeyError, FileNotFoundError):
        return None  # fall back to Application Default Credentials


# ── data (cached) ────────────────────────────────────────────────────────────
@st.cache_data(ttl=600)
def cached_load():
    return load(credentials=_bq_credentials())


# ── build & render report ─────────────────────────────────────────────────────
data = cached_load()
df_scored, avg_brier, std_brier, accuracy, is_sf, monthly, comp, total = data

fmt = _HtmlFmt()
TR  = build_translations(avg_brier, std_brier, fmt)
T   = TR[lang]

# fish emoji needs the larger font-size wrapper in Streamlit
sections = build_sections(T, fmt, *data)
sections["footer"][-1] = '<span style="font-size:1.8em;">\U0001f41f</span>'

for name in ("header", "summary", "competition", "quarterly",
             "recent", "definitions", "footer"):
    render(sections[name])
