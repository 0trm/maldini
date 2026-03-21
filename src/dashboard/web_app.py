"""
web_app.py — FastAPI dashboard for Maldini Stats.

Reads from BigQuery mart tables (dbt-managed).

Run with:
    uvicorn src.dashboard.web_app:app --reload
"""

import html
import json
import os
import sys
import time
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

sys.path.insert(0, str(Path(__file__).resolve().parent))
from report_core import load, build_translations, build_sections

app = FastAPI()
templates = Jinja2Templates(
    directory=Path(__file__).parent / "templates",
    autoescape=False,
)

# Simple time-based cache (10 min TTL)
_cache: dict = {"data": None, "ts": 0.0}


_SA_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
_SECRET_FILE = Path("/etc/secrets/maldinia-113f2b9ce29a.json")


def _get_credentials():
    from google.oauth2 import service_account
    # Option 1: env var (paste JSON as string)
    raw = os.environ.get("GCP_SA_JSON")
    if raw:
        return service_account.Credentials.from_service_account_info(
            json.loads(raw), scopes=_SA_SCOPES
        )
    # Option 2: Render secret file
    if _SECRET_FILE.exists():
        return service_account.Credentials.from_service_account_file(
            str(_SECRET_FILE), scopes=_SA_SCOPES
        )
    # Fall back to Application Default Credentials (local dev)
    return None


def get_data():
    if time.time() - _cache["ts"] > 600:
        _cache["data"] = load(credentials=_get_credentials())
        _cache["ts"] = time.time()
    return _cache["data"]


class _WebFmt:
    lt = "&lt;"
    gt = "&gt;"

    def bold(self, text):
        return f"<b>{text}</b>"

    def underline(self, text):
        return f"<u>{text}</u>"

    def color(self, text, name):
        return f'<span class="c-{name}">{text}</span>'

    def muted(self, text):
        return f'<span class="muted">{text}</span>'

    def highlight(self, text):
        return f'<span class="hl">{text}</span>'

    def escape(self, text):
        return html.escape(str(text))


@app.get("/", response_class=HTMLResponse)
async def report(request: Request, lang: str = "es"):
    data = get_data()
    _, avg_brier, std_brier, *_ = data
    fmt = _WebFmt()
    TR = build_translations(avg_brier, std_brier, fmt)
    T = TR[lang if lang in ("en", "es") else "en"]
    sections = build_sections(T, fmt, *data)
    return templates.TemplateResponse("report.html", {
        "request": request,
        "sections": sections,
        "lang": lang,
    })
