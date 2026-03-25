"""
cli_app.py — CLI terminal report for Maldini Stats.

Reads from BigQuery mart tables (dbt-managed) and prints a formatted
report using ANSI escape codes for colors and styling.

Run with:
    python src/dashboard/cli_app.py [--lang es] [--no-color]
"""

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from report_core import load, build_translations, build_sections


# ── ANSI formatting adapter ──────────────────────────────────────────────────
class _AnsiFmt:
    """ANSI escape-code formatter. Respects NO_COLOR and --no-color."""

    def __init__(self, enabled: bool = True):
        self._on = enabled

    def _esc(self, code: str) -> str:
        return f"\033[{code}m" if self._on else ""

    _COLORS = {
        "blue":    "38;5;33",    # ~#0077BB
        "orange":  "38;5;127",   # → magenta
        "magenta": "38;5;127",   # ~#AA3377
    }

    lt = "<"
    gt = ">"

    def bold(self, text):
        return f"{self._esc('1')}{text}{self._esc('0')}"

    def underline(self, text):
        return f"{self._esc('4')}{text}{self._esc('0')}"

    def color(self, text, name):
        return f"{self._esc(self._COLORS[name])}{text}{self._esc('0')}"

    def muted(self, text):
        return f"{self._esc('2')}{text}{self._esc('0')}"

    def highlight(self, text):
        return f"{self._esc(self._COLORS['blue'])}▎{self._esc('0')} {text}"

    def escape(self, text):
        return str(text)


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Maldini Stats CLI report")
    parser.add_argument("--lang", choices=["en", "es"], default="en")
    parser.add_argument("--no-color", action="store_true",
                        help="Disable ANSI colors")
    args = parser.parse_args()

    use_color = (not args.no_color
                 and "NO_COLOR" not in os.environ
                 and sys.stdout.isatty())

    fmt  = _AnsiFmt(enabled=use_color)
    data = load()
    df_scored, avg_brier, std_brier, accuracy, is_sf, monthly, comp, total, all_brier = data

    TR = build_translations(avg_brier, std_brier, fmt)
    T  = TR[args.lang]

    sections = build_sections(T, fmt, *data)

    for name in ("header", "summary", "distribution", "competition", "quarterly",
                 "recent", "definitions", "footer"):
        for line in sections[name]:
            print(line)
        print()


if __name__ == "__main__":
    main()
