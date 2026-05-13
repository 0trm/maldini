"""
extract.py -- Claude Haiku prediction extraction.

Given a transcript, returns structured prediction rows. No database I/O;
match_type is classified via scoring.classify_match_type (knockout when
pred_draw_pct == 0, else single).
"""

from __future__ import annotations

import json
import re
from datetime import date

import anthropic

from scoring import classify_match_type

SYSTEM_PROMPT = """You are a precise data extractor. Your job is to find football match predictions
in Spanish-language transcripts from the YouTube channel "Mundo Maldini".

Maldini always states predictions as percentages for three outcomes: home win, draw, away win.
Sometimes there is no draw (knockout phases, second legs) -- in that case pred_draw_pct is 0.

Rules:
- Only extract predictions that have explicit percentage numbers.
- Ignore vague statements without percentages.
- Team names: use the canonical Spanish name as spoken (e.g. "Athletic Club", "Barca", "Real Madrid").
- competition: infer from context. One of: LaLiga, UEFA Champions League, UEFA Europa League, Copa del Rey,
  Supercopa de Espana, Supercopa de Europa, Mundial de Clubes, Mundial, UEFA Nations League,
  Copa America, Eurocopa, Copa de Africa. Use "Unknown" if unclear.
- match_date: extract if mentioned in the transcript (YYYY-MM-DD). Leave empty string if not mentioned.
- raw_quote: the verbatim sentence(s) containing the percentages.
- pred_sum: sum of the three percentages. pred_sum_ok: true if pred_sum == 100.

Return ONLY valid JSON -- an array of prediction objects. No markdown, no explanation.
Schema per object:
{
  "match_date": "",
  "home_team": "",
  "away_team": "",
  "competition": "",
  "pred_home_win_pct": 0,
  "pred_draw_pct": 0,
  "pred_away_win_pct": 0,
  "raw_quote": ""
}"""


def extract_predictions_from_transcript(
    client: anthropic.Anthropic,
    video_id: str,
    transcript: str,
) -> list[dict]:
    transcript_text = transcript[:50_000]

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"Extract all match predictions from this transcript:\n\n{transcript_text}",
            }
        ],
    )

    raw = message.content[0].text.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    try:
        predictions = json.loads(raw)
        if not isinstance(predictions, list):
            print(f"  [WARN] video {video_id}: expected list, got {type(predictions)}")
            return []
        return predictions
    except json.JSONDecodeError as e:
        print(f"  [ERROR] video {video_id}: JSON parse failed -- {e}")
        print(f"  Raw response: {raw[:300]}")
        return []


def build_prediction_rows(
    video_id: str,
    video_url: str,
    raw_preds: list[dict],
    publish_date=None,
) -> list[dict]:
    rows = []
    for i, pred in enumerate(raw_preds):
        home_pct = int(pred.get("pred_home_win_pct", 0))
        draw_pct = int(pred.get("pred_draw_pct", 0))
        away_pct = int(pred.get("pred_away_win_pct", 0))
        pred_sum = home_pct + draw_pct + away_pct

        rows.append({
            "prediction_id":     f"{video_id}_{i:03d}",
            "video_id":          video_id,
            "video_url":         video_url,
            "publish_date":      publish_date,
            "match_date":        pred.get("match_date") or None,
            "home_team":         pred.get("home_team", ""),
            "away_team":         pred.get("away_team", ""),
            "competition":       pred.get("competition", "Unknown"),
            "match_type":        classify_match_type(draw_pct),
            "leg_number":        None,
            "pred_home_win_pct": home_pct,
            "pred_draw_pct":     draw_pct,
            "pred_away_win_pct": away_pct,
            "pred_sum":          pred_sum,
            "pred_sum_ok":       pred_sum == 100,
            "raw_quote":         pred.get("raw_quote", ""),
            "extracted_at":      date.today().isoformat(),
        })
    return rows
