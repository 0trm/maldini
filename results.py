"""
results.py -- fetch finished match results from TheSportsDB and match them to
extracted predictions via fuzzy team-name matching + date windowing.

Aggregate (two-leg) resolution is attempted for knockout predictions in
UEFA Champions League / Europa League where TheSportsDB lists both legs.
"""

from __future__ import annotations

import re
import time
from datetime import date, datetime
from typing import Optional

import requests

# ── Lookup tables ──────────────────────────────────────────────────────────────
COMPETITION_MAP = {
    "LaLiga":                    4335,
    "UEFA Champions League":     4480,
    "UEFA Europa League":        4481,
    "Copa del Rey":              4483,
    "UEFA Nations League":       4490,
    "Mundial":                   4429,
    "Copa América":              4499,
    "Eurocopa":                  4502,
    "Copa de África":            4496,
    "Supercopa de España":       4511,
    "Supercopa de Europa":       4512,
    "Mundial de Clubes":         4503,
}

SINGLE_YEAR_SEASON = {"Mundial", "Copa América", "Eurocopa", "Copa de África", "Mundial de Clubes"}

SEASON_OVERRIDES: dict[tuple[str, str], str] = {
    ("Eurocopa", "2021"): "2020",
}

MAX_ROUNDS = {
    "LaLiga":                38,
    "UEFA Champions League": 20,
    "UEFA Europa League":    20,
    "Copa del Rey":          10,
    "UEFA Nations League":   10,
    "Mundial":               10,
    "Copa América":          10,
    "Eurocopa":              10,
    "Copa de África":        10,
    "Supercopa de España":   3,
    "Supercopa de Europa":   1,
    "Mundial de Clubes":     10,
}

AGGREGATE_COMPETITIONS = {"UEFA Champions League", "UEFA Europa League"}

API_BASE   = "https://www.thesportsdb.com/api/v1/json/3"
RATE_LIMIT = 2


# ── Fuzzy team-name matching ──────────────────────────────────────────────────
_NORMALISE = {
    "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u",
    "ñ": "n", "ü": "u", "ç": "c",
}

WORD_SUBS = {
    "marsella":   "marseille",
    "copenhague": "copenhagen",
    "napoles":    "napoli",
    "praga":      "prague",
    "francfort":  "frankfurt",
    "brujas":     "brugge",
    "carabaj":    "qarabag",
    "karabaj":    "qarabag",
    "eslavia":    "slavia",
    "egipto":     "egypt",
    "tunez":      "tunisia",
    "marruecos":  "morocco",
    "sudafrica":  "south africa",
    "camerun":    "cameroon",
    "argelia":    "algeria",
    "benin":      "benin",
    "costa de marfil":                 "ivory coast",
    "republica democratica del congo": "dr congo",
}

TEAM_ALIASES = {
    "sporting portugal":    "sporting cp",
    "union san giluas":     "union saintgilloise",
    "union san giloise":    "union saintgilloise",
    "union san gilas":      "union saintgilloise",
    "union san gjilas":     "union saintgilloise",
    "union saintgilloise":  "union saintgilloise",
    "union saint gilloise": "union saintgilloise",
}

_WORD_SUBS_SORTED = sorted(WORD_SUBS.items(), key=lambda kv: len(kv[0]), reverse=True)


def normalise(s: str) -> str:
    s = s.lower().strip()
    for k, v in _NORMALISE.items():
        s = s.replace(k, v)
    s = re.sub(r"[^a-z0-9 ]", "", s)
    for src, dst in _WORD_SUBS_SORTED:
        s = s.replace(src, dst)
    s = re.sub(r"\b(de|del)\b", "", s)
    s = re.sub(r" +", " ", s).strip()
    s = re.sub(r"^(real |atletico |club |rc |fc |sd |cd )", "", s)
    return s.strip()


def teams_match(pred_name: str, api_name: str) -> bool:
    pn = TEAM_ALIASES.get(normalise(pred_name), normalise(pred_name))
    an = TEAM_ALIASES.get(normalise(api_name), normalise(api_name))
    return pn == an or pn in an or an in pn


# ── Season inference ──────────────────────────────────────────────────────────
def infer_season(comp: str, date_str: str) -> str:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        d = date.today()

    if comp in SINGLE_YEAR_SEASON:
        season = str(d.year)
    elif d.month >= 8:
        season = f"{d.year}-{d.year + 1}"
    else:
        season = f"{d.year - 1}-{d.year}"

    return SEASON_OVERRIDES.get((comp, season), season)


# ── API helpers ───────────────────────────────────────────────────────────────
def api_get(endpoint: str) -> Optional[dict]:
    url = f"{API_BASE}/{endpoint}"
    resp = requests.get(url, timeout=10)
    if resp.status_code == 200:
        return resp.json()
    if resp.status_code == 429:
        print("  [RATE LIMIT] sleeping 60s...")
        time.sleep(60)
        return api_get(endpoint)
    print(f"  [API ERROR] {resp.status_code} -- {url}")
    return None


def fetch_matches_for_competition(league_id: int, season: str, comp: str) -> list[dict]:
    all_finished = []
    max_r = MAX_ROUNDS.get(comp, 10)
    empty_streak = 0

    for r in range(1, max_r + 1):
        data = api_get(f"eventsround.php?id={league_id}&r={r}&s={season}")
        time.sleep(RATE_LIMIT)

        if data is None:
            empty_streak += 1
            if empty_streak >= 3:
                break
            continue

        events = data.get("events") or []
        if not events:
            empty_streak += 1
            if empty_streak >= 3:
                break
            continue

        empty_streak = 0
        finished = [e for e in events if e.get("strStatus") == "Match Finished"]
        all_finished.extend(finished)

        if not finished and events:
            break

    return all_finished


# ── Matching logic ────────────────────────────────────────────────────────────
def get_result_code(home_goals: int, away_goals: int) -> str:
    if home_goals > away_goals:
        return "H"
    if home_goals < away_goals:
        return "A"
    return "D"


def match_prediction_to_result(pred: dict, api_matches: list[dict]) -> Optional[dict]:
    home = pred["home_team"]
    away = pred["away_team"]
    match_date   = str(pred.get("match_date") or "")
    publish_date = str(pred.get("publish_date") or "")

    for m in api_matches:
        api_home = m["strHomeTeam"]
        api_away = m["strAwayTeam"]

        if not (teams_match(home, api_home) and teams_match(away, api_away)):
            continue

        api_date = m.get("dateEvent", "")

        if match_date:
            try:
                d_pred = datetime.strptime(match_date, "%Y-%m-%d").date()
                d_api  = datetime.strptime(api_date, "%Y-%m-%d").date()
                if abs((d_pred - d_api).days) > 1:
                    continue
            except ValueError:
                pass
        elif publish_date and api_date:
            try:
                d_pub = datetime.strptime(publish_date, "%Y-%m-%d").date()
                d_api = datetime.strptime(api_date, "%Y-%m-%d").date()
                if d_api < d_pub or (d_api - d_pub).days > 45:
                    continue
            except ValueError:
                pass

        home_goals = m.get("intHomeScore")
        away_goals = m.get("intAwayScore")
        if home_goals is None or away_goals is None:
            continue

        try:
            home_goals = int(home_goals)
            away_goals = int(away_goals)
        except (ValueError, TypeError):
            continue

        return {
            "home_goals":     home_goals,
            "away_goals":     away_goals,
            "actual_result":  get_result_code(home_goals, away_goals),
            "match_date":     m.get("dateEvent") or match_date,
        }

    return None


def match_aggregate_to_result(pred: dict, api_matches: list[dict]) -> Optional[dict]:
    home = pred["home_team"]
    away = pred["away_team"]

    leg1 = None
    leg2 = None

    for m in api_matches:
        ah = m["strHomeTeam"]
        aa = m["strAwayTeam"]
        if leg1 is None and teams_match(home, ah) and teams_match(away, aa):
            leg1 = m
        elif leg2 is None and teams_match(away, ah) and teams_match(home, aa):
            leg2 = m
        if leg1 and leg2:
            break

    if leg1 is None or leg2 is None:
        return None

    try:
        l1_home = int(leg1["intHomeScore"])
        l1_away = int(leg1["intAwayScore"])
        l2_home = int(leg2["intHomeScore"])
        l2_away = int(leg2["intAwayScore"])
    except (ValueError, TypeError):
        return None

    team1_total = l1_home + l2_away
    team2_total = l1_away + l2_home

    if team1_total > team2_total:
        result = "H"
    elif team2_total > team1_total:
        result = "A"
    else:
        # Tied on aggregate (ET/penalties): cannot determine from goals alone.
        # Caller should fall back to data/results_overrides.csv.
        print(f"  [REVIEW] {home} vs {away} tied on aggregate {team1_total}-{team2_total} "
              "(ET/penalties). Use data/results_overrides.csv to record the winner.")
        return None

    return {
        "home_goals":    team1_total,
        "away_goals":    team2_total,
        "actual_result": result,
        "match_date":    leg2.get("dateEvent", "") or "",
    }


def fetch_result(pred: dict, api_matches: list[dict]) -> Optional[dict]:
    """
    Resolve a prediction to a finished-match result. For knockout predictions in
    UCL/UEL we attempt aggregate matching first; otherwise single-leg only.
    """
    is_knockout = pred.get("match_type") == "knockout"
    is_aggregate_candidate = is_knockout and pred.get("competition") in AGGREGATE_COMPETITIONS

    if is_aggregate_candidate:
        agg = match_aggregate_to_result(pred, api_matches)
        if agg is not None:
            return agg
    return match_prediction_to_result(pred, api_matches)
