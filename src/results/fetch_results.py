"""
fetch_results.py
----------------
Reads pending predictions from BigQuery (predictions with no matching row
in raw.match_results), fetches real match results from TheSportsDB,
and inserts scored rows into raw.match_results.

Brier scores are computed downstream by dbt (fct_predictions model).

API docs: https://www.thesportsdb.com/documentation

Usage:
    python src/results/fetch_results.py
    python src/results/fetch_results.py --dry-run
    python src/results/fetch_results.py --competition "LaLiga"
"""

import re
import argparse
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

PROJECT = "maldinia"

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

# Some competitions use a different season key in TheSportsDB than what infer_season produces
SEASON_OVERRIDES: dict[tuple[str, str], str] = {
    ("Eurocopa", "2021"): "2020",  # Euro 2020 was played in 2021 but listed as "2020" in TheSportsDB
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

API_BASE   = "https://www.thesportsdb.com/api/v1/json/3"
RATE_LIMIT = 2


# ── Fuzzy team name matching ──────────────────────────────────────────────────
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
    url  = f"{API_BASE}/{endpoint}"
    resp = requests.get(url, timeout=10)
    if resp.status_code == 200:
        return resp.json()
    elif resp.status_code == 429:
        print(f"  [RATE LIMIT] sleeping 60s…")
        time.sleep(60)
        return api_get(endpoint)
    else:
        print(f"  [API ERROR] {resp.status_code} — {url}")
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
    elif home_goals < away_goals:
        return "A"
    else:
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
            # No match_date recorded: the game must be on or after the video
            # publish date (Maldini predicts upcoming matches), and within 21
            # days to avoid grabbing a different fixture of the same teams.
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
            "actual_home_goals": home_goals,
            "actual_away_goals": away_goals,
            "actual_result":     get_result_code(home_goals, away_goals),
            "match_date":        m.get("dateEvent") or match_date,
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
        # Tied on aggregate (ET/penalties) — cannot determine from goals alone
        print(f"  [REVIEW] {home} vs {away} tied on aggregate {team1_total}-{team2_total} "
              f"(ET/penalties). Skipping — insert manually into raw.match_results.")
        return None

    return {
        "actual_home_goals": team1_total,
        "actual_away_goals": team2_total,
        "actual_result":     result,
        "match_date":        leg2.get("dateEvent", ""),
    }


# ── BigQuery I/O ──────────────────────────────────────────────────────────────
def load_pending_predictions(bq: bigquery.Client, competition_filter: Optional[str]) -> list[dict]:
    comp_clause = f"AND p.competition = '{competition_filter}'" if competition_filter else ""
    query = f"""
        SELECT p.*
        FROM `{PROJECT}.raw.predictions_extracted` p
        LEFT JOIN `{PROJECT}.raw.match_results` r ON p.prediction_id = r.prediction_id
        WHERE r.prediction_id IS NULL
        {comp_clause}
    """
    return [dict(row) for row in bq.query(query).result()]


def insert_results_to_bq(bq: bigquery.Client, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    df["match_date"] = pd.to_datetime(df["match_date"], errors="coerce").dt.date
    df["fetched_at"] = datetime.now(timezone.utc)

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = bq.load_table_from_dataframe(
        df, f"{PROJECT}.raw.match_results", job_config=job_config
    )
    job.result()
    print(f"✓ Inserted {job.output_rows} row(s) → raw.match_results")


def print_pending_table(rows: list[dict]) -> None:
    today_str = date.today().isoformat()
    past = [r for r in rows if str(r.get("match_date") or r.get("publish_date", "9999")) < today_str]
    if not past:
        print("✓ No pending predictions with past dates.")
        return
    fmt = "{:<12}  {:<22}  {:<28}  {:<28}  {:<10}  {}"
    print(fmt.format("match_date", "competition", "home_team", "away_team", "match_type", "prediction_id"))
    print("-" * 120)
    for r in past:
        print(fmt.format(
            str(r.get("match_date") or r.get("publish_date", "")),
            r["competition"][:22],
            r["home_team"][:28],
            r["away_team"][:28],
            str(r.get("match_type", "")),
            r["prediction_id"],
        ))


def main():
    parser = argparse.ArgumentParser(description="Fetch match results for pending predictions.")
    parser.add_argument("--dry-run",      action="store_true")
    parser.add_argument("--competition",  type=str, default=None,
                        help="Process only this competition (e.g. 'LaLiga')")
    parser.add_argument("--report",       action="store_true",
                        help="After processing, print remaining pending rows with past dates")
    parser.add_argument("--list-pending", action="store_true",
                        help="Print pending predictions with past dates and exit (no fetch)")
    args = parser.parse_args()

    bq = bigquery.Client(project=PROJECT)

    if args.list_pending:
        pending = load_pending_predictions(bq, args.competition)
        print_pending_table(pending)
        return

    pending = load_pending_predictions(bq, args.competition)
    print(f"→ {len(pending)} pending prediction(s)")

    if not pending:
        print("✓ Nothing to update.")
        return

    # Build fetch queue: unique (competition, season) pairs
    seen_comp_seasons: set[tuple[str, str]] = set()
    fetch_queue: list[tuple[str, str]] = []

    for r in pending:
        comp     = r["competition"]
        ref_date = str(r.get("match_date") or r.get("publish_date") or "")
        season   = infer_season(comp, ref_date)
        key      = (comp, season)
        if key not in seen_comp_seasons:
            seen_comp_seasons.add(key)
            fetch_queue.append(key)

    # Pre-fetch API matches for every (competition, season) pair
    api_matches_cache: dict[str, list[dict]] = {}
    warned_no_id: set[str] = set()

    for comp, season in fetch_queue:
        league_id = COMPETITION_MAP.get(comp)
        if league_id is None:
            if comp not in warned_no_id:
                print(f"  [SKIP] No league ID for competition: {comp}")
                warned_no_id.add(comp)
            continue

        print(f"→ Fetching {comp} (league {league_id}, season {season})…")
        matches = fetch_matches_for_competition(league_id, season, comp)
        api_matches_cache.setdefault(comp, []).extend(matches)
        print(f"  {len(matches)} finished match(es) found")

    # Match & build result rows
    result_rows = []
    not_found   = 0

    for pred in pending:
        comp        = pred["competition"]
        api_matches = api_matches_cache.get(comp, [])
        is_aggregate = str(pred.get("match_type", "single")) == "aggregate"

        if is_aggregate:
            result = match_aggregate_to_result(pred, api_matches)
        else:
            result = match_prediction_to_result(pred, api_matches)

        if result is None:
            not_found += 1
            continue

        if args.dry_run:
            suffix = f"AGG {result['actual_home_goals']}-{result['actual_away_goals']}" if is_aggregate else f"{result['actual_home_goals']}-{result['actual_away_goals']}"
            print(
                f"  [DRY RUN] {pred['home_team']} vs {pred['away_team']}  "
                f"{suffix}  ({result['actual_result']})"
            )
        else:
            result_rows.append({
                "result_id":     pred["prediction_id"] + "_result",
                "prediction_id": pred["prediction_id"],
                "competition":   comp,
                "home_team":     pred["home_team"],
                "away_team":     pred["away_team"],
                "home_goals":    result["actual_home_goals"],
                "away_goals":    result["actual_away_goals"],
                "actual_result": result["actual_result"],
                "match_date":    result.get("match_date", ""),
            })

    print(f"\n→ Matched: {len(result_rows)}  |  not found: {not_found}")

    if not args.dry_run and result_rows:
        insert_results_to_bq(bq, result_rows)

    if args.report:
        still_pending = load_pending_predictions(bq, args.competition)
        print(f"\n── Remaining pending rows with past dates ──")
        print_pending_table(still_pending)


if __name__ == "__main__":
    main()
