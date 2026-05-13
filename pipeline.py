"""
pipeline.py -- Maldini Stats end-to-end pipeline.

Reads YouTube video URLs from data/videos.csv (or a single --video-url),
fetches transcripts, extracts predictions via Claude Haiku, fetches finished
match results from TheSportsDB, scores each scored prediction with a Brier
score (DuckDB-friendly schema), and appends new rows to
data/predictions.parquet.

Idempotent: predictions whose video_id is already represented in the parquet
are skipped. Pending predictions (no result yet) are persisted with null
result fields so the next run picks them up.

Usage:
    python pipeline.py --file data/videos.csv
    python pipeline.py --video-url "https://www.youtube.com/watch?v=..."
    python pipeline.py --file data/videos.csv --dry-run
    python pipeline.py --list-pending
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

import anthropic
import duckdb
import pandas as pd
from dotenv import load_dotenv
from googleapiclient.discovery import build

import extract
import ingest
import results
import scoring

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

DATA_DIR        = ROOT / "data"
PREDICTIONS_PARQUET = DATA_DIR / "predictions.parquet"
VIDEOS_CSV          = DATA_DIR / "videos.csv"
OVERRIDES_CSV       = DATA_DIR / "results_overrides.csv"

PARQUET_COLUMNS = [
    "prediction_id", "video_id", "publish_date", "match_date",
    "home_team", "away_team", "competition", "match_type", "leg_number",
    "pred_home_win_pct", "pred_draw_pct", "pred_away_win_pct",
    "actual_result", "home_goals", "away_goals",
    "brier_score", "fetched_at",
]


# ── Parquet I/O ──────────────────────────────────────────────────────────────
def load_predictions() -> pd.DataFrame:
    if not PREDICTIONS_PARQUET.exists():
        return pd.DataFrame(columns=PARQUET_COLUMNS)
    return pd.read_parquet(PREDICTIONS_PARQUET)


def existing_video_ids(df: pd.DataFrame) -> set[str]:
    if df.empty or "video_id" not in df.columns:
        return set()
    return set(df["video_id"].dropna().astype(str).unique())


def write_predictions(df: pd.DataFrame) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out = df.reindex(columns=PARQUET_COLUMNS)
    # Normalise types so DuckDB reads the parquet without coercion warnings.
    for col in ("publish_date", "match_date"):
        out[col] = pd.to_datetime(out[col], errors="coerce").dt.date
    for col in ("leg_number", "pred_home_win_pct", "pred_draw_pct",
                "pred_away_win_pct", "home_goals", "away_goals"):
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int32")
    out["brier_score"] = pd.to_numeric(out["brier_score"], errors="coerce").astype("float64")
    out["fetched_at"] = pd.to_datetime(out["fetched_at"], errors="coerce", utc=True)

    tmp = PREDICTIONS_PARQUET.with_suffix(".parquet.tmp")
    out.to_parquet(tmp, index=False)
    tmp.replace(PREDICTIONS_PARQUET)


# ── Overrides ─────────────────────────────────────────────────────────────────
def load_overrides() -> dict[str, dict]:
    if not OVERRIDES_CSV.exists():
        return {}
    df = pd.read_csv(OVERRIDES_CSV)
    required = {"prediction_id", "home_goals", "away_goals"}
    missing = required - set(df.columns)
    if missing:
        print(f"  [WARN] {OVERRIDES_CSV.name} missing columns {missing}; ignoring file")
        return {}
    out = {}
    for _, row in df.iterrows():
        pid = str(row["prediction_id"]).strip()
        if not pid:
            continue
        out[pid] = {
            "home_goals":  int(row["home_goals"]),
            "away_goals":  int(row["away_goals"]),
            "match_date":  (str(row["match_date"]).strip()
                            if "match_date" in df.columns and pd.notna(row["match_date"])
                            else None),
            "match_type":  (str(row["match_type"]).strip()
                            if "match_type" in df.columns and pd.notna(row["match_type"])
                            else None),
        }
    return out


def apply_override(row: dict, override: dict) -> dict:
    hg = override["home_goals"]
    ag = override["away_goals"]
    row["home_goals"] = hg
    row["away_goals"] = ag
    row["actual_result"] = results.get_result_code(hg, ag)
    if override.get("match_date"):
        row["match_date"] = override["match_date"]
    if override.get("match_type") in ("single", "knockout"):
        row["match_type"] = override["match_type"]
    return row


# ── Per-video processing ──────────────────────────────────────────────────────
def process_video(
    yt,
    anthropic_client: anthropic.Anthropic,
    video_url: str,
) -> list[dict]:
    """Returns a list of prediction rows for this video (without results/Brier yet)."""
    video_id = ingest.parse_video_id(video_url)
    if not video_id:
        print(f"  [SKIP] cannot parse video_id from URL: {video_url}")
        return []

    meta = ingest.fetch_yt_metadata(yt, video_id)
    if not meta:
        print(f"  [SKIP] {video_id}: not found via YouTube API")
        return []

    print(f"  {video_id}  |  {meta['publish_date']}  |  {meta['title']}")

    transcript = ingest.fetch_yt_transcript(video_id)
    if not transcript:
        print(f"    [WARN] transcript unavailable (captions disabled); skipping")
        return []

    raw_preds = extract.extract_predictions_from_transcript(anthropic_client, video_id, transcript)
    rows = extract.build_prediction_rows(video_id, video_url, raw_preds, publish_date=meta["publish_date"])
    print(f"    -> {len(rows)} prediction(s) extracted")
    return rows


# ── Results + scoring ─────────────────────────────────────────────────────────
def attach_results(
    new_rows: list[dict],
    overrides: dict[str, dict],
) -> list[dict]:
    """Mutates rows in place to add result + brier_score. Returns the same list."""
    if not new_rows:
        return new_rows

    # Pre-fetch API matches per (competition, season) pair.
    seen: set[tuple[str, str]] = set()
    fetch_plan: list[tuple[str, str]] = []
    for r in new_rows:
        comp = r["competition"]
        ref_date = str(r.get("match_date") or r.get("publish_date") or "")
        season = results.infer_season(comp, ref_date)
        key = (comp, season)
        if key not in seen:
            seen.add(key)
            fetch_plan.append(key)

    api_cache: dict[str, list[dict]] = {}
    warned: set[str] = set()
    for comp, season in fetch_plan:
        league_id = results.COMPETITION_MAP.get(comp)
        if league_id is None:
            if comp not in warned:
                print(f"  [SKIP] no league ID for competition: {comp}")
                warned.add(comp)
            continue
        print(f"  fetching {comp} (league {league_id}, season {season})...")
        matches = results.fetch_matches_for_competition(league_id, season, comp)
        api_cache.setdefault(comp, []).extend(matches)
        print(f"    {len(matches)} finished match(es)")

    fetched_at = datetime.now(timezone.utc)

    for row in new_rows:
        pid = row["prediction_id"]
        override = overrides.get(pid)
        if override:
            row = apply_override(row, override)
            row["fetched_at"] = fetched_at
            row["brier_score"] = scoring.compute_brier(row)
            continue

        api_matches = api_cache.get(row["competition"], [])
        result = results.fetch_result(row, api_matches)
        if result is None:
            row.update({
                "actual_result": None,
                "home_goals": None,
                "away_goals": None,
                "brier_score": None,
                "fetched_at": None,
            })
            continue

        row.update({
            "actual_result": result["actual_result"],
            "home_goals":    result["home_goals"],
            "away_goals":    result["away_goals"],
            "match_date":    result.get("match_date") or row.get("match_date"),
            "fetched_at":    fetched_at,
        })
        row["brier_score"] = scoring.compute_brier(row)

    return new_rows


# ── Pending listing ───────────────────────────────────────────────────────────
def list_pending() -> None:
    df = load_predictions()
    if df.empty:
        print("No predictions yet.")
        return
    pending = df[df["actual_result"].isna()].copy()
    today = pd.Timestamp.today().date()
    pending = pending[pending["publish_date"].apply(
        lambda d: pd.notna(d) and pd.Timestamp(d).date() <= today
    )]
    if pending.empty:
        print("No pending predictions with past publish dates.")
        return
    cols = ["prediction_id", "competition", "home_team", "away_team",
            "match_type", "match_date", "publish_date"]
    print(pending[cols].to_string(index=False))


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Maldini Stats end-to-end pipeline.")
    parser.add_argument("--file",      type=Path, help="CSV of video URLs (column: video_url).")
    parser.add_argument("--video-url", type=str,  help="Process a single YouTube URL.")
    parser.add_argument("--dry-run",   action="store_true", help="Don't write the parquet.")
    parser.add_argument("--list-pending", action="store_true",
                        help="Print predictions with no result yet and exit.")
    args = parser.parse_args()

    if args.list_pending:
        list_pending()
        return

    if not args.file and not args.video_url:
        args.file = VIDEOS_CSV

    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY not set in .env")

    yt = build("youtube", "v3", developerKey=api_key)
    client = anthropic.Anthropic()

    existing = load_predictions()
    seen_ids = existing_video_ids(existing)
    print(f"existing parquet rows: {len(existing)}  |  unique video_ids: {len(seen_ids)}")

    # Build the list of (url) to process.
    if args.video_url:
        urls = [args.video_url]
    else:
        if not args.file.exists():
            raise FileNotFoundError(f"{args.file} does not exist")
        df_videos = pd.read_csv(args.file)
        if "video_url" not in df_videos.columns:
            raise ValueError(f"{args.file} missing required column 'video_url'")
        urls = df_videos["video_url"].dropna().astype(str).tolist()

    new_rows: list[dict] = []
    for url in urls:
        vid = ingest.parse_video_id(url)
        if vid and vid in seen_ids:
            print(f"  [SKIP] {vid} already in parquet")
            continue
        print(f"\n-> {url}")
        new_rows.extend(process_video(yt, client, url))

    if not new_rows:
        print("\nNothing new to process.")
        return

    print(f"\nresolving results for {len(new_rows)} new prediction(s)...")
    overrides = load_overrides()
    if overrides:
        print(f"  loaded {len(overrides)} override(s) from {OVERRIDES_CSV.name}")
    attach_results(new_rows, overrides)

    new_df = pd.DataFrame(new_rows)
    combined = pd.concat([existing, new_df], ignore_index=True)

    # DuckDB pass: dedupe on prediction_id (keep latest) and sort.
    con = duckdb.connect(":memory:")
    con.register("combined", combined)
    dedup = con.sql("""
        select * exclude (rn)
        from (
            select *, row_number() over (
                partition by prediction_id
                order by fetched_at desc nulls last
            ) as rn
            from combined
        )
        where rn = 1
        order by publish_date nulls last, prediction_id
    """).df()

    scored = dedup["brier_score"].notna().sum()
    pending = dedup["brier_score"].isna().sum()
    print(f"\nnew rows: {len(new_df)}  |  total scored: {scored}  |  pending: {pending}")

    if args.dry_run:
        print("[DRY RUN] not writing parquet")
        return

    write_predictions(dedup)
    print(f"-> wrote {PREDICTIONS_PARQUET.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
