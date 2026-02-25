# read_sqlite.py
from __future__ import annotations

import argparse
import sqlite3
from typing import Optional, Tuple

import pandas as pd


def connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con


def list_runs(con: sqlite3.Connection) -> pd.DataFrame:
    # runs may not exist in older DBs; handle gracefully
    try:
        df = pd.read_sql_query(
            "SELECT run_id, started_at, note FROM runs ORDER BY started_at DESC",
            con,
        )
        if "started_at" in df.columns:
            df["started_at_utc"] = pd.to_datetime(df["started_at"], unit="s", utc=True)
        return df
    except Exception:
        # fallback: infer from minutes
        df = pd.read_sql_query(
            "SELECT DISTINCT run_id FROM minutes ORDER BY run_id DESC",
            con,
        )
        df["started_at"] = None
        df["note"] = None
        return df


def get_time_bounds(con: sqlite3.Connection, run_id: str) -> Tuple[Optional[int], Optional[int]]:
    row = con.execute(
        "SELECT MIN(minute_ts) AS t0, MAX(minute_ts) AS t1 FROM minutes WHERE run_id=?",
        (run_id,),
    ).fetchone()
    if row is None:
        return None, None
    return row["t0"], row["t1"]


def load_minutes(
    con: sqlite3.Connection,
    run_id: str,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
) -> pd.DataFrame:
    sql = "SELECT * FROM minutes WHERE run_id=?"
    params = [run_id]
    if start_ts is not None:
        sql += " AND minute_ts >= ?"
        params.append(int(start_ts))
    if end_ts is not None:
        sql += " AND minute_ts <= ?"
        params.append(int(end_ts))
    sql += " ORDER BY minute_ts"

    df = pd.read_sql_query(sql, con, params=params)
    if not df.empty and "minute_ts" in df.columns:
        df["t"] = pd.to_datetime(df["minute_ts"], unit="s", utc=True)
    return df


def load_trades(
    con: sqlite3.Connection,
    run_id: str,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
) -> pd.DataFrame:
    sql = "SELECT * FROM trades WHERE run_id=?"
    params = [run_id]
    if start_ts is not None:
        sql += " AND ts >= ?"
        params.append(int(start_ts))
    if end_ts is not None:
        sql += " AND ts <= ?"
        params.append(int(end_ts))
    sql += " ORDER BY ts"

    df = pd.read_sql_query(sql, con, params=params)
    if not df.empty:
        df["t"] = pd.to_datetime(df["ts"], unit="s", utc=True)
        df["minute_t"] = pd.to_datetime(df["minute_ts"], unit="s", utc=True)
    return df


def make_minute_view(minutes: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    """
    回傳一個「每分鐘一列」的 view：
      - minutes 原欄位
      - 加上 trades_count, trades_events, trades_actions, trades_reason_sample 等聚合欄位
    """
    out = minutes.copy()
    if out.empty:
        return out

    if trades is None or trades.empty:
        out["trades_count"] = 0
        out["trades_events"] = ""
        out["trades_actions"] = ""
        out["trades_reason_sample"] = ""
        return out

    g = trades.groupby("minute_ts", as_index=False).agg(
        trades_count=("id", "count"),
        trades_events=("event", lambda s: ",".join(map(str, s.tolist()))),
        trades_actions=("action", lambda s: ",".join(map(str, s.tolist()))),
        trades_reason_sample=("reason", lambda s: str(s.iloc[0]) if len(s) else ""),
    )

    out = out.merge(g, on="minute_ts", how="left")
    out["trades_count"] = out["trades_count"].fillna(0).astype(int)
    out["trades_events"] = out["trades_events"].fillna("")
    out["trades_actions"] = out["trades_actions"].fillna("")
    out["trades_reason_sample"] = out["trades_reason_sample"].fillna("")
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Read live_ratio_trader.sqlite (minutes + trades)")
    p.add_argument("--db", default=r"log\live_ratio_trader.sqlite", help="path to sqlite db")
    p.add_argument("--list-runs", action="store_true", help="list run_id")
    p.add_argument("--run-id", default=None, help="run_id to load (required unless --list-runs)")
    p.add_argument("--start", default=None, help="start time (UTC) like 2026-02-23T12:00:00Z or epoch seconds")
    p.add_argument("--end", default=None, help="end time (UTC) like 2026-02-23T18:00:00Z or epoch seconds")
    p.add_argument("--csv-out", default=None, help="export merged minute view to CSV")
    p.add_argument("--show-head", type=int, default=10, help="print head(n) of each table")
    return p.parse_args()


def parse_time(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    s = s.strip()
    # epoch seconds
    if s.isdigit():
        return int(s)
    # ISO-ish
    # accept ...Z or without Z
    ts = pd.to_datetime(s, utc=True, errors="raise")
    return int(ts.timestamp())


def main() -> None:
    args = parse_args()
    con = connect(args.db)

    if args.list_runs:
        runs = list_runs(con)
        print(runs.to_string(index=False))
        return

    if not args.run_id:
        runs = list_runs(con)
        print("No --run-id provided. Available runs:\n")
        print(runs.to_string(index=False))
        return

    start_ts = parse_time(args.start)
    end_ts = parse_time(args.end)

    # helpful bounds
    t0, t1 = get_time_bounds(con, args.run_id)
    if t0 is not None and t1 is not None:
        print(f"[bounds] minutes: {pd.to_datetime(t0, unit='s', utc=True)} .. {pd.to_datetime(t1, unit='s', utc=True)}")

    minutes = load_minutes(con, args.run_id, start_ts=start_ts, end_ts=end_ts)
    trades = load_trades(con, args.run_id, start_ts=start_ts, end_ts=end_ts)

    print(f"\nminutes rows={len(minutes)} cols={len(minutes.columns)}")
    print(minutes.head(args.show_head).to_string(index=False))

    print(f"\ntrades rows={len(trades)} cols={len(trades.columns)}")
    print(trades.head(args.show_head).to_string(index=False))

    merged = make_minute_view(minutes, trades)
    print(f"\nminute_view rows={len(merged)} cols={len(merged.columns)}")
    print(merged.head(args.show_head).to_string(index=False))

    if args.csv_out:
        merged.to_csv(args.csv_out, index=False)
        print(f"\n[export] wrote {args.csv_out}")

    con.close()


if __name__ == "__main__":
    main()