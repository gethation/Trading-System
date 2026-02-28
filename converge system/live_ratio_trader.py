# live_ratio_trader.py
from __future__ import annotations

import sys
import time
import math
import shutil
import datetime as dt
import sqlite3
import uuid
from dataclasses import dataclass
from collections import deque
from typing import Deque, Optional, Tuple, Dict, Literal, Any

import pandas as pd
import ccxt

from tool import KCEXTool
from ccxt_bybit_fetcher import resolve_bybit_swap_symbol, download_ohlcv_ccxt

UTC = dt.timezone.utc
Pos = Literal["LONG_SPREAD", "SHORT_SPREAD"]


# ---------- console ----------
def overwrite_line(line: str) -> None:
    """同一行刷新顯示（避免 wrap 黏行）"""
    width = shutil.get_terminal_size((120, 20)).columns
    if width < 40:
        width = 120
    usable = max(0, width - 1)
    line = line[:usable]
    sys.stdout.write("\r" + line.ljust(usable))
    sys.stdout.flush()


def log_line(msg: str, tick_refresh: bool) -> None:
    """要印正常 log（signal/trade）前先換行，避免跟 tick 刷新行黏住"""
    if tick_refresh:
        sys.stdout.write("\n")
        sys.stdout.flush()
    print(msg, flush=True)


# ---------- math ----------
def ratio_from_prices(paxg: float, xaut: float) -> float:
    """(PAXG - XAUT) / (PAXG + XAUT) * 200"""
    denom = paxg + xaut
    if denom <= 0:
        return float("nan")
    return (paxg - xaut) / (denom + 1e-12) * 200.0


def mid_price(last: float, bid: float, ask: float) -> float:
    if bid > 0 and ask > 0:
        return 0.5 * (bid + ask)
    if bid > 0 and last > 0:
        return 0.5 * (bid + last)
    if ask > 0 and last > 0:
        return 0.5 * (ask + last)
    return last


def safe_bid(bid: float, mid: float) -> float:
    return bid if bid > 0 else mid


def safe_ask(ask: float, mid: float) -> float:
    return ask if ask > 0 else mid


# ---------- persistence (SQLite) ----------
class DBLogger:
    """
    記錄：
      - 每分鐘 rolling 更新後的統計（minutes table）
      - 交易事件（trades table）
    設計目標：讓你後續用 pandas / 其他程式方便 query、畫圖、回測對照。
    """

    def __init__(self, db_path: str, run_id: str):
        self.db_path = db_path
        self.run_id = run_id
        self.conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)  # autocommit
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def _init_db(self) -> None:
        cur = self.conn.cursor()
        # PRAGMA for better concurrency (read while writing) & decent safety
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA temp_store=MEMORY;")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS minutes (
              run_id TEXT NOT NULL,
              minute_ts INTEGER NOT NULL,              -- minute bucket start (UTC epoch seconds)
              bar_time_utc TEXT,
              p_close_mid REAL,
              x_close_mid REAL,
              ratio_close_mid REAL,
              mean REAL,
              std REAL,
              z_mid REAL,
              cross_up INTEGER,
              cross_down INTEGER,
              pos TEXT,
              -- exec snapshot at log time (using current tick quotes)
              ratio_exec_L REAL,
              ratio_exec_S REAL,
              z_exec_L REAL,
              z_exec_S REAL,
              p_last REAL, p_bid REAL, p_ask REAL,
              x_last REAL, x_bid REAL, x_ask REAL,
              signal TEXT,
              note TEXT,
              PRIMARY KEY (run_id, minute_ts)
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_minutes_run_min ON minutes(run_id, minute_ts);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_minutes_min ON minutes(minute_ts);")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
              run_id TEXT NOT NULL,
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts INTEGER NOT NULL,                      -- event time (UTC epoch seconds)
              minute_ts INTEGER NOT NULL,               -- derived bucket start
              event TEXT NOT NULL,                      -- ENTRY/EXIT/STOP
              action TEXT NOT NULL,                     -- open/close
              target_pos TEXT,                          -- LONG_SPREAD/SHORT_SPREAD
              pos_before TEXT,
              pos_after TEXT,
              reason TEXT,
              leg_usdt REAL,
              order_type TEXT,
              margin_mode TEXT,
              leverage TEXT,
              mean REAL,
              std REAL,
              ratio_exec_L REAL,
              ratio_exec_S REAL,
              z_exec_L REAL,
              z_exec_S REAL,
              p_last REAL, p_bid REAL, p_ask REAL,
              x_last REAL, x_bid REAL, x_ask REAL,
              ok_paxg INTEGER,
              ok_xaut INTEGER,
              screenshot_paxg TEXT,
              screenshot_xaut TEXT,
              raw_result TEXT
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_run_ts ON trades(run_id, ts);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts);")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
              run_id TEXT PRIMARY KEY,
              started_at INTEGER,
              note TEXT
            );
            """
        )
        cur.execute(
            "INSERT OR IGNORE INTO runs(run_id, started_at, note) VALUES (?, ?, ?);",
            (self.run_id, int(time.time()), None),
        )

    def log_minute(self, row: Dict[str, Any]) -> None:
        try:
            cols = list(row.keys())
            vals = [row[c] for c in cols]
            sql = f"INSERT OR REPLACE INTO minutes ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))});"
            self.conn.execute(sql, vals)
        except Exception as e:
            log_line(f"[db] log_minute failed: {e}", tick_refresh=False)

    def set_minute_signal(self, minute_ts: int, signal: str) -> None:
        """在 minutes 表上標記這個 minute bucket 的 signal；若該 minute row 尚未存在，先插入空殼 row。"""
        try:
            minute_ts = int(minute_ts)
            bar_time_utc = dt.datetime.fromtimestamp(minute_ts, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
            # 先插入空殼（允許其他欄位為 NULL）
            self.conn.execute(
                "INSERT OR IGNORE INTO minutes(run_id, minute_ts, bar_time_utc, signal) VALUES (?, ?, ?, ?);",
                (self.run_id, minute_ts, bar_time_utc, signal),
            )
            # 如果已存在，signal 以逗號累加（保留全部訊號）
            self.conn.execute(
                """UPDATE minutes
                   SET signal = CASE
                     WHEN signal IS NULL OR signal = '' THEN ?
                     ELSE signal || ',' || ?
                   END
                   WHERE run_id=? AND minute_ts=?;""",
                (signal, signal, self.run_id, minute_ts),
            )
        except Exception as e:
            log_line(f"[db] set_minute_signal failed: {e}", tick_refresh=False)

    def log_trade(self, row: Dict[str, Any]) -> None:
        try:
            cols = list(row.keys())
            vals = [row[c] for c in cols]
            sql = f"INSERT INTO trades ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))});"
            self.conn.execute(sql, vals)
        except Exception as e:
            log_line(f"[db] log_trade failed: {e}", tick_refresh=False)


# ---------- rolling ----------
class RollingStats:
    def __init__(self, window: int):
        self.n = int(window)
        self.buf: Deque[float] = deque()
        self.sum = 0.0
        self.sumsq = 0.0

    def ready(self) -> bool:
        return len(self.buf) >= self.n

    def push(self, x: float) -> None:
        if not math.isfinite(x):
            return
        self.buf.append(x)
        self.sum += x
        self.sumsq += x * x
        if len(self.buf) > self.n:
            old = self.buf.popleft()
            self.sum -= old
            self.sumsq -= old * old

    def mean_std(self) -> Tuple[float, float]:
        if len(self.buf) == 0:
            return float("nan"), float("nan")
        m = self.sum / len(self.buf)
        v = self.sumsq / len(self.buf) - m * m
        v = max(v, 0.0)
        return m, math.sqrt(v)

    def calibrate_to_once(self, target_mean: float, target_std: float, eps: float = 1e-12) -> None:
        """
        一次性校正：把當前 window 的 mean/std 對齊到 target_mean/target_std
        x' = a x + b
        a = target_std / cur_std
        b = target_mean - a * cur_mean
        """
        if len(self.buf) == 0:
            return
        if not (math.isfinite(target_mean) and math.isfinite(target_std)) or target_std <= 0:
            return

        cur_mean, cur_std = self.mean_std()
        if not (math.isfinite(cur_mean) and math.isfinite(cur_std)) or cur_std <= eps:
            return

        a = target_std / cur_std
        b = target_mean - a * cur_mean

        new_buf = deque((a * x + b) for x in self.buf)
        self.buf = new_buf
        self.sum = sum(self.buf)
        self.sumsq = sum(x * x for x in self.buf)


class MinuteMidAggregator:
    """高頻 mid -> 每分鐘一筆 mid close（用該分鐘最後一次看到的 mid）"""

    def __init__(self):
        self.cur_min_key: Optional[int] = None
        self.last_p_mid: Optional[float] = None
        self.last_x_mid: Optional[float] = None

    def update(self, ts: float, p_mid: float, x_mid: float) -> Optional[Tuple[int, float, float]]:
        min_key = int(ts // 60)
        if self.cur_min_key is None:
            self.cur_min_key = min_key
            self.last_p_mid, self.last_x_mid = p_mid, x_mid
            return None

        if min_key == self.cur_min_key:
            self.last_p_mid, self.last_x_mid = p_mid, x_mid
            return None

        out_key = self.cur_min_key
        out_p, out_x = self.last_p_mid, self.last_x_mid

        self.cur_min_key = min_key
        self.last_p_mid, self.last_x_mid = p_mid, x_mid

        if out_p is None or out_x is None:
            return None
        return out_key, float(out_p), float(out_x)


# ---------- config ----------
@dataclass
class Config:
    # warmup
    lookback: int = 1000
    extra: int = 300
    timeframe: str = "1m"
    tz: str = "America/New_York"

    # strategy thresholds (exec z-score)
    entry_z: float = 2.0
    exit_buffer_z: float = 1.5
    stop_z: float = 5.0

    # order
    leg_usdt: float = 30.0
    order_type: str = "market"
    margin_mode: str = "Cross"
    leverage: str = "20"
    take_screenshot: bool = False

    # live
    poll_sec: float = 1.0
    auth_path: str = "auth.json"
    headless: bool = True
    show_tick_refresh: bool = True

    # one-time calibration (from TradingView / KCEX)
    kcex_mean: Optional[float] = None
    kcex_std: Optional[float] = None

    # 防止連續觸發（只限制 ENTRY/EXIT，不限制 STOP）
    min_trade_interval_sec: float = 3.0

    # logging
    db_path: str = r"log\live_ratio_trader.sqlite"
    run_id: Optional[str] = None
    run_note: Optional[str] = None
    log_every_minute_info = False
    init_pos: Optional[Pos] = None  # None / "LONG_SPREAD" / "SHORT_SPREAD"


# ---------- warmup ----------
def warmup_bybit_ratio_close(cfg: Config) -> list[float]:
    now = dt.datetime.now(UTC).replace(second=0, microsecond=0)
    minutes = cfg.lookback + cfg.extra
    since_dt = now - dt.timedelta(minutes=minutes)

    since = since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    until = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    exchange = ccxt.bybit({
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    })

    paxg_symbol = resolve_bybit_swap_symbol(exchange, "PAXG", "USDT")
    xaut_symbol = resolve_bybit_swap_symbol(exchange, "XAUT", "USDT")
    print(f"[warmup] Resolved: {paxg_symbol} / {xaut_symbol}")

    paxg_df, _, _ = download_ohlcv_ccxt(
        symbol=paxg_symbol,
        timeframe=cfg.timeframe,
        since=since,
        until=until,
        exchange=exchange,
        tz=cfg.tz,
        save_csv=False,
        save_parquet=False,
        show_pbar=True,
    )
    xaut_df, _, _ = download_ohlcv_ccxt(
        symbol=xaut_symbol,
        timeframe=cfg.timeframe,
        since=since,
        until=until,
        exchange=exchange,
        tz=cfg.tz,
        save_csv=False,
        save_parquet=False,
        show_pbar=True,
    )

    merged = (
        pd.concat(
            [
                paxg_df[["close"]].rename(columns={"close": "PAXG_close"}),
                xaut_df[["close"]].rename(columns={"close": "XAUT_close"}),
            ],
            axis=1,
        )
        .dropna(how="any")
        .sort_index()
    )

    ratio = (merged["PAXG_close"] - merged["XAUT_close"]) / (merged["PAXG_close"] + merged["XAUT_close"] + 1e-12) * 200.0
    ratio = ratio.replace([float("inf"), float("-inf")], pd.NA).dropna()

    vals = [float(x) for x in ratio.tolist() if math.isfinite(float(x))]
    print(f"[warmup] bars={len(vals)} (need >= lookback={cfg.lookback})")
    return vals


# ---------- trader ----------
class LiveMeanReversionTrader:
    def __init__(self, cfg: Config, k: KCEXTool, db: DBLogger):
        self.cfg = cfg
        self.k = k
        self.db = db

        self.stats = RollingStats(cfg.lookback)
        self.agg = MinuteMidAggregator()

        if cfg.init_pos not in (None, "LONG_SPREAD", "SHORT_SPREAD"):
            raise ValueError(f"invalid init_pos: {cfg.init_pos}")

        self.pos = cfg.init_pos


        # cross detection (use 1m close(mid) vs mean BEFORE push)
        self.prev_diff_close: Optional[float] = None

        self.last_trade_ts: float = 0.0

    def can_trade(self) -> bool:
        return (time.time() - self.last_trade_ts) >= self.cfg.min_trade_interval_sec

    def mark_traded(self) -> None:
        self.last_trade_ts = time.time()

    def _dry_run(self, action: str) -> bool:
        """leg_usdt=0 -> 不下單（方便 dry run）"""
        if float(self.cfg.leg_usdt) == 0.0:
            log_line(f"[dry-run] {action} (leg_usdt=0)", tick_refresh=self.cfg.show_tick_refresh)
            return True
        return False

    def _log_trade_event(
        self,
        *,
        ts: float,
        event: str,
        action: str,
        target_pos: Optional[str],
        pos_before: Optional[str],
        pos_after: Optional[str],
        reason: str,
        mean: float,
        std: float,
        ratio_exec_L: float,
        ratio_exec_S: float,
        z_exec_L: float,
        z_exec_S: float,
        p_last: float, p_bid: float, p_ask: float,
        x_last: float, x_bid: float, x_ask: float,
        res: Optional[Dict[str, Any]] = None,
    ) -> None:
        minute_ts = int(ts // 60) * 60
        ok_paxg = None
        ok_xaut = None
        sc_paxg = None
        sc_xaut = None
        raw = None
        if res is not None:
            try:
                ok_paxg = 1 if res["PAXG"].ok else 0
                ok_xaut = 1 if res["XAUT"].ok else 0
                sc_paxg = getattr(res["PAXG"], "screenshot", None)
                sc_xaut = getattr(res["XAUT"], "screenshot", None)
                raw = repr(res)
            except Exception:
                raw = repr(res)

        self.db.log_trade(
            {
                "run_id": self.db.run_id,
                "ts": int(ts),
                "minute_ts": int(minute_ts),
                "event": event,
                "action": action,
                "target_pos": target_pos,
                "pos_before": pos_before,
                "pos_after": pos_after,
                "reason": reason,
                "leg_usdt": float(self.cfg.leg_usdt),
                "order_type": self.cfg.order_type,
                "margin_mode": self.cfg.margin_mode,
                "leverage": self.cfg.leverage,
                "mean": float(mean),
                "std": float(std),
                "ratio_exec_L": float(ratio_exec_L),
                "ratio_exec_S": float(ratio_exec_S),
                "z_exec_L": float(z_exec_L),
                "z_exec_S": float(z_exec_S),
                "p_last": float(p_last),
                "p_bid": float(p_bid),
                "p_ask": float(p_ask),
                "x_last": float(x_last),
                "x_bid": float(x_bid),
                "x_ask": float(x_ask),
                "ok_paxg": ok_paxg,
                "ok_xaut": ok_xaut,
                "screenshot_paxg": sc_paxg,
                "screenshot_xaut": sc_xaut,
                "raw_result": raw,
            }
        )

    def _trade_open_long(self, *, ts: float, reason: str, ctx: Dict[str, Any]) -> None:
        pos_before = self.pos
        if self._dry_run("OPEN long_spread"):
            self.pos = "LONG_SPREAD"
            self.mark_traded()
            self._log_trade_event(event="ENTRY", action="open", target_pos="LONG_SPREAD", pos_before=pos_before,
                                 pos_after=self.pos, reason="dry-run " + reason, ts=ts, res=None, **ctx)
            return

        res = self.k.two_legs_trade(
            strategy="long_paxg",
            action="open",
            order_type=self.cfg.order_type,
            amount_usdt=str(self.cfg.leg_usdt),
            margin_mode=self.cfg.margin_mode,
            leverage=self.cfg.leverage,
            take_screenshot=self.cfg.take_screenshot,
        )
        if not (res["PAXG"].ok and res["XAUT"].ok):
            self._log_trade_event(event="ENTRY", action="open", target_pos="LONG_SPREAD", pos_before=pos_before,
                                 pos_after=pos_before, reason="FAILED " + reason, ts=ts, res=res, **ctx)
            raise RuntimeError(f"OPEN long_spread failed: {res}")
        self.pos = "LONG_SPREAD"
        self.mark_traded()
        self._log_trade_event(event="ENTRY", action="open", target_pos="LONG_SPREAD", pos_before=pos_before,
                             pos_after=self.pos, reason=reason, ts=ts, res=res, **ctx)

    def _trade_open_short(self, *, ts: float, reason: str, ctx: Dict[str, Any]) -> None:
        pos_before = self.pos
        if self._dry_run("OPEN short_spread"):
            self.pos = "SHORT_SPREAD"
            self.mark_traded()
            self._log_trade_event(event="ENTRY", action="open", target_pos="SHORT_SPREAD", pos_before=pos_before,
                                 pos_after=self.pos, reason="dry-run " + reason, ts=ts, res=None, **ctx)
            return

        res = self.k.two_legs_trade(
            strategy="short_paxg",
            action="open",
            order_type=self.cfg.order_type,
            amount_usdt=str(self.cfg.leg_usdt),
            margin_mode=self.cfg.margin_mode,
            leverage=self.cfg.leverage,
            take_screenshot=self.cfg.take_screenshot,
        )
        if not (res["PAXG"].ok and res["XAUT"].ok):
            self._log_trade_event(event="ENTRY", action="open", target_pos="SHORT_SPREAD", pos_before=pos_before,
                                 pos_after=pos_before, reason="FAILED " + reason, ts=ts, res=res, **ctx)
            raise RuntimeError(f"OPEN short_spread failed: {res}")
        self.pos = "SHORT_SPREAD"
        self.mark_traded()
        self._log_trade_event(event="ENTRY", action="open", target_pos="SHORT_SPREAD", pos_before=pos_before,
                             pos_after=self.pos, reason=reason, ts=ts, res=res, **ctx)

    def _trade_close_long(self, *, ts: float, reason: str, ctx: Dict[str, Any], event_type: str) -> None:
        pos_before = self.pos
        if self._dry_run("CLOSE long_spread"):
            self.pos = None
            self.mark_traded()
            self._log_trade_event(event=event_type, action="close", target_pos="LONG_SPREAD", pos_before=pos_before,
                                 pos_after=self.pos, reason="dry-run " + reason, ts=ts, res=None, **ctx)
            return

        res = self.k.two_legs_trade(
            strategy="long_paxg",
            action="close",
            order_type=self.cfg.order_type,
            amount_usdt=str(self.cfg.leg_usdt),
            margin_mode=self.cfg.margin_mode,
            leverage=self.cfg.leverage,
            take_screenshot=self.cfg.take_screenshot,
        )
        if not (res["PAXG"].ok and res["XAUT"].ok):
            self._log_trade_event(event=event_type, action="close", target_pos="LONG_SPREAD", pos_before=pos_before,
                                 pos_after=pos_before, reason="FAILED " + reason, ts=ts, res=res, **ctx)
            raise RuntimeError(f"CLOSE long_spread failed: {res}")
        self.pos = None
        self.mark_traded()
        self._log_trade_event(event=event_type, action="close", target_pos="LONG_SPREAD", pos_before=pos_before,
                             pos_after=self.pos, reason=reason, ts=ts, res=res, **ctx)

    def _trade_close_short(self, *, ts: float, reason: str, ctx: Dict[str, Any], event_type: str) -> None:
        pos_before = self.pos
        if self._dry_run("CLOSE short_spread"):
            self.pos = None
            self.mark_traded()
            self._log_trade_event(event=event_type, action="close", target_pos="SHORT_SPREAD", pos_before=pos_before,
                                 pos_after=self.pos, reason="dry-run " + reason, ts=ts, res=None, **ctx)
            return

        res = self.k.two_legs_trade(
            strategy="short_paxg",
            action="close",
            order_type=self.cfg.order_type,
            amount_usdt=str(self.cfg.leg_usdt),
            margin_mode=self.cfg.margin_mode,
            leverage=self.cfg.leverage,
            take_screenshot=self.cfg.take_screenshot,
        )
        if not (res["PAXG"].ok and res["XAUT"].ok):
            self._log_trade_event(event=event_type, action="close", target_pos="SHORT_SPREAD", pos_before=pos_before,
                                 pos_after=pos_before, reason="FAILED " + reason, ts=ts, res=res, **ctx)
            raise RuntimeError(f"CLOSE short_spread failed: {res}")
        self.pos = None
        self.mark_traded()
        self._log_trade_event(event=event_type, action="close", target_pos="SHORT_SPREAD", pos_before=pos_before,
                             pos_after=self.pos, reason=reason, ts=ts, res=res, **ctx)

    def on_tick(self, ts: float, p_last: float, p_bid: float, p_ask: float, x_last: float, x_bid: float, x_ask: float) -> None:
        """
        規則：
        - Rolling window 每分鐘 push 一次「1m close(mid)」
        - ENTRY / EXIT 只在「分鐘更新（push）那一刻」評估與觸發
        - STOP（止損）每秒（每 tick）評估與觸發（不受 min_trade_interval_sec 限制）
        - 每分鐘會把 rolling 的資料寫進 SQLite（minutes table）；交易事件寫進 trades table
        """
        # --- mid (rolling) ---
        p_mid = mid_price(p_last, p_bid, p_ask)
        x_mid = mid_price(x_last, x_bid, x_ask)

        minute_updated = False
        cross_up = False
        cross_down = False
        minute_ts_for_bar: Optional[int] = None
        p_close_mid = None
        x_close_mid = None
        ratio_close_mid = None
        z_close_mid = None

        # --- update rolling per minute (mid close) ---
        out = self.agg.update(ts, p_mid, x_mid)
        if out is not None:
            minute_updated = True
            min_key, p_close_mid, x_close_mid = out
            minute_ts_for_bar = int(min_key * 60)
            ratio_close_mid = ratio_from_prices(p_close_mid, x_close_mid)

            # cross detection ONLY on 1m close(mid), using mean BEFORE pushing this bar
            mean_pre, _std_pre = self.stats.mean_std()
            if math.isfinite(mean_pre) and math.isfinite(ratio_close_mid):
                diff_close = ratio_close_mid - mean_pre
                # if self.prev_diff_close is not None and math.isfinite(self.prev_diff_close):
                #     cross_up = diff_close > 0.0
                #     cross_down = diff_close < 0.0
                self.prev_diff_close = diff_close

            self.stats.push(ratio_close_mid)

        if not self.stats.ready():
            return

        mean, std = self.stats.mean_std()
        if not (math.isfinite(mean) and math.isfinite(std) and std > 1e-12):
            return

        # --- exec prices ---
        p_sell = safe_bid(p_bid, p_mid)  # sell paxg
        p_buy = safe_ask(p_ask, p_mid)   # buy paxg
        x_sell = safe_bid(x_bid, x_mid)  # sell xaut
        x_buy = safe_ask(x_ask, x_mid)   # buy xaut

        # L side: BUY PAXG@ask, SELL XAUT@bid  (open long / close short)
        ratio_exec_L = ratio_from_prices(p_buy, x_sell)
        z_exec_L = (ratio_exec_L - mean) / (std + 1e-12)

        # S side: SELL PAXG@bid, BUY XAUT@ask  (open short / close long)
        ratio_exec_S = ratio_from_prices(p_sell, x_buy)
        z_exec_S = (ratio_exec_S - mean) / (std + 1e-12)

        # --- tick display (concise) ---
        now_utc = dt.datetime.fromtimestamp(ts, tz=UTC).strftime("%H:%M:%S")
        tick_line = (
            f"[tick {now_utc}Z] pos={self.pos or '-'} "
            f"ZL={z_exec_L:+.3f} ZS={z_exec_S:+.3f} "
            f"m={mean:+.6f} s={std:.6f}"
        )
        if self.cfg.show_tick_refresh:
            overwrite_line(tick_line)
        else:
            print(tick_line, flush=True)

        # --- minute log (after push; uses current tick quotes for exec snapshot) ---
        if minute_updated and minute_ts_for_bar is not None:
            # compute z_mid on close(mid) using post-push mean/std for consistency in charts
            if ratio_close_mid is not None and math.isfinite(ratio_close_mid):
                z_close_mid = (ratio_close_mid - mean) / (std + 1e-12)

            bar_time_utc = dt.datetime.fromtimestamp(minute_ts_for_bar, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
            self.db.log_minute(
                {
                    "run_id": self.db.run_id,
                    "minute_ts": int(minute_ts_for_bar),
                    "bar_time_utc": bar_time_utc,
                    "p_close_mid": float(p_close_mid) if p_close_mid is not None else None,
                    "x_close_mid": float(x_close_mid) if x_close_mid is not None else None,
                    "ratio_close_mid": float(ratio_close_mid) if ratio_close_mid is not None else None,
                    "mean": float(mean),
                    "std": float(std),
                    "z_mid": float(z_close_mid) if z_close_mid is not None else None,
                    "pos": self.pos,
                    "ratio_exec_L": float(ratio_exec_L) if math.isfinite(ratio_exec_L) else None,
                    "ratio_exec_S": float(ratio_exec_S) if math.isfinite(ratio_exec_S) else None,
                    "z_exec_L": float(z_exec_L) if math.isfinite(z_exec_L) else None,
                    "z_exec_S": float(z_exec_S) if math.isfinite(z_exec_S) else None,
                    "p_last": float(p_last),
                    "p_bid": float(p_bid),
                    "p_ask": float(p_ask),
                    "x_last": float(x_last),
                    "x_bid": float(x_bid),
                    "x_ask": float(x_ask),
                    "signal": None,
                    "note": None,
                }
            )
            if self.cfg.log_every_minute_info:
                log_line(
                    f"[1m close(mid)] {bar_time_utc}Z Pmid={p_close_mid:.2f} Xmid={x_close_mid:.2f} "
                    f"rmid={ratio_close_mid:+.6f} m={mean:+.6f} s={std:.6f} Zmid={z_close_mid:+.3f} "
                    f"cross_up={cross_up} cross_down={cross_down}",
                    tick_refresh=self.cfg.show_tick_refresh,
                )

        # build ctx for trade log
        ctx = {
            "mean": float(mean),
            "std": float(std),
            "ratio_exec_L": float(ratio_exec_L),
            "ratio_exec_S": float(ratio_exec_S),
            "z_exec_L": float(z_exec_L),
            "z_exec_S": float(z_exec_S),
            "p_last": float(p_last),
            "p_bid": float(p_bid),
            "p_ask": float(p_ask),
            "x_last": float(x_last),
            "x_bid": float(x_bid),
            "x_ask": float(x_ask),
        }

        # =========================
        # STOP: 每秒判斷（不受 min_trade_interval_sec 限制）
        # =========================
        # STOP: 用「關倉那一側」的 z
        if self.pos == "LONG_SPREAD" and abs(z_exec_S) >= self.cfg.stop_z:
            signal = f"STOP_LONG |ZS|={abs(z_exec_S):.3f}>= {self.cfg.stop_z}"
            log_line(f"[signal] {signal} -> close", self.cfg.show_tick_refresh)
            # mark minute signal on current minute bucket
            self.db.set_minute_signal(int(ts // 60) * 60, "STOP_LONG")
            self._trade_close_long(ts=ts, reason=signal, ctx=ctx, event_type="STOP")
            return

        if self.pos == "SHORT_SPREAD" and abs(z_exec_L) >= self.cfg.stop_z:
            signal = f"STOP_SHORT |ZL|={abs(z_exec_L):.3f}>= {self.cfg.stop_z}"
            log_line(f"[signal] {signal} -> close", self.cfg.show_tick_refresh)
            self.db.set_minute_signal(int(ts // 60) * 60, "STOP_SHORT")
            self._trade_close_short(ts=ts, reason=signal, ctx=ctx, event_type="STOP")
            return

        # =========================
        # ENTRY / EXIT: 只在每分鐘 rolling 更新那一刻判斷
        # =========================
        if not minute_updated:
            return

        if not self.can_trade():
            return

        # EXIT: cross + buffer（用關倉那側 z）
        if self.pos == "LONG_SPREAD":
            if cross_up and z_exec_S >= self.cfg.exit_buffer_z:
                signal = f"EXIT_LONG (1m) cross_up & ZS={z_exec_S:+.3f} >= {self.cfg.exit_buffer_z}"
                log_line(f"[signal] {signal} -> close", self.cfg.show_tick_refresh)
                if minute_ts_for_bar is not None:
                    self.db.set_minute_signal(minute_ts_for_bar, "EXIT_LONG")
                self._trade_close_long(ts=ts, reason=signal, ctx=ctx, event_type="EXIT")
                return

        if self.pos == "SHORT_SPREAD":
            if cross_down and z_exec_L <= -self.cfg.exit_buffer_z:
                signal = f"EXIT_SHORT (1m) cross_down & ZL={z_exec_L:+.3f} <= {-self.cfg.exit_buffer_z}"
                log_line(f"[signal] {signal} -> close", self.cfg.show_tick_refresh)
                if minute_ts_for_bar is not None:
                    self.db.set_minute_signal(minute_ts_for_bar, "EXIT_SHORT")
                self._trade_close_short(ts=ts, reason=signal, ctx=ctx, event_type="EXIT")
                return

        # ENTRY: 只在無倉位時進場（1m 判斷）
        if self.pos is None:
            if z_exec_S >= self.cfg.entry_z:
                signal = f"ENTRY_SHORT (1m) ZS={z_exec_S:+.3f} >= {self.cfg.entry_z}"
                log_line(f"[signal] {signal} -> open short_spread", self.cfg.show_tick_refresh)
                if minute_ts_for_bar is not None:
                    self.db.set_minute_signal(minute_ts_for_bar, "ENTRY_SHORT")
                self._trade_open_short(ts=ts, reason=signal, ctx=ctx)
                return

            if z_exec_L <= -self.cfg.entry_z:
                signal = f"ENTRY_LONG (1m) ZL={z_exec_L:+.3f} <= {-self.cfg.entry_z}"
                log_line(f"[signal] {signal} -> open long_spread", self.cfg.show_tick_refresh)
                if minute_ts_for_bar is not None:
                    self.db.set_minute_signal(minute_ts_for_bar, "ENTRY_LONG")
                self._trade_open_long(ts=ts, reason=signal, ctx=ctx)
                return