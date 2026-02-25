# live_ratio_feed.py
from __future__ import annotations

import sys
import time
import math
import shutil
import datetime as dt
from dataclasses import dataclass
from collections import deque
from typing import Deque, Optional, Tuple, Dict

import pandas as pd
import ccxt

from tool import KCEXTool
from ccxt_bybit_fetcher import resolve_bybit_swap_symbol, download_ohlcv_ccxt

UTC = dt.timezone.utc


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
    """印 log 前先換行，避免跟 tick 刷新行黏住"""
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
    """優先用 bid/ask 算 mid；缺資料就退回 last（或與 last 折衷）"""
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


# ---------- rolling ----------
class RollingStats:
    """固定長度 rolling mean/std（O(1) 更新），並支援一次性校正"""

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

    # live
    poll_sec: float = 1.0
    auth_path: str = "auth.json"
    headless: bool = False
    show_tick_refresh: bool = True  # True: 同一行刷新；False: 每秒 print 一行

    # one-time calibration (from TradingView / KCEX)
    # 填 None 就不校正；填數字就初始化校正一次
    kcex_mean: Optional[float] = None
    kcex_std: Optional[float] = None


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


# ---------- main ----------
def main():
    cfg = Config(
        lookback=1000,
        extra=300,
        poll_sec=1.0,
        auth_path=r"auth.json",
        headless=False,
        show_tick_refresh=True,
        kcex_mean=406/1000,
        kcex_std=47/1000,
    )

    # 1) warmup rolling（Bybit close ≈ mid）
    stats = RollingStats(cfg.lookback)
    for v in warmup_bybit_ratio_close(cfg):
        stats.push(v)

    if not stats.ready():
        raise RuntimeError(f"warmup not enough: have={len(stats.buf)} need={cfg.lookback}")

    m0, s0 = stats.mean_std()
    print(f"[warmup] ready. rolling_len={len(stats.buf)} mean={m0:+.6f} std={s0:.6f}")

    # 2) one-time calibration to KCEX mean/std (optional)
    if cfg.kcex_mean is not None and cfg.kcex_std is not None:
        stats.calibrate_to_once(float(cfg.kcex_mean), float(cfg.kcex_std))
        m1, s1 = stats.mean_std()
        print(f"[calib ] applied once. target_mean={cfg.kcex_mean:+.6f} target_std={cfg.kcex_std:.6f}")
        print(f"[calib ] after: mean={m1:+.6f} std={s1:.6f}")

    # 3) live: rolling 用 mid 的 1m close 更新；tick 顯示 exec z-score（兩方向）
    agg = MinuteMidAggregator()
    k = KCEXTool(cfg.auth_path, headless=cfg.headless)
    k.start(["PAXG", "XAUT"])
    print("[live] polling KCEX snapshots ... (Ctrl+C to stop)")

    try:
        while True:
            snaps: Dict[str, object] = k.get_multi_snapshot(["PAXG", "XAUT"])

            p = snaps["PAXG"]
            x = snaps["XAUT"]

            p_last = float(p.price)
            p_bid = float(p.bid_price)
            p_ask = float(p.ask_price)

            x_last = float(x.price)
            x_bid = float(x.bid_price)
            x_ask = float(x.ask_price)

            ts = float(p.ts)

            # mid (rolling base)
            p_mid = mid_price(p_last, p_bid, p_ask)
            x_mid = mid_price(x_last, x_bid, x_ask)
            ratio_mid = ratio_from_prices(p_mid, x_mid)

            mean, std = stats.mean_std()

            # exec prices (both sides)
            p_sell = safe_bid(p_bid, p_mid)   # SELL PAXG
            p_buy  = safe_ask(p_ask, p_mid)   # BUY  PAXG
            x_sell = safe_bid(x_bid, x_mid)   # SELL XAUT
            x_buy  = safe_ask(x_ask, x_mid)   # BUY  XAUT

            # L side: BUY PAXG@ask, SELL XAUT@bid (open long / close short)
            ratio_exec_L = ratio_from_prices(p_buy, x_sell)
            # S side: SELL PAXG@bid, BUY XAUT@ask (open short / close long)
            ratio_exec_S = ratio_from_prices(p_sell, x_buy)

            if math.isfinite(mean) and math.isfinite(std) and std > 1e-12:
                zL = (ratio_exec_L - mean) / (std + 1e-12)
                zS = (ratio_exec_S - mean) / (std + 1e-12)
            else:
                zL = float("nan")
                zS = float("nan")

            # tick display (concise)
            now_utc = dt.datetime.fromtimestamp(ts, tz=UTC).strftime("%H:%M:%S")
            tick_line = (
                f"[tick {now_utc}Z] "
                f"rm={ratio_mid:+.6f} m={mean:+.6f} s={std:.6f} "
                f"ZL={zL:+.3f} ZS={zS:+.3f}"
            )
            if cfg.show_tick_refresh:
                overwrite_line(tick_line)
            else:
                print(tick_line, flush=True)

            # minute close(mid) -> update rolling
            out = agg.update(ts, p_mid, x_mid)
            if out is not None:
                min_key, p_close_mid, x_close_mid = out
                ratio_close_mid = ratio_from_prices(p_close_mid, x_close_mid)

                stats.push(ratio_close_mid)
                mean2, std2 = stats.mean_std()
                zmid = (ratio_close_mid - mean2) / (std2 + 1e-12) if std2 > 1e-12 else float("nan")

                bar_time_utc = dt.datetime.fromtimestamp(min_key * 60, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
                log_line(
                    f"[1m close(mid)] {bar_time_utc}Z "
                    f"rm={ratio_close_mid:+.6f} m={mean2:+.6f} s={std2:.6f} Zmid={zmid:+.3f}",
                    tick_refresh=cfg.show_tick_refresh,
                )

            time.sleep(cfg.poll_sec)

    finally:
        k.stop()
        log_line("[live] stopped.", tick_refresh=cfg.show_tick_refresh)


if __name__ == "__main__":
    main()