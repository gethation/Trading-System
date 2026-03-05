from live_ratio_trader import Config, DBLogger, warmup_bybit_ratio_close, LiveMeanReversionTrader, log_line
from tool import KCEXTool
from typing import Dict, Any
import datetime as dt
import time
import uuid
import os

UTC = dt.timezone.utc


def _should_refresh_on_error(e: Exception) -> bool:
    s = str(e).lower()
    retry_keys = [
        "timeout",
        "timed out",
        "locator.click",
        "locator.wait_for",
        "page.wait_for",
        "target page, context or browser has been closed",
        "page closed",
        "browser has been closed",
        "context has been closed",
        "execution context was destroyed",
        "intercepts pointer events",
        "barrier broken/timeout",
        "pre-trade ui health check failed",
        "worker not ready",
        "startup failed",
    ]
    return any(k in s for k in retry_keys)


def main():
    cfg = Config(
        lookback=500,
        extra=300,
        entry_z=2.0,
        exit_buffer_z=1.0,
        stop_z=30.0,
        leg_usdt=2500.0,
        leverage="50",
        margin_mode="Cross",
        poll_sec=1.0,
        auth_path="auth.json",
        headless=True,
        show_tick_refresh=True,
        # one-time calibration (optional)
        kcex_mean=632 / 1000,
        kcex_std=46 / 1000,
        min_trade_interval_sec=3.0,
        # logging
        db_path=r"log\live_ratio_trader.sqlite",
        run_id=None,
        run_note=None,
        init_pos=None,  # None / "LONG_SPREAD" / "SHORT_SPREAD"
    )

    # run id
    run_id = (
        cfg.run_id
        if cfg.run_id is not None
        else dt.datetime.now(UTC).strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
    )
    db = DBLogger(cfg.db_path, run_id=run_id)
    if cfg.run_note:
        try:
            db.conn.execute("UPDATE runs SET note=? WHERE run_id=?;", (cfg.run_note, run_id))
        except Exception:
            pass

    # warmup rolling with bybit
    stats_seed = warmup_bybit_ratio_close(cfg)

    k = KCEXTool(cfg.auth_path, headless=cfg.headless)
    trader = LiveMeanReversionTrader(cfg, k, db)

    for v in stats_seed[-cfg.lookback:]:
        trader.stats.push(v)

    if not trader.stats.ready():
        raise RuntimeError(f"warmup not enough: have={len(trader.stats.buf)} need={cfg.lookback}")

    # optional one-time calibration (align rolling window to TradingView/KCEX mean/std)
    if cfg.kcex_mean is not None and cfg.kcex_std is not None:
        before_m, before_s = trader.stats.mean_std()
        trader.stats.calibrate_to_once(float(cfg.kcex_mean), float(cfg.kcex_std))
        after_m, after_s = trader.stats.mean_std()
        print(
            f"[calib] applied once: before mean={before_m:.6f} std={before_s:.6f} -> "
            f"after mean={after_m:.6f} std={after_s:.6f} (target mean={cfg.kcex_mean} std={cfg.kcex_std})",
            flush=True,
        )

    mean, std = trader.stats.mean_std()
    print(f"[warmup] ready. rolling_len={len(trader.stats.buf)} mean={mean:.6f} std={std:.6f}", flush=True)

    # start KCEX workers
    k.start(["PAXG", "XAUT"])

    shot_dir = "screens"
    os.makedirs(shot_dir, exist_ok=True)
    shot_paths = {
        "PAXG": os.path.join(shot_dir, r"PAXG_latest.png"),
        "XAUT": os.path.join(shot_dir, r"XAUT_latest.png"),
    }

    last_capture_minute = None
    consecutive_recoveries = 0
    max_consecutive_recoveries = 8

    print(f"[live] start tick loop ... (Ctrl+C to stop) | run_id={run_id} db={cfg.db_path}", flush=True)

    try:
        while True:
            try:
                snaps: Dict[str, Any] = k.get_multi_snapshot(["PAXG", "XAUT"])
                p = snaps["PAXG"]
                x = snaps["XAUT"]

                trader.on_tick(
                    ts=float(p.ts),
                    p_last=float(p.price),
                    p_bid=float(p.bid_price),
                    p_ask=float(p.ask_price),
                    x_last=float(x.price),
                    x_bid=float(x.bid_price),
                    x_ask=float(x.ask_price),
                )

                cur_minute = int(float(p.ts) // 60)
                if cur_minute != last_capture_minute:
                    last_capture_minute = cur_minute
                    try:
                        _ = k.capture_pages(shot_paths, full_page=False)
                    except Exception as e:
                        log_line(
                            f"[shot] capture failed: {e}",
                            tick_refresh=cfg.show_tick_refresh,
                        )

                consecutive_recoveries = 0
                time.sleep(cfg.poll_sec)

            except Exception as e:
                msg = str(e)
                log_line(
                    f"[live] loop error: {msg}",
                    tick_refresh=cfg.show_tick_refresh,
                )

                if _should_refresh_on_error(e):
                    consecutive_recoveries += 1

                    if consecutive_recoveries > max_consecutive_recoveries:
                        raise RuntimeError(
                            f"too many consecutive recoveries ({consecutive_recoveries})"
                        ) from e

                    try:
                        log_line(
                            f"[live] try refresh browsers ({consecutive_recoveries}/{max_consecutive_recoveries})...",
                            tick_refresh=cfg.show_tick_refresh,
                        )
                        k.refresh(["PAXG", "XAUT"])
                        log_line(
                            "[live] browsers refreshed, continue loop.",
                            tick_refresh=cfg.show_tick_refresh,
                        )
                        time.sleep(max(0.5, cfg.poll_sec))
                        continue
                    except Exception as e2:
                        log_line(
                            f"[live] refresh failed: {e2}",
                            tick_refresh=cfg.show_tick_refresh,
                        )
                        raise

                # 非可恢復錯誤，不要硬吞，直接停掉
                raise

    except KeyboardInterrupt:
        log_line("[live] keyboard interrupt.", tick_refresh=cfg.show_tick_refresh)

    finally:
        k.stop()
        db.close()
        log_line("[live] stopped.", tick_refresh=cfg.show_tick_refresh)


if __name__ == "__main__":
    main()
