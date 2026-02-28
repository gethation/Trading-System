from __future__ import annotations

from tool import KCEXTool
import time


def main():
    # 1) 啟動兩個常駐 worker（各自一個 page/thread，不用每次下單重開）
    k = KCEXTool(storage_state_path="auth.json", headless=True).start(["PAXG", "XAUT"])

    try:
        # 2) (可選) 同時抓兩邊快照，幫你確認當下價格來源、bid/ask
        snaps = k.get_multi_snapshot(["PAXG", "XAUT"])
        paxg = snaps["PAXG"]
        xaut = snaps["XAUT"]
        print(f"PAXG price={paxg.price} ask={paxg.ask_price} bid={paxg.bid_price} src={paxg.source}")
        print(f"XAUT price={xaut.price} ask={xaut.ask_price} bid={xaut.bid_price} src={xaut.source}")

        # 3) 市價單：long_paxg => PAXG 開多 + XAUT 開空（Barrier 同步點擊）
        #    amount_usdt: 兩腿都用同一個 USDT 金額（依你的 tool.py 設計）:contentReference[oaicite:2]{index=2}
        results = k.two_legs_trade(
            strategy="long_paxg",   # 或 "short_paxg"
            action="close",          # "open" / "close"
            order_type="market",    # "market" / "limit"
            amount_usdt="10",       # 字串
            margin_mode="Cross",    # "Cross" / "Isolated"
            leverage="20",          # 字串
            # 市價單不需要 paxg_price/xaut_price（會被忽略）:contentReference[oaicite:3]{index=3}
            barrier_timeout_sec=8.0,
            take_screenshot=True,
        )
        print("=== two_legs_trade results ===")
        for sym, r in results.items():
            print(f"{sym}: ok={r.ok} error={r.error} screenshot={r.screenshot}")

    finally:
        # 4) 收尾（關 browser/context/page）
        k.stop()


if __name__ == "__main__":
    main()