# test_order.py
import argparse
from tool import KCEXTool
import time

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--auth", default="auth.json", help="path to auth.json")
    ap.add_argument("--headless", action="store_true", help="run headless")
    ap.add_argument("--amount", default="10", help="USDT amount per leg (string), e.g. 5")
    ap.add_argument("--leverage", default="50", help="leverage (string), e.g. 5")
    ap.add_argument("--margin", default="Cross", choices=["Cross", "Isolated"])
    ap.add_argument("--strategy", default="long_paxg", choices=["long_paxg", "short_paxg"])
    ap.add_argument("--action", default="open", choices=["open", "close"])
    ap.add_argument("--order_type", default="market", choices=["market", "limit"])
    ap.add_argument("--paxg_price", default="0")
    ap.add_argument("--xaut_price", default="0")
    args = ap.parse_args()

    k = KCEXTool(args.auth, headless=args.headless)

    try:
        # 啟動兩個 browser/page
        k.start(["PAXG", "XAUT"])
        time.sleep(5)


        # 直接測兩腿下單（你 tool.py 裡會自動做 pre-trade health_check_ui）
        t0 = time.perf_counter()
        res = k.two_legs_trade(
            strategy=args.strategy,        # long_paxg or short_paxg
            action=args.action,            # open or close
            order_type=args.order_type,    # market or limit
            amount_usdt=str(args.amount),
            margin_mode=args.margin,
            leverage=str(args.leverage),
            paxg_price=str(args.paxg_price),
            xaut_price=str(args.xaut_price),
            barrier_timeout_sec=8.0,
            take_screenshot=True,
        )
        t1 = time.perf_counter()

        elapsed = t1 - t0
        print(f"two_legs_trade() elapsed: {elapsed:.3f}s")

        print("=== RESULT ===")
        print(res)
        print("PAXG ok:", res["PAXG"].ok, "err:", res["PAXG"].error)
        print("XAUT ok:", res["XAUT"].ok, "err:", res["XAUT"].error)

    finally:
        k.stop()


if __name__ == "__main__":
    main()