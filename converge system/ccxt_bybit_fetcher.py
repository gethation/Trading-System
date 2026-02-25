import math
import warnings
from pathlib import Path
from typing import Optional, Union, Tuple

import ccxt
import pandas as pd
from tqdm.rich import tqdm, TqdmExperimentalWarning
warnings.filterwarnings("ignore", category=TqdmExperimentalWarning)


def resolve_bybit_swap_symbol(exchange: ccxt.Exchange, base: str, quote: str = "USDT") -> str:
    """
    Resolve unified CCXT symbol for Bybit linear perpetual (swap).
    Typical symbol formats on Bybit in CCXT:
      - BASE/USDT:USDT   (most common for linear perpetual)
      - BASE/USDT        (rare for swap; more common for spot)
    """
    exchange.load_markets()

    candidates = [
        f"{base}/{quote}:USDT",
        f"{base}/{quote}:{quote}",
        f"{base}/{quote}",
    ]
    for s in candidates:
        if s in exchange.markets:
            return s

    hits = [s for s in exchange.symbols if s.startswith(f"{base}/{quote}")]
    raise ValueError(
        f"Bybit swap symbol not found for base={base}, quote={quote}. "
        f"Tried: {candidates}. Matches (first 30): {hits[:30]}"
    )


def download_ohlcv_ccxt(
    symbol: str,
    timeframe: str = "1m",
    since: Union[str, int] = None,
    until: Optional[Union[str, int]] = None,
    *,
    exchange: Optional[ccxt.Exchange] = None,
    limit: int = 1000,
    tz: str = "America/New_York",
    save_dir: Union[str, Path] = "data",
    mark: str = "",
    save_csv: bool = True,
    save_parquet: bool = True,
    parquet_engine: str = "pyarrow",
    show_pbar: bool = True,
) -> Tuple[pd.DataFrame, Optional[Path], Optional[Path]]:
    """
    下載任意 CCXT 交易所的 OHLCV（你現在用 Bybit 永續也適用），回傳 DataFrame，並可選擇存成 CSV/Parquet。

    - symbol: CCXT unified symbol (Bybit 永續常見：'PAXG/USDT:USDT')
    - since/until: ISO8601 字串或毫秒 timestamp(int)
      - until 不包含那根（與你原本邏輯一致：candle[0] < until）
    """
    if since is None:
        raise ValueError("since 不能是 None，請提供開始時間（ISO8601 或毫秒 timestamp）")

    if exchange is None:
        # 你想抓 Bybit 永續就不要用這個預設；建議外面傳入 bybit(exchange)
        exchange = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "future"}})

    exchange.load_markets()
    if symbol not in exchange.markets:
        # 提供一些可能的候選，尤其是 Bybit swap 很常需要 :USDT
        base = symbol.split("/")[0] if "/" in symbol else symbol
        candidates = [s for s in exchange.symbols if base in s]
        raise ValueError(
            f"Symbol '{symbol}' not found on exchange '{exchange.id}'. "
            f"Candidates containing '{base}' (first 30): {candidates[:30]}"
        )

    def to_ms(x: Union[str, int]) -> int:
        if isinstance(x, int):
            return x
        if isinstance(x, str):
            ms = exchange.parse8601(x)
            if ms is None:
                raise ValueError(f"無法 parse8601: {x}")
            return ms
        raise TypeError(f"since/until 只接受 str 或 int，現在是 {type(x)}")

    since_ms = to_ms(since)
    until_ms = to_ms(until) if until is not None else None

    ms_per_candle = exchange.parse_timeframe(timeframe) * 1000
    end_timestamp = until_ms if until_ms is not None else exchange.milliseconds()

    if since_ms >= end_timestamp:
        if show_pbar:
            print(f"start time {exchange.iso8601(since_ms)} after until time {exchange.iso8601(end_timestamp)}")
        empty = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        empty.index.name = "datetime"
        return empty, None, None

    total_candles = (end_timestamp - since_ms) // ms_per_candle + 1
    total_iters = math.ceil(total_candles / limit) if total_candles > 0 else 0

    pbar = None
    if show_pbar:
        pbar = tqdm(total=total_iters, desc=f"Fetching {exchange.id} {symbol} {timeframe}", dynamic_ncols=True, unit="batch")

    all_ohlcv = []
    cursor = since_ms

    while True:
        current_limit = limit

        if until_ms is not None and cursor + limit * ms_per_candle > until_ms:
            remaining_candles = (until_ms - cursor) // ms_per_candle + 1
            current_limit = max(1, int(remaining_candles))

        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, cursor, current_limit)
        if not ohlcv:
            break

        # until 不包含那根（< until）
        if until_ms is not None:
            ohlcv = [c for c in ohlcv if c[0] < until_ms]
            if not ohlcv:
                break

        all_ohlcv.extend(ohlcv)
        new_cursor = ohlcv[-1][0] + ms_per_candle

        if pbar is not None:
            pbar.update(1)

        if (until_ms is not None and new_cursor >= until_ms) or (len(ohlcv) < current_limit):
            break

        cursor = new_cursor

    if pbar is not None:
        pbar.close()

    if not all_ohlcv:
        empty = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        empty.index.name = "datetime"
        return empty, None, None

    df = pd.DataFrame(all_ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df.index = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df.index = df.index.tz_convert(tz).tz_localize(None)
    df.drop(columns=["timestamp"], inplace=True)
    df.index.name = "datetime"
    df = df.sort_index()

    # 去重（保留最後一筆）
    dup = df.index.duplicated(keep="last")
    if dup.any():
        df = df[~dup]

    csv_path = None
    pq_path = None

    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    # 讓檔名不受 symbol 的 :USDT 影響（Windows 冒號不能在檔名中）
    base = symbol.split("/")[0]
    base_name = f"{base}_{timeframe}"
    if mark:
        base_name = f"{base_name}({mark})"

    if save_csv:
        csv_path = save_dir / f"{base_name}.csv"
        df.to_csv(csv_path, index=True)

    if save_parquet:
        pq_path = save_dir / f"{base_name}.parquet"
        df.to_parquet(pq_path, engine=parquet_engine)

    return df, csv_path, pq_path


if __name__ == "__main__":
    # Bybit 永續（perpetual / swap）
    exchange = ccxt.bybit({
        "enableRateLimit": True,
        "options": {
            "defaultType": "swap",  # 永續請用 swap
        },
    })

    base = "XAUT"
    symbol = resolve_bybit_swap_symbol(exchange, base, "USDT")
    print("Resolved symbol:", symbol)

    df, csv_path, pq_path = download_ohlcv_ccxt(
        symbol=symbol,
        timeframe="5m",
        since="2025-04-01T00:00:00Z",
        until="2026-01-24T00:00:00Z",
        exchange=exchange,
        save_dir="data",
        mark="bybit_swap",
    )

    print("CSV:", csv_path)
    print("Parquet:", pq_path)
