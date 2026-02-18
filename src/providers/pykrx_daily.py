from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
from pykrx import stock
from tenacity import retry, stop_after_attempt, wait_fixed

from src.providers.base import DataProvider


class PykrxDailyProvider(DataProvider):
    def get_universe(self, universe_spec: str) -> list[dict]:
        markets = [x.strip().upper() for x in universe_spec.split(",") if x.strip()]
        rows: list[dict] = []
        for market in markets:
            market_name = "KOSPI" if market == "KOSPI" else "KOSDAQ"
            tickers = stock.get_market_ticker_list(market=market_name)
            for ticker in tickers[:300]:
                rows.append(
                    {
                        "ticker": ticker,
                        "name": stock.get_market_ticker_name(ticker),
                        "market": market_name,
                    }
                )
        return rows

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    def _daily(self, ticker: str, days: int = 80) -> pd.DataFrame:
        end = datetime.now()
        start = end - timedelta(days=days * 2)
        df = stock.get_market_ohlcv_by_date(start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), ticker)
        if df.empty:
            return pd.DataFrame()
        df = df.tail(days).copy()
        df.columns = ["open", "high", "low", "close", "volume", "value", "change"]
        df["dt"] = pd.to_datetime(df.index)
        df["ticker"] = ticker
        return df[["ticker", "dt", "open", "high", "low", "close", "volume", "value"]]

    def get_latest_ohlcv(self, tickers: list[str], interval: str = "60m") -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for ticker in tickers:
            df = self._daily(ticker)
            if not df.empty:
                frames.append(df)
        if not frames:
            return pd.DataFrame(columns=["ticker", "dt", "open", "high", "low", "close", "volume", "value"])
        return pd.concat(frames, ignore_index=True)

    def get_investor_flow(self, tickers: list[str], window: int = 20) -> pd.DataFrame:
        return pd.DataFrame({"ticker": tickers, "flow_score": [0.0] * len(tickers)})

    def get_sector_map(self, tickers: list[str]) -> dict[str, str]:
        out: dict[str, str] = {}
        for ticker in tickers:
            try:
                out[ticker] = stock.get_market_ticker_name(ticker)[:2]
            except Exception:
                out[ticker] = "UNKNOWN"
        return out
