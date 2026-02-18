from __future__ import annotations

from datetime import timedelta

import FinanceDataReader as fdr
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed

from src.providers.base import DataProvider


class FdrDailyProvider(DataProvider):
    def __init__(self) -> None:
        self._stock_listing_cache: dict[str, pd.DataFrame] = {}

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    def _listing(self, market: str) -> pd.DataFrame:
        if market not in self._stock_listing_cache:
            self._stock_listing_cache[market] = fdr.StockListing(market)
        return self._stock_listing_cache[market]

    def get_universe(self, universe_spec: str) -> list[dict]:
        markets = [x.strip().upper() for x in universe_spec.split(",") if x.strip()]
        rows: list[dict] = []
        for market in markets:
            df = self._listing(market)
            for _, row in df[["Code", "Name"]].head(300).iterrows():
                rows.append({"ticker": str(row["Code"]), "name": str(row["Name"]), "market": market})
        return rows

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    def _daily(self, ticker: str, days: int = 80) -> pd.DataFrame:
        end = pd.Timestamp.today().normalize()
        start = end - timedelta(days=days * 2)
        df = fdr.DataReader(ticker, start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"))
        df = df.tail(days).copy()
        if df.empty:
            return pd.DataFrame()
        df = df.rename(columns=str.lower)
        df["dt"] = pd.to_datetime(df.index)
        df["ticker"] = ticker
        if "value" not in df.columns:
            df["value"] = df["close"] * df["volume"]
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
        for market in ["KOSPI", "KOSDAQ"]:
            df = self._listing(market)
            if "Sector" not in df.columns:
                continue
            sub = df[df["Code"].astype(str).isin(tickers)][["Code", "Sector"]]
            for _, row in sub.iterrows():
                out[str(row["Code"])] = str(row["Sector"] or "UNKNOWN")
        return out
