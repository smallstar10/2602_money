from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class DataProvider(ABC):
    @abstractmethod
    def get_universe(self, universe_spec: str) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def get_latest_ohlcv(self, tickers: list[str], interval: str = "60m") -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def get_investor_flow(self, tickers: list[str], window: int = 20) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def get_sector_map(self, tickers: list[str]) -> dict[str, str]:
        raise NotImplementedError

    def get_buzz_score(self, tickers: list[str]) -> dict[str, float]:
        return {ticker: 0.0 for ticker in tickers}
