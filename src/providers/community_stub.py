from __future__ import annotations


def get_buzz_score(tickers: list[str]) -> dict[str, float]:
    return {ticker: 0.0 for ticker in tickers}
