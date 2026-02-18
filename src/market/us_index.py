from __future__ import annotations

from io import StringIO

import pandas as pd
import requests


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _regime(ret_1d: float, ret_5d: float, vol_20: float) -> str:
    if ret_1d > 0.006 and ret_5d > 0.015:
        return "리스크온(상승 추세)"
    if ret_1d < -0.006 and ret_5d < -0.015:
        return "리스크오프(하락 추세)"
    if vol_20 > 0.02:
        return "변동성 확대 구간"
    return "중립/혼조"


def fetch_sp500_snapshot(timeout: int = 10) -> dict | None:
    """Fetch S&P500 daily context from Stooq.

    Returns dict with close, 1d/5d return, 20d vol, regime, risk_score.
    """
    url = "https://stooq.com/q/d/l/?s=^spx&i=d"
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()

    df = pd.read_csv(StringIO(r.text))
    if df.empty or "Close" not in df.columns:
        return None

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date", "Close"]).sort_values("Date").tail(260).copy()
    if len(df) < 25:
        return None

    close = df["Close"].astype(float)
    ret = close.pct_change().fillna(0.0)

    ret_1d = float(ret.iloc[-1])
    ret_5d = float(close.iloc[-1] / close.iloc[-6] - 1.0) if len(close) >= 6 else 0.0
    vol_20 = float(ret.tail(20).std()) if len(ret) >= 20 else float(ret.std())

    # 0~100, 높을수록 위험회피 성격
    risk_raw = 50.0 + (-ret_1d * 900.0) + (-ret_5d * 400.0) + (vol_20 * 1000.0)
    risk_score = _clip(risk_raw, 0.0, 100.0)

    return {
        "date": df.iloc[-1]["Date"].strftime("%Y-%m-%d"),
        "close": float(close.iloc[-1]),
        "ret_1d": ret_1d,
        "ret_5d": ret_5d,
        "vol_20": vol_20,
        "regime": _regime(ret_1d, ret_5d, vol_20),
        "risk_score": risk_score,
    }
