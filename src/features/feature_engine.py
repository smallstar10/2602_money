from __future__ import annotations

import pandas as pd

from src.features.indicators import atr, moving_average


def _safe_ratio(a: float, b: float) -> float:
    if b == 0 or pd.isna(b):
        return 0.0
    return float(a / b)


def _efficiency_ratio(close: pd.Series, window: int = 8) -> float:
    if len(close) <= window:
        return 0.0
    net = float(abs(close.iloc[-1] - close.iloc[-1 - window]))
    steps = close.diff().abs().tail(window).sum()
    if pd.isna(steps) or float(steps) == 0.0:
        return 0.0
    return float(net / float(steps))


def build_features(
    ohlcv: pd.DataFrame,
    sector_map: dict[str, str],
    investor_flow: pd.DataFrame | None = None,
    buzz_score: dict[str, float] | None = None,
    lookback: int = 20,
) -> pd.DataFrame:
    if ohlcv.empty:
        return pd.DataFrame()

    buzz_score = buzz_score or {}
    flow_map = {}
    if investor_flow is not None and not investor_flow.empty:
        flow_map = {str(r["ticker"]): float(r["flow_score"]) for _, r in investor_flow.iterrows()}

    ohlcv = ohlcv.sort_values(["ticker", "dt"]).copy()
    last_ret = ohlcv.groupby("ticker")["close"].pct_change().fillna(0.0)
    ohlcv["ret"] = last_ret

    sector_breadth_map: dict[str, float] = {}
    sector_rotation_map: dict[str, float] = {}
    for sector in set(sector_map.values()):
        tickers = [k for k, v in sector_map.items() if v == sector]
        sub = ohlcv[ohlcv["ticker"].isin(tickers)]
        if sub.empty:
            sector_breadth_map[sector] = 0.5
            sector_rotation_map[sector] = 0.0
            continue
        last = sub.groupby("ticker").tail(1)
        breadth = float((last["ret"] > 0).mean())
        sector_breadth_map[sector] = breadth

        ret_vals: list[float] = []
        value_surge_vals: list[float] = []
        for _, g in sub.groupby("ticker"):
            g = g.sort_values("dt")
            if len(g) < 3:
                continue
            ret_vals.append(float(g["close"].pct_change().iloc[-1]))
            prev_mean = float(g["value"].tail(lookback + 1).iloc[:-1].mean() or 0.0)
            value_surge_vals.append(_safe_ratio(float(g["value"].iloc[-1]), prev_mean))
        avg_ret = float(sum(ret_vals) / len(ret_vals)) if ret_vals else 0.0
        avg_value_surge = float(sum(value_surge_vals) / len(value_surge_vals)) if value_surge_vals else 1.0
        # Positive when sector has synchronized advance with fresh turnover.
        sector_rotation_map[sector] = 0.5 * avg_ret + 0.3 * (breadth - 0.5) + 0.2 * (avg_value_surge - 1.0)

    rows: list[dict] = []
    for ticker, sub in ohlcv.groupby("ticker"):
        sub = sub.sort_values("dt").copy()
        if len(sub) < 5:
            continue

        ma20 = moving_average(sub["close"], 20)
        atr14 = atr(sub, 14)
        latest = sub.iloc[-1]

        mean_value = sub["value"].tail(lookback + 1).iloc[:-1].mean()
        mean_volume = sub["volume"].tail(lookback + 1).iloc[:-1].mean()
        latest_atr = atr14.iloc[-1] if len(atr14) else 0.0
        mean_atr = atr14.tail(lookback + 1).iloc[:-1].mean() if len(atr14) > 1 else 0.0
        ma_latest = ma20.iloc[-1] if len(ma20) else latest["close"]
        close = sub["close"].astype(float)
        ret = close.pct_change().fillna(0.0)
        ma60 = moving_average(close, 60)
        rs_5 = float(close.iloc[-1] / close.iloc[-6] - 1.0) if len(close) >= 6 else 0.0
        positive_ratio_6 = float((ret.tail(6) > 0).mean()) if len(ret) >= 6 else float((ret > 0).mean())
        drawdown_20 = float(close.iloc[-1] / close.tail(20).max() - 1.0) if len(close) >= 2 else 0.0
        vol_short = float(ret.tail(5).std() or 0.0)
        vol_long = float(ret.tail(20).std() or 0.0)
        volatility_shock = _safe_ratio(vol_short, vol_long)
        ma60_latest = float(ma60.iloc[-1]) if len(ma60) and pd.notna(ma60.iloc[-1]) else float(ma_latest)
        trend_strength = 0.5 * float((close.iloc[-1] - ma_latest) / ma_latest) if ma_latest else 0.0
        trend_strength += 0.5 * float((ma_latest - ma60_latest) / ma60_latest) if ma60_latest else 0.0
        prev_high_20 = float(sub["high"].tail(21).iloc[:-1].max()) if len(sub) >= 3 else float(close.iloc[-1])
        prev_low_20 = float(sub["low"].tail(21).iloc[:-1].min()) if len(sub) >= 3 else float(close.iloc[-1])
        breakout_20 = float((close.iloc[-1] - prev_high_20) / prev_high_20) if prev_high_20 else 0.0
        range_denom = float(prev_high_20 - prev_low_20)
        range_position_20 = float((close.iloc[-1] - prev_low_20) / range_denom) if range_denom > 0 else 0.5
        efficiency_8 = _efficiency_ratio(close, 8)

        sector = sector_map.get(ticker, "UNKNOWN")
        rows.append(
            {
                "ticker": ticker,
                "price": float(latest["close"]),
                "money_value_surge": _safe_ratio(float(latest["value"]), float(mean_value if pd.notna(mean_value) else 0.0)),
                "volume_surge": _safe_ratio(float(latest["volume"]), float(mean_volume if pd.notna(mean_volume) else 0.0)),
                "ma_trend": float((latest["close"] - ma_latest) / ma_latest) if ma_latest else 0.0,
                "atr_regime": _safe_ratio(float(latest_atr if pd.notna(latest_atr) else 0.0), float(mean_atr if pd.notna(mean_atr) else 0.0)),
                "return_1h": float(sub["close"].pct_change().iloc[-1]) if len(sub) > 1 else 0.0,
                "rs_5": rs_5,
                "momentum_persistence": positive_ratio_6,
                "drawdown_20": drawdown_20,
                "volatility_shock": volatility_shock,
                "trend_strength": trend_strength,
                "breakout_20": breakout_20,
                "range_position_20": range_position_20,
                "efficiency_8": efficiency_8,
                "sector": sector,
                "sector_breadth": sector_breadth_map.get(sector, 0.5),
                "sector_rotation": float(sector_rotation_map.get(sector, 0.0)),
                "flow_score": flow_map.get(ticker, 0.0),
                "buzz_score": float(buzz_score.get(ticker, 0.0)),
                "value_latest": float(latest["value"]),
            }
        )

    return pd.DataFrame(rows)
