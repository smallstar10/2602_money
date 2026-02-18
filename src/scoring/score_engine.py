from __future__ import annotations

import numpy as np
import pandas as pd

from src.scoring.schema import SCORE_SCALE_MAP


def _clip_scale(value: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    return float(np.clip((value - lo) / (hi - lo), 0.0, 1.0))


def score_candidates(features: pd.DataFrame, weights: dict[str, float]) -> pd.DataFrame:
    if features.empty:
        return features

    df = features.copy()
    for raw_key, (lo, hi, norm_key) in SCORE_SCALE_MAP.items():
        if raw_key not in df.columns:
            df[norm_key] = 0.0
            continue
        df[norm_key] = df[raw_key].apply(lambda x: _clip_scale(x, lo, hi))

    raw = pd.Series(0.0, index=df.index, dtype=float)
    for raw_key, (_, _, norm_key) in SCORE_SCALE_MAP.items():
        raw += float(weights.get(raw_key, 0.0)) * df[norm_key]

    df["score"] = (raw * 100.0).round(2)
    return df.sort_values("score", ascending=False)
