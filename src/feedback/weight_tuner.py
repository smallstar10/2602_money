from __future__ import annotations

import json

import pandas as pd

from src.core import db
from src.scoring.weights import activate_new_weights


def _winsorize(s: pd.Series, lo: float = 0.03, hi: float = 0.97) -> pd.Series:
    if s.empty:
        return s
    ql = float(s.quantile(lo))
    qh = float(s.quantile(hi))
    return s.clip(lower=ql, upper=qh)


def tune_weights(
    sqlite_path: str,
    base_weights: dict[str, float],
    max_delta: float = 0.03,
    min_samples: int = 60,
    warmup_days: int = 14,
) -> tuple[dict[str, float], str]:
    days_row = db.fetchone(
        sqlite_path,
        """
        SELECT COUNT(DISTINCT substr(r.ts_kst, 1, 10)) AS n_days
        FROM outcomes o
        JOIN runs r ON r.run_id = o.run_id
        WHERE o.horizon='1d'
        """,
    )
    n_days = int(days_row["n_days"]) if days_row else 0
    if n_days < warmup_days:
        return base_weights, f"OFF(warmup<{warmup_days}d)"

    rows = db.fetchall(
        sqlite_path,
        """
        SELECT o.ret, c.features_json
        FROM outcomes o
        JOIN candidates c ON c.run_id = o.run_id AND c.ticker = o.ticker
        WHERE o.horizon='1d'
        ORDER BY o.run_id DESC
        LIMIT 400
        """,
    )
    if len(rows) < min_samples:
        return base_weights, f"OFF(sample<{min_samples})"

    recs = []
    for r in rows:
        try:
            feat = json.loads(r["features_json"] or "{}")
        except Exception:
            feat = {}
        feat["ret"] = float(r["ret"])
        recs.append(feat)
    df = pd.DataFrame(recs).dropna(axis=1, how="all")
    if "ret" not in df.columns:
        return base_weights, "OFF(no-ret)"

    updated = base_weights.copy()
    for key in base_weights.keys():
        if key not in df.columns or not pd.api.types.is_numeric_dtype(df[key]):
            continue
        sub = df[[key, "ret"]].dropna()
        if len(sub) < 30 or sub[key].nunique() < 5:
            continue
        feat = _winsorize(sub[key])
        ret = _winsorize(sub["ret"])
        ic = float(feat.corr(ret, method="spearman"))
        if pd.isna(ic):
            ic = 0.0
        q_low = float(feat.quantile(0.2))
        q_high = float(feat.quantile(0.8))
        low_ret = float(ret[feat <= q_low].mean()) if (feat <= q_low).any() else 0.0
        high_ret = float(ret[feat >= q_high].mean()) if (feat >= q_high).any() else 0.0
        spread = high_ret - low_ret
        spread_signal = max(-1.0, min(1.0, spread / 0.05))
        signal = 0.7 * ic + 0.3 * spread_signal
        delta = max(-max_delta, min(max_delta, signal * 0.012))
        updated[key] = max(0.0, updated.get(key, 0.0) + delta)

    s = sum(updated.values())
    if s <= 0:
        return base_weights, "OFF(invalid-sum)"
    normalized = {k: v / s for k, v in updated.items()}
    activate_new_weights(sqlite_path, normalized)
    return normalized, "ON"
