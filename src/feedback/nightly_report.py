from __future__ import annotations

import json

import pandas as pd

from src.core import db


def build_nightly_stats(sqlite_path: str, lookback_days: int = 7) -> dict:
    rows = db.fetchall(
        sqlite_path,
        """
        SELECT ret FROM outcomes
        WHERE horizon='1d'
          AND run_id IN (
            SELECT run_id FROM runs
            WHERE ts_kst >= datetime('now', '-7 day')
          )
        """,
    )
    if not rows:
        return {"avg_ret_1d": 0.0, "win_rate_1d": 0.0, "n": 0}

    rets = [float(r["ret"]) for r in rows]
    avg = sum(rets) / len(rets)
    win = sum(1 for r in rets if r > 0) / len(rets)
    return {"avg_ret_1d": avg, "win_rate_1d": win, "n": len(rets)}


def _factor_strength_label(ic: float, spread: float) -> str:
    return f"IC {ic:+.3f}, Q5-Q1 {spread:+.2%}"


def build_factor_diagnostics(sqlite_path: str, lookback_days: int = 21, max_rows: int = 1400) -> dict:
    rows = db.fetchall(
        sqlite_path,
        """
        SELECT o.ret, c.features_json
        FROM outcomes o
        JOIN candidates c ON c.run_id = o.run_id AND c.ticker = o.ticker
        JOIN runs r ON r.run_id = o.run_id
        WHERE o.horizon='1d'
          AND r.ts_kst >= datetime('now', ?)
        ORDER BY o.run_id DESC
        LIMIT ?
        """,
        (f"-{int(lookback_days)} day", int(max_rows)),
    )
    if len(rows) < 40:
        return {"factor_top": "표본 부족", "factor_bottom": "표본 부족"}

    recs: list[dict] = []
    for r in rows:
        try:
            feat = json.loads(r["features_json"] or "{}")
        except Exception:
            feat = {}
        feat["ret"] = float(r["ret"])
        recs.append(feat)
    df = pd.DataFrame(recs)
    if "ret" not in df.columns:
        return {"factor_top": "ret 없음", "factor_bottom": "ret 없음"}

    keys = [c for c in df.columns if c != "ret" and pd.api.types.is_numeric_dtype(df[c])]
    if not keys:
        return {"factor_top": "팩터 없음", "factor_bottom": "팩터 없음"}

    scores: list[tuple[str, float, float, int]] = []
    for k in keys:
        sub = df[[k, "ret"]].dropna()
        if len(sub) < 30 or sub[k].nunique() < 5:
            continue
        ic = float(sub[k].corr(sub["ret"], method="spearman"))
        if pd.isna(ic):
            ic = 0.0
        q_low = float(sub[k].quantile(0.2))
        q_high = float(sub[k].quantile(0.8))
        low_ret = float(sub[sub[k] <= q_low]["ret"].mean()) if (sub[k] <= q_low).any() else 0.0
        high_ret = float(sub[sub[k] >= q_high]["ret"].mean()) if (sub[k] >= q_high).any() else 0.0
        spread = high_ret - low_ret
        composite = 0.65 * ic + 0.35 * max(-0.1, min(0.1, spread)) / 0.1
        scores.append((k, composite, ic, spread))

    if not scores:
        return {"factor_top": "유효 팩터 없음", "factor_bottom": "유효 팩터 없음"}

    scores.sort(key=lambda x: x[1], reverse=True)
    top_name, _, top_ic, top_spread = scores[0]
    bot_name, _, bot_ic, bot_spread = scores[-1]
    return {
        "factor_top": f"{top_name} ({_factor_strength_label(top_ic, top_spread)})",
        "factor_bottom": f"{bot_name} ({_factor_strength_label(bot_ic, bot_spread)})",
    }


def build_paper_stats(sqlite_path: str) -> dict:
    row = db.fetchone(sqlite_path, "SELECT account_id, ts_kst, cash, nav FROM paper_accounts ORDER BY account_id DESC LIMIT 1")
    prev = db.fetchone(sqlite_path, "SELECT nav FROM paper_accounts ORDER BY account_id DESC LIMIT 1 OFFSET 1")
    trades = db.fetchone(sqlite_path, "SELECT COUNT(*) AS n FROM paper_orders WHERE ts_kst LIKE date('now', '+9 hours') || '%'")
    if row is None:
        return {"paper_nav": 0.0, "paper_cash": 0.0, "paper_pnl_day": 0.0, "paper_trades_today": 0}
    nav = float(row["nav"])
    prev_nav = float(prev["nav"]) if prev else nav
    return {
        "paper_nav": nav,
        "paper_cash": float(row["cash"]),
        "paper_pnl_day": nav - prev_nav,
        "paper_trades_today": int(trades["n"] if trades else 0),
    }
