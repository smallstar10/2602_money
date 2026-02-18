from __future__ import annotations

from src.core import db
from src.core.timeutil import kst_iso


REGIME_PARAMS = {
    "CONSERVATIVE": {"entry_score_threshold": 62.0, "position_scale": 0.6},
    "NEUTRAL": {"entry_score_threshold": 55.0, "position_scale": 1.0},
    "AGGRESSIVE": {"entry_score_threshold": 50.0, "position_scale": 1.25},
}


def load_strategy_state(sqlite_path: str) -> dict:
    row = db.fetchone(
        sqlite_path,
        """
        SELECT regime, entry_score_threshold, position_scale, note
        FROM strategy_state
        WHERE active=1
        ORDER BY state_id DESC
        LIMIT 1
        """,
    )
    if row is None:
        p = REGIME_PARAMS["NEUTRAL"]
        return {
            "regime": "NEUTRAL",
            "entry_score_threshold": p["entry_score_threshold"],
            "position_scale": p["position_scale"],
            "note": "default",
        }
    return {
        "regime": str(row["regime"]),
        "entry_score_threshold": float(row["entry_score_threshold"]),
        "position_scale": float(row["position_scale"]),
        "note": str(row["note"] or ""),
    }


def _decide_regime(stats: dict) -> tuple[str, str]:
    n = int(stats.get("n", 0))
    win = float(stats.get("win_rate_1d", 0.0))
    avg = float(stats.get("avg_ret_1d", 0.0))
    pnl_day = float(stats.get("paper_pnl_day", 0.0))

    if n >= 20 and win >= 0.58 and avg >= 0.002 and pnl_day >= 0:
        return "AGGRESSIVE", "strong edge"
    if n >= 20 and (win <= 0.45 or avg <= -0.0015 or pnl_day < 0):
        return "CONSERVATIVE", "drawdown control"
    return "NEUTRAL", "balanced"


def update_strategy_state(sqlite_path: str, stats: dict) -> tuple[dict, str]:
    current = load_strategy_state(sqlite_path)
    regime, reason = _decide_regime(stats)
    p = REGIME_PARAMS[regime]

    changed = (
        current["regime"] != regime
        or abs(current["entry_score_threshold"] - p["entry_score_threshold"]) > 1e-9
        or abs(current["position_scale"] - p["position_scale"]) > 1e-9
    )

    if changed:
        db.execute(sqlite_path, "UPDATE strategy_state SET active=0 WHERE active=1")
        db.execute(
            sqlite_path,
            """
            INSERT INTO strategy_state(
                ts_kst, regime, entry_score_threshold, position_scale, note, active
            ) VALUES (?,?,?,?,?,1)
            """,
            (kst_iso(), regime, p["entry_score_threshold"], p["position_scale"], reason),
        )

    latest = load_strategy_state(sqlite_path)
    return latest, ("UPDATED" if changed else "UNCHANGED")
