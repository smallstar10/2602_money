from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.core import db
from src.core.timeutil import kst_iso


@dataclass
class LabResult:
    params: dict[str, Any]
    n_runs: int
    avg_ret: float
    win_rate: float
    vol: float
    objective: float


def _calc_metrics(run_rets: list[float]) -> tuple[float, float, float]:
    if not run_rets:
        return 0.0, 0.0, 0.0
    s = pd.Series(run_rets, dtype=float)
    avg = float(s.mean())
    win = float((s > 0).mean())
    vol = float(s.std(ddof=0) if len(s) > 1 else 0.0)
    return avg, win, vol


def _objective(avg_ret: float, win_rate: float, vol: float) -> float:
    downside = max(0.0, vol - 0.008)
    return 0.70 * avg_ret + 0.25 * (win_rate - 0.5) - 0.20 * downside


def _load_dataset(sqlite_path: str, limit_rows: int = 5000) -> pd.DataFrame:
    rows = db.fetchall(
        sqlite_path,
        """
        SELECT c.run_id, c.ticker, c.score, o.ret
        FROM candidates c
        JOIN outcomes o ON o.run_id = c.run_id AND o.ticker = c.ticker
        WHERE o.horizon='1d'
        ORDER BY c.run_id DESC
        LIMIT ?
        """,
        (int(limit_rows),),
    )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows])


def run_strategy_lab(sqlite_path: str, min_runs: int = 25) -> dict[str, Any]:
    df = _load_dataset(sqlite_path)
    if df.empty:
        return {"status": "OFF(no-data)", "summary": "표본 없음"}

    thresholds = [48.0, 52.0, 55.0, 58.0, 62.0]
    max_positions_set = [1, 2, 3, 4]

    by_run: dict[int, list[tuple[float, float]]] = defaultdict(list)
    for _, r in df.iterrows():
        by_run[int(r["run_id"])].append((float(r["score"]), float(r["ret"])))

    results: list[LabResult] = []
    for th in thresholds:
        for max_pos in max_positions_set:
            run_rets: list[float] = []
            for run_id, rows in by_run.items():
                selected = sorted([x for x in rows if x[0] >= th], key=lambda x: x[0], reverse=True)[:max_pos]
                if not selected:
                    continue
                run_rets.append(sum(x[1] for x in selected) / len(selected))
            if len(run_rets) < min_runs:
                continue
            avg, win, vol = _calc_metrics(run_rets)
            obj = _objective(avg, win, vol)
            results.append(
                LabResult(
                    params={"entry_score_threshold": th, "max_positions": max_pos},
                    n_runs=len(run_rets),
                    avg_ret=avg,
                    win_rate=win,
                    vol=vol,
                    objective=obj,
                )
            )

    if not results:
        return {"status": f"OFF(sample<{min_runs})", "summary": "유효 조합 없음"}

    results.sort(key=lambda x: x.objective, reverse=True)
    best = results[0]

    db.execute(sqlite_path, "UPDATE strategy_experiments SET active=0 WHERE active=1")
    db.execute(
        sqlite_path,
        "INSERT INTO strategy_experiments(ts_kst, params_json, metrics_json, objective, active) VALUES (?,?,?,?,1)",
        (
            kst_iso(),
            json.dumps(best.params, ensure_ascii=True),
            json.dumps(
                {
                    "n_runs": best.n_runs,
                    "avg_ret": best.avg_ret,
                    "win_rate": best.win_rate,
                    "vol": best.vol,
                },
                ensure_ascii=True,
            ),
            float(best.objective),
        ),
    )

    return {
        "status": "ON",
        "summary": (
            f"th {best.params['entry_score_threshold']:.1f}, pos {best.params['max_positions']} | "
            f"avg {best.avg_ret:+.3%}, win {best.win_rate:.1%}, vol {best.vol:.3%}, n={best.n_runs}"
        ),
        "best_entry_score_threshold": float(best.params["entry_score_threshold"]),
        "best_max_positions": int(best.params["max_positions"]),
    }


def latest_strategy_lab(sqlite_path: str) -> dict[str, Any]:
    row = db.fetchone(
        sqlite_path,
        """
        SELECT params_json, metrics_json, objective
        FROM strategy_experiments
        WHERE active=1
        ORDER BY exp_id DESC
        LIMIT 1
        """,
    )
    if row is None:
        return {"status": "N/A", "summary": "이력 없음"}
    try:
        params = json.loads(row["params_json"] or "{}")
        m = json.loads(row["metrics_json"] or "{}")
    except Exception:
        return {"status": "N/A", "summary": "파싱 실패"}
    return {
        "status": "ON",
        "summary": (
            f"th {float(params.get('entry_score_threshold', math.nan)):.1f}, "
            f"pos {int(params.get('max_positions', 0))} | "
            f"avg {float(m.get('avg_ret', 0.0)):+.3%}, "
            f"win {float(m.get('win_rate', 0.0)):.1%}, n={int(m.get('n_runs', 0))}"
        ),
    }

