from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import pandas as pd

from src.core import db
from src.core.timeutil import kst_iso, now_kst


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _max_drawdown(nav_series: pd.Series) -> float:
    if nav_series.empty:
        return 0.0
    running_max = nav_series.cummax()
    dd = nav_series / running_max - 1.0
    return abs(float(dd.min()))


def _fetch_nav_frame(sqlite_path: str, lookback_days: int, now: datetime) -> pd.DataFrame:
    rows = db.fetchall(
        sqlite_path,
        """
        SELECT ts_kst, nav, cash
        FROM paper_accounts
        ORDER BY account_id ASC
        """,
    )
    if not rows:
        return pd.DataFrame(columns=["ts_kst", "ts", "nav", "cash"])

    df = pd.DataFrame([{"ts_kst": str(r["ts_kst"]), "nav": _safe_float(r["nav"]), "cash": _safe_float(r["cash"])} for r in rows])
    df["ts"] = pd.to_datetime(df["ts_kst"], errors="coerce", utc=True).dt.tz_convert("Asia/Seoul")
    cutoff = pd.Timestamp(now)
    if cutoff.tzinfo is None:
        cutoff = cutoff.tz_localize("Asia/Seoul")
    else:
        cutoff = cutoff.tz_convert("Asia/Seoul")
    cutoff = cutoff - pd.Timedelta(days=max(1, int(lookback_days)))
    df = df[df["ts"] >= cutoff].copy()
    df = df.sort_values("ts")
    return df


def _fetch_order_stats(sqlite_path: str, lookback_days: int) -> tuple[int, int]:
    total_row = db.fetchone(sqlite_path, "SELECT COUNT(*) AS n FROM paper_orders")
    lookback_row = db.fetchone(
        sqlite_path,
        """
        SELECT COUNT(*) AS n
        FROM paper_orders
        WHERE ts_kst >= datetime('now', ?)
        """,
        (f"-{max(1, int(lookback_days))} day",),
    )
    return _safe_int(total_row["n"] if total_row else 0), _safe_int(lookback_row["n"] if lookback_row else 0)


def _fetch_outcome_stats(sqlite_path: str, lookback_days: int) -> tuple[int, float, float]:
    rows = db.fetchall(
        sqlite_path,
        """
        SELECT o.ret
        FROM outcomes o
        JOIN runs r ON r.run_id = o.run_id
        WHERE o.horizon='1d'
          AND r.ts_kst >= datetime('now', ?)
        """,
        (f"-{max(1, int(lookback_days))} day",),
    )
    if not rows:
        return 0, 0.0, 0.0
    rets = [_safe_float(r["ret"]) for r in rows]
    n = len(rets)
    avg = sum(rets) / n
    win = sum(1 for x in rets if x > 0) / n
    return n, avg, win


def _risk_plan(level: str, base_risk_per_trade_pct: float, base_daily_loss_pct: float, base_max_new_positions: int) -> dict[str, Any]:
    if level == "READY":
        mult = 1.0
        mode = "manual_live_small"
    elif level == "WATCH":
        mult = 0.6
        mode = "paper_plus_small_probe"
    else:
        mult = 0.35
        mode = "paper_only"
    risk = round(max(0.1, base_risk_per_trade_pct * mult), 2)
    day_loss = round(max(0.3, base_daily_loss_pct * mult), 2)
    max_new = max(1, int(round(max(1, base_max_new_positions) * mult)))
    return {
        "mode": mode,
        "risk_per_trade_pct": risk,
        "daily_loss_limit_pct": day_loss,
        "max_new_positions": max_new,
    }


def _level_text(level: str) -> str:
    if level == "READY":
        return "실전 가능(소액·수동)"
    if level == "WATCH":
        return "병행 준비(모의+소액 검증)"
    return "트레이닝 필요(모의 유지)"


def build_training_report(
    sqlite_path: str,
    *,
    lookback_days: int,
    min_days: int,
    min_trades: int,
    target_return: float,
    max_drawdown_limit: float,
    base_risk_per_trade_pct: float,
    base_daily_loss_pct: float,
    base_max_new_positions: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    ts = now or now_kst()
    nav_df = _fetch_nav_frame(sqlite_path, lookback_days, ts)
    order_total, order_lookback = _fetch_order_stats(sqlite_path, lookback_days)
    out_n, out_avg, out_win = _fetch_outcome_stats(sqlite_path, lookback_days)

    if nav_df.empty:
        return {
            "ts_kst": kst_iso(ts),
            "lookback_days": int(lookback_days),
            "score": 0.0,
            "level": "TRAINING",
            "level_text": _level_text("TRAINING"),
            "ready": False,
            "metrics": {
                "history_days": 0,
                "order_total": order_total,
                "order_lookback": order_lookback,
                "cumulative_return": 0.0,
                "max_drawdown": 0.0,
                "daily_win_rate": 0.0,
                "outcome_n": out_n,
                "outcome_avg_ret_1d": out_avg,
                "outcome_win_rate_1d": out_win,
            },
            "gates": [
                {"key": "history_days", "label": f"기록 일수 >= {min_days}일", "pass": False, "value": 0, "target": min_days},
                {"key": "order_count", "label": f"모의 체결 >= {min_trades}건", "pass": False, "value": order_total, "target": min_trades},
            ],
            "risk_plan": _risk_plan("TRAINING", base_risk_per_trade_pct, base_daily_loss_pct, base_max_new_positions),
            "checklist": [
                "모의 투자 데이터가 부족합니다. 최소 2주 이상 기록을 먼저 쌓으세요.",
                "체결 사유(reason)와 손절/익절 로그를 함께 점검하세요.",
            ],
        }

    history_days = int(nav_df["ts"].dt.date.nunique())
    nav_series = nav_df["nav"].astype(float)
    nav_first = float(nav_series.iloc[0])
    nav_last = float(nav_series.iloc[-1])
    cumulative_return = (nav_last / nav_first - 1.0) if nav_first > 0 else 0.0
    max_drawdown = _max_drawdown(nav_series)

    daily = (
        nav_df.assign(day=nav_df["ts"].dt.date)
        .sort_values("ts")
        .groupby("day", as_index=False)
        .tail(1)
        .sort_values("ts")
    )
    daily_ret = daily["nav"].pct_change().dropna()
    daily_win = float((daily_ret > 0).mean()) if not daily_ret.empty else 0.0

    sample_score = min(1.0, history_days / max(1, min_days)) * 18.0 + min(1.0, order_total / max(1, min_trades)) * 12.0
    perf_score = _clip((cumulative_return + 0.03) / 0.10, 0.0, 1.0) * 25.0
    dd_score = _clip((max_drawdown_limit * 1.5 - max_drawdown) / max(1e-6, max_drawdown_limit * 1.5), 0.0, 1.0) * 20.0
    out_win_for_score = out_win if out_n >= 10 else daily_win
    consistency_score = _clip((daily_win - 0.45) / 0.20, 0.0, 1.0) * 15.0 + _clip((out_win_for_score - 0.45) / 0.20, 0.0, 1.0) * 10.0
    score = round(sample_score + perf_score + dd_score + consistency_score, 1)

    gate_history = history_days >= min_days
    gate_orders = order_total >= min_trades
    gate_return = cumulative_return >= target_return
    gate_dd = max_drawdown <= max_drawdown_limit
    gate_daily_win = daily_win >= 0.50
    gate_outcome_win = (out_n < 20) or (out_win >= 0.50)

    ready = bool(gate_history and gate_orders and gate_return and gate_dd and gate_daily_win and gate_outcome_win)
    if ready and score >= 75.0:
        level = "READY"
    elif score >= 60.0 and gate_history and gate_orders:
        level = "WATCH"
    else:
        level = "TRAINING"

    risk_plan = _risk_plan(level, base_risk_per_trade_pct, base_daily_loss_pct, base_max_new_positions)

    gates = [
        {"key": "history_days", "label": f"기록 일수 >= {min_days}일", "pass": gate_history, "value": history_days, "target": min_days},
        {"key": "order_count", "label": f"모의 체결 >= {min_trades}건", "pass": gate_orders, "value": order_total, "target": min_trades},
        {"key": "cum_return", "label": f"누적수익률 >= {target_return:.1%}", "pass": gate_return, "value": cumulative_return, "target": target_return},
        {"key": "max_dd", "label": f"최대낙폭 <= {max_drawdown_limit:.1%}", "pass": gate_dd, "value": max_drawdown, "target": max_drawdown_limit},
        {"key": "daily_win", "label": "일간 승률 >= 50%", "pass": gate_daily_win, "value": daily_win, "target": 0.50},
        {"key": "outcome_win", "label": "후행 승률(표본>=20 시) >= 50%", "pass": gate_outcome_win, "value": out_win, "target": 0.50},
    ]

    checklist = [
        "실전 주문은 반드시 수동 확인 후 실행(자동 주문 OFF 유지).",
        f"1회 손실 위험은 계좌의 {risk_plan['risk_per_trade_pct']:.2f}% 이내로 제한.",
        f"일중 손실 {risk_plan['daily_loss_limit_pct']:.2f}% 도달 시 당일 신규 진입 중단.",
        "신규 진입 전: 스코어, 손절 기준, 뉴스/이벤트 리스크를 3점 체크.",
        "주간 단위로 모의 vs 실전(소액) 성과 괴리를 점검하고 파라미터를 재보정.",
    ]
    if level != "READY":
        checklist.append("준비도 GREEN 전까지는 모의/소액 병행만 허용.")

    return {
        "ts_kst": kst_iso(ts),
        "lookback_days": int(lookback_days),
        "score": score,
        "level": level,
        "level_text": _level_text(level),
        "ready": ready,
        "metrics": {
            "history_days": history_days,
            "order_total": order_total,
            "order_lookback": order_lookback,
            "cumulative_return": cumulative_return,
            "max_drawdown": max_drawdown,
            "daily_win_rate": daily_win,
            "daily_ret_mean": float(daily_ret.mean()) if not daily_ret.empty else 0.0,
            "daily_ret_std": float(daily_ret.std()) if not daily_ret.empty else 0.0,
            "outcome_n": out_n,
            "outcome_avg_ret_1d": out_avg,
            "outcome_win_rate_1d": out_win,
            "nav_start": nav_first,
            "nav_end": nav_last,
        },
        "gates": gates,
        "risk_plan": risk_plan,
        "checklist": checklist,
    }


def save_training_report(sqlite_path: str, report: dict[str, Any], mode: str, note: str = "") -> int:
    metrics = json.dumps(report.get("metrics", {}), ensure_ascii=True)
    checklist = json.dumps(
        {
            "gates": report.get("gates", []),
            "risk_plan": report.get("risk_plan", {}),
            "checklist": report.get("checklist", []),
        },
        ensure_ascii=False,
    )
    return db.execute(
        sqlite_path,
        """
        INSERT INTO training_reports(
            ts_kst, mode, score, level, ready, metrics_json, checklist_json, note
        ) VALUES (?,?,?,?,?,?,?,?)
        """,
        (
            str(report.get("ts_kst") or kst_iso()),
            str(mode),
            _safe_float(report.get("score")),
            str(report.get("level") or "TRAINING"),
            1 if bool(report.get("ready")) else 0,
            metrics,
            checklist,
            str(note or ""),
        ),
    )


def load_recent_training_reports(sqlite_path: str, limit: int = 5) -> list[dict[str, Any]]:
    rows = db.fetchall(
        sqlite_path,
        """
        SELECT report_id, ts_kst, mode, score, level, ready, metrics_json, checklist_json, note
        FROM training_reports
        ORDER BY report_id DESC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    )
    out: list[dict[str, Any]] = []
    for r in rows:
        try:
            metrics = json.loads(str(r["metrics_json"] or "{}"))
        except Exception:
            metrics = {}
        out.append(
            {
                "report_id": int(r["report_id"]),
                "ts_kst": str(r["ts_kst"]),
                "mode": str(r["mode"]),
                "score": _safe_float(r["score"]),
                "level": str(r["level"]),
                "ready": bool(int(r["ready"])),
                "metrics": metrics,
                "note": str(r["note"] or ""),
            }
        )
    return out
