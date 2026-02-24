from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from src.core import db
from src.core.config import Settings
from src.core.timeutil import kst_iso

LIVE_TRADE_STATE_KEY = "live_trading_enabled"


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None or v == "":
            return default
        return int(v)
    except Exception:
        return default


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _state_get(sqlite_path: str, key: str) -> str | None:
    row = db.fetchone(sqlite_path, "SELECT value FROM bot_state WHERE key=?", (key,))
    if row is None:
        return None
    return str(row["value"])


def _state_set(sqlite_path: str, key: str, value: str, ts_kst: datetime | None = None) -> None:
    db.execute(
        sqlite_path,
        """
        INSERT INTO bot_state(key, value, updated_ts_kst)
        VALUES (?,?,?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_ts_kst=excluded.updated_ts_kst
        """,
        (key, value, kst_iso(ts_kst)),
    )


def get_live_trading_enabled(sqlite_path: str, default_on: bool = False) -> bool:
    raw = _state_get(sqlite_path, LIVE_TRADE_STATE_KEY)
    if raw is None:
        if default_on:
            _state_set(sqlite_path, LIVE_TRADE_STATE_KEY, "1")
            return True
        return False
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def set_live_trading_enabled(sqlite_path: str, enabled: bool, ts_kst: datetime | None = None) -> None:
    _state_set(sqlite_path, LIVE_TRADE_STATE_KEY, "1" if enabled else "0", ts_kst=ts_kst)


def _replace_live_positions(sqlite_path: str, positions: list[dict[str, Any]], ts_iso: str) -> None:
    db.execute(sqlite_path, "DELETE FROM live_positions")
    rows: list[tuple[Any, ...]] = []
    for p in positions:
        qty = _safe_int(p.get("qty"))
        if qty <= 0:
            continue
        ticker = str(p.get("ticker") or "").strip()
        if not ticker:
            continue
        rows.append(
            (
                ticker,
                str(p.get("name") or ticker),
                qty,
                _safe_float(p.get("avg_price")),
                _safe_float(p.get("last_price")),
                _safe_float(p.get("eval_amount")),
                _safe_float(p.get("pnl_amount")),
                _safe_float(p.get("pnl_pct")),
                ts_iso,
            )
        )
    db.executemany(
        sqlite_path,
        """
        INSERT INTO live_positions(
            ticker, name, qty, avg_price, last_price, eval_amount, pnl_amount, pnl_pct, updated_ts_kst
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        rows,
    )


def sync_live_snapshot(sqlite_path: str, provider: Any, ts_kst: datetime, note: str = "") -> dict[str, Any]:
    data = provider.inquire_balance()
    cash = _safe_float(data.get("cash"))
    total_eval = _safe_float(data.get("total_eval"))
    total_asset = _safe_float(data.get("total_asset"))
    deposit_cash = _safe_float(data.get("deposit_cash"), cash)
    positions = data.get("positions") or []
    if not isinstance(positions, list):
        positions = []
    ts_iso = kst_iso(ts_kst)
    db.execute(
        sqlite_path,
        "INSERT INTO live_accounts(ts_kst, cash, total_eval, total_asset, note) VALUES (?,?,?,?,?)",
        (ts_iso, cash, total_eval, total_asset, note),
    )
    _replace_live_positions(sqlite_path, positions, ts_iso)
    return {
        "cash": cash,
        "deposit_cash": deposit_cash,
        "total_eval": total_eval,
        "total_asset": total_asset if total_asset > 0 else (cash + total_eval),
        "positions": positions,
    }


def _daily_live_order_count(sqlite_path: str, ts_kst: datetime) -> int:
    prefix = ts_kst.strftime("%Y-%m-%d")
    row = db.fetchone(sqlite_path, "SELECT COUNT(*) AS n FROM live_orders WHERE ts_kst LIKE ?", (prefix + "%",))
    return _safe_int(row["n"]) if row else 0


def _daily_live_order_stats(sqlite_path: str, ts_kst: datetime) -> dict[str, int]:
    prefix = ts_kst.strftime("%Y-%m-%d")
    row = db.fetchone(
        sqlite_path,
        """
        SELECT
          SUM(CASE WHEN status='submitted' THEN 1 ELSE 0 END) AS submitted_n,
          SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed_n,
          COUNT(*) AS total_n
        FROM live_orders
        WHERE ts_kst LIKE ?
        """,
        (prefix + "%",),
    )
    if row is None:
        return {"submitted": 0, "failed": 0, "total": 0}
    return {
        "submitted": _safe_int(row["submitted_n"]),
        "failed": _safe_int(row["failed_n"]),
        "total": _safe_int(row["total_n"]),
    }


def _live_day_return(sqlite_path: str, ts_kst: datetime, current_total_asset: float) -> float:
    prefix = ts_kst.strftime("%Y-%m-%d")
    row = db.fetchone(
        sqlite_path,
        """
        SELECT total_asset
        FROM live_accounts
        WHERE ts_kst LIKE ?
        ORDER BY snap_id ASC
        LIMIT 1
        """,
        (prefix + "%",),
    )
    start_asset = _safe_float(row["total_asset"]) if row else 0.0
    if start_asset <= 0:
        return 0.0
    return (max(0.0, current_total_asset) / start_asset) - 1.0


def _live_account_drawdown(sqlite_path: str, lookback_snapshots: int = 120) -> float:
    rows = db.fetchall(
        sqlite_path,
        "SELECT total_asset FROM live_accounts ORDER BY snap_id DESC LIMIT ?",
        (max(10, int(lookback_snapshots)),),
    )
    if not rows:
        return 0.0

    assets = [_safe_float(r["total_asset"]) for r in reversed(rows)]
    peak = 0.0
    worst = 0.0
    for v in assets:
        if v <= 0:
            continue
        peak = max(peak, v)
        if peak <= 0:
            continue
        dd = (v / peak) - 1.0
        worst = min(worst, dd)
    return abs(min(0.0, worst))


def _is_fund_limit_error(message: str) -> bool:
    m = str(message or "").lower()
    return ("apbk0952" in m) or ("주문가능금액" in m) or ("증거금" in m and "부족" in m)


def _sell_rule(row: pd.Series, pos: dict[str, Any]) -> tuple[bool, str]:
    ret_1h = _safe_float(row.get("return_1h"))
    drawdown_20 = _safe_float(row.get("drawdown_20"))
    flow = _safe_float(row.get("flow_score"))
    score = _safe_float(row.get("score"))
    pnl_pct = _safe_float(pos.get("pnl_pct"))

    if pnl_pct <= -0.08:
        return True, "hard_stop_pnl"
    if ret_1h <= -0.035:
        return True, "stop_loss_1h"
    if drawdown_20 < -0.10:
        return True, "drawdown_break"
    if flow < -0.6 and score < 45.0:
        return True, "flow_reversal"
    if (ret_1h >= 0.07 and flow < 0) or (pnl_pct >= 0.12 and flow < 0.2):
        return True, "take_profit_fade"
    return False, ""


def _insert_live_order(
    sqlite_path: str,
    *,
    ts_iso: str,
    side: str,
    ticker: str,
    name: str,
    qty: int,
    price: float,
    order_no: str,
    status: str,
    reason: str,
    run_id: int,
) -> None:
    db.execute(
        sqlite_path,
        """
        INSERT INTO live_orders(
            ts_kst, side, ticker, name, qty, price, order_no, status, reason, run_id
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (ts_iso, side, ticker, name, int(qty), float(price), order_no, status, reason[:280], int(run_id)),
    )


def _build_risk_overlay(
    *,
    settings: Settings,
    sqlite_path: str,
    ts_kst: datetime,
    base_threshold: float,
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    total_asset = _safe_float(snapshot.get("total_asset"))
    cash = _safe_float(snapshot.get("cash"))
    total_eval = _safe_float(snapshot.get("total_eval"))
    positions = snapshot.get("positions") or []
    unrealized_pnl = sum(_safe_float(p.get("pnl_amount")) for p in positions)
    unrealized_ret = (unrealized_pnl / total_eval) if total_eval > 0 else 0.0
    day_return = _live_day_return(sqlite_path, ts_kst, total_asset)
    account_drawdown = _live_account_drawdown(sqlite_path, lookback_snapshots=120)
    stats = _daily_live_order_stats(sqlite_path, ts_kst)
    failed = int(stats.get("failed", 0))
    total_orders = int(stats.get("total", 0))
    fail_rate = (failed / total_orders) if total_orders > 0 else 0.0
    cash_ratio = (cash / total_asset) if total_asset > 0 else 0.0

    mode = "normal"
    threshold_add = 0.0
    order_scale = 1.0
    position_scale = 1.0
    reserve_add = 0.0

    risk_off = (
        day_return <= -abs(float(settings.live_risk_off_day_loss_pct))
        or account_drawdown >= abs(float(settings.live_risk_off_drawdown_pct))
        or unrealized_ret <= -0.05
    )
    if risk_off:
        mode = "defensive"
        threshold_add += 5.0
        order_scale *= 0.55
        position_scale *= 0.70
        reserve_add += 0.05
    elif day_return >= abs(float(settings.live_risk_on_day_gain_pct)) and fail_rate < 0.3 and unrealized_ret > -0.01:
        mode = "offensive"
        threshold_add -= 1.0
        order_scale *= 1.10
        position_scale *= 1.10

    if total_orders >= 3 and fail_rate >= 0.60:
        mode = "defensive"
        threshold_add += 2.0
        order_scale *= 0.75
        reserve_add += 0.03

    if cash_ratio < (settings.live_cash_reserve_pct * 0.70):
        threshold_add += 2.0
        order_scale *= 0.70
        reserve_add += 0.02

    effective_threshold = _clamp(base_threshold + threshold_add, 40.0, 95.0)
    return {
        "mode": mode,
        "effective_threshold": float(effective_threshold),
        "order_scale": _clamp(order_scale, 0.35, 1.30),
        "position_scale": _clamp(position_scale, 0.50, 1.20),
        "reserve_pct": _clamp(settings.live_cash_reserve_pct + reserve_add, 0.0, 0.80),
        "day_return": float(day_return),
        "account_drawdown": float(account_drawdown),
        "cash_ratio": float(cash_ratio),
        "unrealized_ret": float(unrealized_ret),
        "fail_rate_today": float(fail_rate),
    }


def _calc_buy_budget(
    *,
    cash_now: float,
    total_asset: float,
    used_capital: float,
    capital_cap: float,
    reserve_pct: float,
) -> tuple[float, float]:
    reserve_cash = max(0.0, total_asset * max(0.0, reserve_pct))
    spendable_cash = max(0.0, cash_now - reserve_cash)
    if capital_cap > 0:
        cap_headroom = max(0.0, capital_cap - used_capital)
        return max(0.0, min(spendable_cash, cap_headroom)), reserve_cash
    return max(0.0, spendable_cash), reserve_cash


def _inquire_buying_power(
    provider: Any,
    *,
    ticker: str,
    price: float,
    order_type: str,
    cache: dict[str, dict[str, Any]],
    refresh: bool = False,
) -> dict[str, Any]:
    if not hasattr(provider, "inquire_buying_power"):
        return {}
    key = f"{ticker}:{int(round(max(1.0, float(price or 1.0))))}:{order_type}"
    if not refresh and key in cache:
        return cache[key]
    try:
        out = provider.inquire_buying_power(
            ticker=ticker,
            price=float(price),
            order_type=order_type,
        )
        if isinstance(out, dict):
            cache[key] = out
            return out
    except Exception:
        return {}
    return {}


def execute_live_trading(
    *,
    settings: Settings,
    provider: Any,
    run_id: int,
    ts_kst: datetime,
    ranked_entries: pd.DataFrame,
    market_state: pd.DataFrame,
    strategy_entry_threshold: float,
) -> dict[str, Any]:
    base_threshold = float(max(settings.live_entry_score_threshold, strategy_entry_threshold))
    summary: dict[str, Any] = {
        "enabled": bool(settings.live_enable),
        "active": False,
        "status": "disabled",
        "orders_submitted": 0,
        "orders_failed": 0,
        "buys": 0,
        "sells": 0,
        "threshold": base_threshold,
    }
    ts_iso = kst_iso(ts_kst)

    if not settings.live_enable:
        summary["status"] = "live_env_off"
        return summary
    if not hasattr(provider, "inquire_balance") or not hasattr(provider, "place_cash_order"):
        summary["status"] = "provider_unsupported"
        return summary

    active = get_live_trading_enabled(settings.sqlite_path, default_on=settings.live_auto_start)
    summary["active"] = active

    try:
        snap_pre = sync_live_snapshot(
            settings.sqlite_path,
            provider,
            ts_kst,
            note=f"pre-run:{run_id}:active={1 if active else 0}",
        )
    except Exception as exc:
        summary["status"] = "balance_error"
        summary["error"] = f"{type(exc).__name__}: {exc}"
        return summary

    summary["cash"] = _safe_float(snap_pre.get("cash"))
    summary["total_asset"] = _safe_float(snap_pre.get("total_asset"))
    summary["positions"] = len(snap_pre.get("positions") or [])

    if not active:
        summary["status"] = "standby"
        return summary

    trades_today = _daily_live_order_count(settings.sqlite_path, ts_kst)
    budget_left = max(0, int(settings.live_max_trades_per_day) - trades_today)
    summary["trades_today"] = trades_today
    if budget_left <= 0:
        summary["status"] = "daily_limit_reached"
        return summary

    risk = _build_risk_overlay(
        settings=settings,
        sqlite_path=settings.sqlite_path,
        ts_kst=ts_kst,
        base_threshold=base_threshold,
        snapshot=snap_pre,
    )
    summary.update(
        {
            "risk_mode": str(risk.get("mode")),
            "day_return": float(risk.get("day_return", 0.0)),
            "account_drawdown": float(risk.get("account_drawdown", 0.0)),
            "cash_ratio": float(risk.get("cash_ratio", 0.0)),
            "unrealized_ret": float(risk.get("unrealized_ret", 0.0)),
            "fail_rate_today": float(risk.get("fail_rate_today", 0.0)),
            "threshold": float(risk.get("effective_threshold", base_threshold)),
        }
    )

    market_rows = market_state.copy() if not market_state.empty else pd.DataFrame()
    market_by_ticker: dict[str, pd.Series] = {str(r["ticker"]): r for _, r in market_rows.iterrows()}
    threshold = float(summary["threshold"])

    positions_now = snap_pre.get("positions") or []
    cash_now = _safe_float(snap_pre.get("cash"))
    used_capital = sum(_safe_float(p.get("eval_amount")) for p in positions_now)
    capital_cap = max(0.0, _safe_float(settings.live_max_capital_krw))
    buy_budget, reserve_cash = _calc_buy_budget(
        cash_now=cash_now,
        total_asset=_safe_float(snap_pre.get("total_asset")),
        used_capital=used_capital,
        capital_cap=capital_cap,
        reserve_pct=float(risk.get("reserve_pct", settings.live_cash_reserve_pct)),
    )
    summary["buy_budget"] = float(buy_budget)
    summary["reserve_cash"] = float(reserve_cash)

    # 1) Exit pass
    if settings.live_allow_sell:
        for p in positions_now:
            if budget_left <= 0:
                break
            ticker = str(p.get("ticker") or "").strip()
            if not ticker:
                continue
            row = market_by_ticker.get(ticker)
            if row is None:
                continue
            do_sell, reason = _sell_rule(row, p)
            if not do_sell:
                continue
            qty = _safe_int(p.get("qty"))
            if qty <= 0:
                continue
            px = _safe_float(row.get("price"), _safe_float(p.get("last_price"), _safe_float(p.get("avg_price"), 0.0)))
            if px <= 0:
                continue
            name = str(row.get("name") or p.get("name") or ticker)
            try:
                order = provider.place_cash_order(
                    ticker=ticker,
                    qty=qty,
                    side="SELL",
                    order_type=settings.live_order_type,
                    price=0.0,
                )
                _insert_live_order(
                    settings.sqlite_path,
                    ts_iso=ts_iso,
                    side="SELL",
                    ticker=ticker,
                    name=name,
                    qty=qty,
                    price=px,
                    order_no=str(order.get("order_no") or ""),
                    status="submitted",
                    reason=reason,
                    run_id=run_id,
                )
                summary["orders_submitted"] += 1
                summary["sells"] += 1
            except Exception as exc:
                _insert_live_order(
                    settings.sqlite_path,
                    ts_iso=ts_iso,
                    side="SELL",
                    ticker=ticker,
                    name=name,
                    qty=qty,
                    price=px,
                    order_no="",
                    status="failed",
                    reason=f"{reason}|{type(exc).__name__}:{str(exc)[:160]}",
                    run_id=run_id,
                )
                summary["orders_failed"] += 1
            budget_left -= 1

    # Refresh before entry pass (cash/positions can change right after sells).
    try:
        snap_mid = sync_live_snapshot(settings.sqlite_path, provider, ts_kst, note=f"mid-run:{run_id}")
    except Exception:
        snap_mid = snap_pre

    positions_mid = snap_mid.get("positions") or []
    held = {str(p.get("ticker") or "") for p in positions_mid}
    held.discard("")
    cash_now = _safe_float(snap_mid.get("cash"))
    used_capital = sum(_safe_float(p.get("eval_amount")) for p in positions_mid)
    buy_budget, reserve_cash = _calc_buy_budget(
        cash_now=cash_now,
        total_asset=_safe_float(snap_mid.get("total_asset")),
        used_capital=used_capital,
        capital_cap=capital_cap,
        reserve_pct=float(risk.get("reserve_pct", settings.live_cash_reserve_pct)),
    )
    summary["buy_budget"] = float(buy_budget)
    summary["reserve_cash"] = float(reserve_cash)

    max_pos_effective = max(1, int(round(float(settings.live_max_positions) * float(risk.get("position_scale", 1.0)))))
    slots = max(0, max_pos_effective - len(held))
    summary["max_positions_effective"] = int(max_pos_effective)

    buying_power_cache: dict[str, dict[str, Any]] = {}

    # 2) Entry pass
    if budget_left > 0 and buy_budget > 0 and slots > 0 and not ranked_entries.empty:
        for _, row in ranked_entries.iterrows():
            if slots <= 0 or budget_left <= 0 or buy_budget <= 0:
                break
            ticker = str(row.get("ticker") or "").strip()
            if not ticker or ticker in held:
                continue
            score = _safe_float(row.get("score"))
            if score < threshold:
                continue
            px = _safe_float(row.get("price"))
            if px <= 0:
                continue

            remaining = max(1, min(slots, budget_left))
            per_slot_budget = buy_budget / float(remaining)
            max_order_value = max(
                float(settings.live_min_order_krw),
                _safe_float(snap_mid.get("total_asset")) * float(settings.live_max_order_pct),
            )
            target_notional = min(per_slot_budget * float(risk.get("order_scale", 1.0)), max_order_value, buy_budget)

            bp = _inquire_buying_power(
                provider,
                ticker=ticker,
                price=px,
                order_type=settings.live_order_type,
                cache=buying_power_cache,
                refresh=False,
            )
            psbl_qty = 0
            if bp:
                psbl_qty = max(_safe_int(bp.get("nrcvb_buy_qty")), _safe_int(bp.get("max_buy_qty")))
                psbl_cash = max(
                    _safe_float(bp.get("nrcvb_buy_amt")),
                    _safe_float(bp.get("ord_psbl_cash")),
                    _safe_float(bp.get("max_buy_amt")),
                )
                if psbl_cash > 0:
                    target_notional = min(target_notional, psbl_cash)

            if target_notional < float(settings.live_min_order_krw):
                continue
            qty = int(target_notional // px)
            if psbl_qty > 0:
                qty = min(qty, psbl_qty)
            if qty <= 0:
                continue

            name = str(row.get("name") or ticker)
            final_qty = qty
            order_no = ""
            status = "submitted"
            reason = f"entry_score={score:.2f}|thr={threshold:.2f}|risk={risk.get('mode')}"
            est_cost = qty * px

            try:
                order = provider.place_cash_order(
                    ticker=ticker,
                    qty=qty,
                    side="BUY",
                    order_type=settings.live_order_type,
                    price=0.0,
                )
                order_no = str(order.get("order_no") or "")
            except Exception as exc:
                err_text = f"{type(exc).__name__}:{str(exc)[:180]}"
                if settings.live_retry_on_fund_error and _is_fund_limit_error(str(exc)):
                    bp_retry = _inquire_buying_power(
                        provider,
                        ticker=ticker,
                        price=px,
                        order_type=settings.live_order_type,
                        cache=buying_power_cache,
                        refresh=True,
                    )
                    retry_cap = max(_safe_int(bp_retry.get("nrcvb_buy_qty")), _safe_int(bp_retry.get("max_buy_qty")))
                    retry_qty = min(retry_cap, max(1, int(qty * 0.6))) if retry_cap > 0 else max(1, int(qty * 0.5))
                    if retry_qty < qty and retry_qty > 0:
                        try:
                            order = provider.place_cash_order(
                                ticker=ticker,
                                qty=retry_qty,
                                side="BUY",
                                order_type=settings.live_order_type,
                                price=0.0,
                            )
                            final_qty = retry_qty
                            est_cost = final_qty * px
                            order_no = str(order.get("order_no") or "")
                            reason += f"|retry_qty={retry_qty}|first_err={err_text[:90]}"
                        except Exception as exc2:
                            status = "failed"
                            reason += f"|{err_text}|retry_fail={type(exc2).__name__}:{str(exc2)[:120]}"
                    else:
                        status = "failed"
                        reason += f"|{err_text}|retry_skip=qty_cap"
                else:
                    status = "failed"
                    reason += f"|{err_text}"

            _insert_live_order(
                settings.sqlite_path,
                ts_iso=ts_iso,
                side="BUY",
                ticker=ticker,
                name=name,
                qty=final_qty,
                price=px,
                order_no=order_no,
                status=status,
                reason=reason,
                run_id=run_id,
            )
            budget_left -= 1
            if status == "submitted":
                buy_budget = max(0.0, buy_budget - est_cost)
                held.add(ticker)
                slots -= 1
                summary["orders_submitted"] += 1
                summary["buys"] += 1
            else:
                summary["orders_failed"] += 1

            if _is_fund_limit_error(reason):
                # Funding error가 나온 직후에는 보수적으로 추가 진입을 제한.
                buy_budget = max(0.0, buy_budget * 0.6)

    try:
        snap_post = sync_live_snapshot(settings.sqlite_path, provider, ts_kst, note=f"post-run:{run_id}")
        summary["cash"] = _safe_float(snap_post.get("cash"))
        summary["total_asset"] = _safe_float(snap_post.get("total_asset"))
        summary["positions"] = len(snap_post.get("positions") or [])
    except Exception:
        pass

    if summary["orders_submitted"] > 0 and summary["orders_failed"] == 0:
        summary["status"] = "orders_submitted"
    elif summary["orders_submitted"] > 0 and summary["orders_failed"] > 0:
        summary["status"] = "partial_failed"
    elif summary["orders_failed"] > 0:
        summary["status"] = "all_failed"
    else:
        summary["status"] = "no_order"

    return summary
