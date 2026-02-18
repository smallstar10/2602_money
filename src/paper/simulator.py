from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.core import db


def _today_prefix(ts_kst: datetime) -> str:
    return ts_kst.strftime("%Y-%m-%d")


def _daily_order_count(sqlite_path: str, ts_kst: datetime) -> int:
    row = db.fetchone(
        sqlite_path,
        "SELECT COUNT(*) AS n FROM paper_orders WHERE ts_kst LIKE ?",
        (_today_prefix(ts_kst) + "%",),
    )
    return int(row["n"]) if row else 0


def _latest_account(sqlite_path: str) -> tuple[float, float]:
    row = db.fetchone(sqlite_path, "SELECT cash, nav FROM paper_accounts ORDER BY account_id DESC LIMIT 1")
    if row is None:
        return 1_000_000.0, 1_000_000.0
    return float(row["cash"]), float(row["nav"])


def _load_positions(sqlite_path: str) -> dict[str, dict]:
    rows = db.fetchall(sqlite_path, "SELECT ticker, name, qty, avg_price FROM paper_positions")
    out: dict[str, dict] = {}
    for r in rows:
        out[str(r["ticker"])] = {
            "name": str(r["name"] or r["ticker"]),
            "qty": int(r["qty"]),
            "avg_price": float(r["avg_price"]),
        }
    return out


def _replace_positions(sqlite_path: str, positions: dict[str, dict], ts_iso: str) -> None:
    db.execute(sqlite_path, "DELETE FROM paper_positions")
    rows = [
        (ticker, p["name"], int(p["qty"]), float(p["avg_price"]), ts_iso)
        for ticker, p in positions.items()
        if p["qty"] > 0
    ]
    db.executemany(
        sqlite_path,
        "INSERT INTO paper_positions(ticker, name, qty, avg_price, updated_ts_kst) VALUES (?,?,?,?,?)",
        rows,
    )


def _mark_to_market(cash: float, positions: dict[str, dict], price_map: dict[str, float]) -> float:
    val = cash
    for ticker, p in positions.items():
        px = float(price_map.get(ticker, p["avg_price"]))
        val += px * p["qty"]
    return val


def _sell_rule(row: pd.Series, pos: dict) -> tuple[bool, str]:
    ret = float(row.get("return_1h", 0.0))
    drawdown = float(row.get("drawdown_20", 0.0))
    if ret <= -0.035:
        return True, "stop-loss"
    if ret >= 0.06:
        return True, "take-profit"
    if drawdown < -0.09:
        return True, "trend-break"
    return False, ""


def run_paper_trading(
    sqlite_path: str,
    run_id: int,
    ts_kst: datetime,
    ranked_entries: pd.DataFrame,
    market_state: pd.DataFrame,
    fallback_price_map: dict[str, float] | None,
    initial_cash: float,
    max_trades_per_day: int,
    max_positions: int,
    entry_score_threshold: float,
    fee_bps: float,
    slippage_bps: float,
) -> dict:
    ts_iso = ts_kst.isoformat(timespec="seconds")
    trades_today = _daily_order_count(sqlite_path, ts_kst)
    budget_left = max(0, max_trades_per_day - trades_today)
    fallback_price_map = fallback_price_map or {}

    cash, _ = _latest_account(sqlite_path)
    if cash <= 0 and trades_today == 0:
        cash = initial_cash
    positions = _load_positions(sqlite_path)

    market_rows = market_state.copy() if not market_state.empty else pd.DataFrame()
    market_by_ticker = {
        str(r["ticker"]): r
        for _, r in market_rows.iterrows()
    }
    price_map = {str(r["ticker"]): float(r["price"]) for _, r in market_rows.iterrows()} | fallback_price_map

    if budget_left <= 0:
        nav = _mark_to_market(cash, positions, price_map)
        db.execute(
            sqlite_path,
            "INSERT INTO paper_accounts(ts_kst, cash, nav, note) VALUES (?,?,?,?)",
            (ts_iso, cash, nav, f"paper-run:{run_id}:limit-reached"),
        )
        return {"orders": 0, "cash": round(cash, 2), "nav": round(nav, 2)}

    orders: list[tuple] = []

    # 1) Exit pass: evaluate current holdings against full market_state, not only top entries.
    for ticker in list(positions.keys()):
        if budget_left <= 0:
            continue
        row = market_by_ticker.get(ticker)
        if row is None:
            continue
        do_sell, reason = _sell_rule(row, positions[ticker])
        if not do_sell:
            continue
        qty = int(positions[ticker]["qty"])
        if qty <= 0:
            continue
        base_price = float(row.get("price", positions[ticker]["avg_price"]))
        exec_price = base_price * (1.0 - slippage_bps / 10000.0)
        gross = qty * exec_price
        fee = gross * fee_bps / 10000.0
        cash += gross - fee
        orders.append(
            (
                ts_iso,
                "SELL",
                ticker,
                str(row.get("name", positions[ticker]["name"])),
                qty,
                exec_price,
                f"{reason}|fee={fee:.2f}",
                run_id,
            )
        )
        del positions[ticker]
        budget_left -= 1

    # 2) Entry pass (top rank first)
    slots = max(0, max_positions - len(positions))
    if slots > 0 and budget_left > 0:
        allocation = cash / max(1, min(slots, budget_left))
        for _, row in ranked_entries.iterrows():
            if slots <= 0 or budget_left <= 0:
                break
            ticker = str(row["ticker"])
            if ticker in positions:
                continue
            score = float(row.get("score", 0.0))
            if score < entry_score_threshold:
                continue
            base_price = float(row["price"])
            exec_price = base_price * (1.0 + slippage_bps / 10000.0)
            qty = int(allocation // exec_price)
            if qty <= 0:
                continue
            gross = qty * exec_price
            fee = gross * fee_bps / 10000.0
            total_cost = gross + fee
            if total_cost > cash:
                continue
            cash -= total_cost
            positions[ticker] = {"name": str(row.get("name", ticker)), "qty": qty, "avg_price": exec_price}
            orders.append((ts_iso, "BUY", ticker, str(row.get("name", ticker)), qty, exec_price, f"top-score-entry|fee={fee:.2f}", run_id))
            slots -= 1
            budget_left -= 1

    db.executemany(
        sqlite_path,
        "INSERT INTO paper_orders(ts_kst, side, ticker, name, qty, price, reason, run_id) VALUES (?,?,?,?,?,?,?,?)",
        orders,
    )
    _replace_positions(sqlite_path, positions, ts_iso)
    nav = _mark_to_market(cash, positions, price_map)
    db.execute(
        sqlite_path,
        "INSERT INTO paper_accounts(ts_kst, cash, nav, note) VALUES (?,?,?,?)",
        (ts_iso, cash, nav, f"paper-run:{run_id}"),
    )

    return {"orders": len(orders), "cash": round(cash, 2), "nav": round(nav, 2)}
