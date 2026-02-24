from __future__ import annotations

import json
import sys
import traceback
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core import db
from src.core.config import load_settings
from src.core.logger import get_logger
from src.core.market_calendar import is_krx_open_day
from src.core.timeutil import now_kst, within_time_window
from src.analysis.llm_analyst import build_analyst_note
from src.features.feature_engine import build_features
from src.feedback.rebalance import load_strategy_state
from src.events.news_risk import build_event_context
from src.live import execute_live_trading
from src.market.us_index import fetch_sp500_snapshot
from src.notify.formatters import format_hourly_message
from src.notify.telegram_notify import TelegramNotifier
from src.providers import load_provider
from src.providers.community_stub import get_buzz_score
from src.paper.simulator import run_paper_trading
from src.scoring.score_engine import score_candidates
from src.scoring.schema import FEATURE_EXPORT_KEYS
from src.scoring.weights import load_active_weights

logger = get_logger(__name__)


def _serialize_candidate_features(row: pd.Series) -> str:
    payload = {k: float(row[k]) for k in FEATURE_EXPORT_KEYS if k in row}
    return json.dumps(payload, ensure_ascii=True)


def _build_base_rationale(row: pd.Series) -> str:
    return (
        f"거래대금 {float(row['money_value_surge']):.2f}x / 거래량 {float(row['volume_surge']):.2f}x; "
        f"flow {float(row['flow_score']):.2f}; atr {float(row['atr_regime']):.2f}; "
        f"breadth {float(row['sector_breadth']):.2f}; rotation {float(row['sector_rotation']):.3f}; "
        f"rs5 {float(row['rs_5']):.2%}; persist {float(row['momentum_persistence']):.2f}; "
        f"trend {float(row['trend_strength']):.3f}; breakout {float(row['breakout_20']):.2%}; "
        f"eff {float(row['efficiency_8']):.2f}"
    )


def _build_candidate_db_row(run_id: int, row: pd.Series, settings: Any) -> tuple[Any, ...]:
    features_json = _serialize_candidate_features(row)
    base_rationale = _build_base_rationale(row)
    llm_note = build_analyst_note(settings, row.to_dict() | {"name": row["name"]})
    rationale = f"{base_rationale} | {llm_note}" if llm_note else base_rationale
    return (
        run_id,
        row["ticker"],
        row["name"],
        float(row["score"]),
        float(row["price"]),
        features_json,
        rationale,
    )


def main() -> int:
    settings = load_settings()
    db.init_db(settings.sqlite_path)
    notifier = TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id)
    now = now_kst()

    if now.weekday() >= 5:
        logger.info("weekend, skip")
        return 0
    if not is_krx_open_day(now):
        logger.info("market holiday, skip")
        return 0
    if not within_time_window(now, settings.run_hourly_start, settings.run_hourly_end):
        logger.info("outside run window, skip")
        return 0

    try:
        provider = load_provider(settings)
        universe = provider.get_universe(settings.universe)
        tickers = [x["ticker"] for x in universe]
        name_map = {x["ticker"]: x["name"] for x in universe}

        ohlcv = provider.get_latest_ohlcv(tickers, interval="60m")
        flow = provider.get_investor_flow(tickers, window=20)
        sector_map = provider.get_sector_map(tickers)
        buzz = get_buzz_score(tickers)
        strategy_state = load_strategy_state(settings.sqlite_path)
        sp500 = fetch_sp500_snapshot() if settings.sp500_enable else None
        event_ctx = build_event_context(settings, now) if settings.event_risk_enable else None

        run_id = db.execute(
            settings.sqlite_path,
            "INSERT INTO runs(ts_kst, provider, universe, top_n, note) VALUES (?,?,?,?,?)",
            (now.isoformat(timespec="seconds"), settings.data_provider, settings.universe, settings.top_n, "hourly-scan"),
        )

        snapshot_price_map: dict[str, float] = {}
        if not ohlcv.empty:
            latest = ohlcv.sort_values(["ticker", "dt"]).groupby("ticker").tail(1)
            snapshot_rows = []
            for _, r in latest.iterrows():
                ticker = str(r["ticker"])
                price = float(r["close"])
                snapshot_price_map[ticker] = price
                snapshot_rows.append((run_id, now.isoformat(timespec="seconds"), ticker, price))
            db.executemany(
                settings.sqlite_path,
                "INSERT OR REPLACE INTO price_snapshots(run_id, ts_kst, ticker, price) VALUES (?,?,?,?)",
                snapshot_rows,
            )

        feats = build_features(ohlcv, sector_map=sector_map, investor_flow=flow, buzz_score=buzz)
        weights = load_active_weights(settings.sqlite_path)

        if feats.empty:
            paper_note = ""
            live_summary = None
            if settings.paper_enable:
                paper = run_paper_trading(
                    sqlite_path=settings.sqlite_path,
                    run_id=run_id,
                    ts_kst=now,
                    ranked_entries=pd.DataFrame(),
                    market_state=pd.DataFrame(),
                    fallback_price_map=snapshot_price_map,
                    initial_cash=settings.paper_initial_cash,
                    max_trades_per_day=settings.paper_max_trades_per_day,
                    max_positions=settings.paper_max_positions,
                    entry_score_threshold=float(strategy_state["entry_score_threshold"]),
                    fee_bps=settings.paper_fee_bps,
                    slippage_bps=settings.paper_slippage_bps,
                )
                paper_note = f" paper_orders={paper['orders']} nav={paper['nav']}"
            if settings.live_enable:
                live_summary = execute_live_trading(
                    settings=settings,
                    provider=provider,
                    run_id=run_id,
                    ts_kst=now,
                    ranked_entries=pd.DataFrame(),
                    market_state=pd.DataFrame(),
                    strategy_entry_threshold=float(strategy_state["entry_score_threshold"]),
                )
                paper_note += (
                    f" live={live_summary.get('status')}"
                    f" live_sub={int(live_summary.get('orders_submitted', 0))}"
                    f" live_fail={int(live_summary.get('orders_failed', 0))}"
                )
            db.execute(
                settings.sqlite_path,
                "UPDATE runs SET note=? WHERE run_id=?",
                (f"hourly-scan:no-feature-data{paper_note}", run_id),
            )
            notifier.send(
                format_hourly_message(
                    now,
                    feats,
                    settings.top_n,
                    sp500=sp500,
                    event_ctx=event_ctx,
                    live_summary=live_summary,
                )
            )
            logger.info("hourly run done: run_id=%s candidates=0 (no feature data)", run_id)
            return 0

        market_state = score_candidates(feats, weights)
        market_state["name"] = market_state["ticker"].map(name_map).fillna(market_state["ticker"])

        eligible = feats[(feats["value_latest"] >= settings.min_value_krw) & (feats["return_1h"].abs() <= settings.max_abs_return_1h)]
        eligible_tickers = set(eligible["ticker"].astype(str).tolist())
        ranked = market_state[market_state["ticker"].astype(str).isin(eligible_tickers)].sort_values("score", ascending=False)

        rows = []
        for _, row in ranked.head(settings.top_n).iterrows():
            rows.append(_build_candidate_db_row(run_id, row, settings))

        db.executemany(
            settings.sqlite_path,
            "INSERT INTO candidates(run_id, ticker, name, score, price, features_json, rationale) VALUES (?,?,?,?,?,?,?)",
            rows,
        )

        note_parts = ["hourly-scan"]
        if ranked.empty:
            note_parts.append("no-eligible")
        note_parts.append(f"regime={strategy_state['regime']}")

        if settings.paper_enable:
            scaled_positions = max(1, int(round(settings.paper_max_positions * float(strategy_state["position_scale"]))))
            paper = run_paper_trading(
                sqlite_path=settings.sqlite_path,
                run_id=run_id,
                ts_kst=now,
                ranked_entries=ranked.head(settings.top_n).copy(),
                market_state=market_state.copy(),
                fallback_price_map=snapshot_price_map,
                initial_cash=settings.paper_initial_cash,
                max_trades_per_day=settings.paper_max_trades_per_day,
                max_positions=scaled_positions,
                entry_score_threshold=float(strategy_state["entry_score_threshold"]),
                fee_bps=settings.paper_fee_bps,
                slippage_bps=settings.paper_slippage_bps,
            )
            note_parts.append(f"paper_orders={paper['orders']} nav={paper['nav']}")

        live_summary = None
        if settings.live_enable:
            live_summary = execute_live_trading(
                settings=settings,
                provider=provider,
                run_id=run_id,
                ts_kst=now,
                ranked_entries=ranked.head(settings.top_n).copy(),
                market_state=market_state.copy(),
                strategy_entry_threshold=float(strategy_state["entry_score_threshold"]),
            )
            note_parts.append(
                f"live={live_summary.get('status')}"
                f" live_sub={int(live_summary.get('orders_submitted', 0))}"
                f" live_fail={int(live_summary.get('orders_failed', 0))}"
            )

        db.execute(
            settings.sqlite_path,
            "UPDATE runs SET note=? WHERE run_id=?",
            (" ".join(note_parts), run_id),
        )

        message = format_hourly_message(
            now,
            ranked,
            settings.top_n,
            sp500=sp500,
            event_ctx=event_ctx,
            live_summary=live_summary,
        )
        notifier.send(message)
        logger.info("hourly run done: run_id=%s candidates=%s", run_id, len(rows))
        return 0

    except Exception as exc:
        stack = "\n".join(traceback.format_exc().splitlines()[-5:])
        notifier.send(f"[2602_money] hourly error\n{type(exc).__name__}: {exc}\n{stack}")
        logger.exception("hourly failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
