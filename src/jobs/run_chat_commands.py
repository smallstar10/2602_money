from __future__ import annotations

import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core import db
from src.core.config import load_settings
from src.core.logger import get_logger
from src.core.timeutil import kst_iso, now_kst
from src.live import get_live_trading_enabled, set_live_trading_enabled, sync_live_snapshot
from src.news import build_news_digest
from src.notify.formatters import (
    format_ecosystem_status,
    format_news_digest,
    format_training_report,
    format_training_report_log,
)
from src.notify.telegram_notify import TelegramNotifier
from src.ops import collect_ecosystem_status
from src.paper.training_coach import build_training_report, load_recent_training_reports, save_training_report

logger = get_logger(__name__)


HELP_TEXT = (
    "사용 가능한 명령어\n"
    "/상태 - money/hotdeal/blog 통합 상태\n"
    "/뉴스 - Tech + 주요 뉴스 10건 (한국 90%/미국 10%)\n"
    "/최근 - 최근 스캔 상위 후보\n"
    "/모의투자 - 현재 보유 포지션/분석\n"
    "/트레이닝 - 실전 준비도 점수 + 리스크 예산 + 체크리스트\n"
    "/실전준비 - /트레이닝과 동일\n"
    "/트레이닝 로그 - 최근 준비도 리포트 이력\n"
    "/실전ON - 자동 주문 토글 ON\n"
    "/실전OFF - 자동 주문 토글 OFF\n"
    "/실전상태 - 자동 주문 상태/최근 계좌 스냅샷\n"
    "/도움말"
)


def _state_get(sqlite_path: str, key: str, default: str = "") -> str:
    row = db.fetchone(sqlite_path, "SELECT value FROM bot_state WHERE key=?", (key,))
    if row is None:
        return default
    return str(row["value"])


def _state_set(sqlite_path: str, key: str, value: str) -> None:
    db.execute(
        sqlite_path,
        """
        INSERT INTO bot_state(key, value, updated_ts_kst)
        VALUES (?,?,?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_ts_kst=excluded.updated_ts_kst
        """,
        (key, value, kst_iso()),
    )


def _recent_candidates(sqlite_path: str, n: int = 5) -> list[dict]:
    rows = db.fetchall(
        sqlite_path,
        """
        SELECT c.ticker, c.name, c.score, c.price
        FROM candidates c
        WHERE c.run_id = (SELECT MAX(run_id) FROM runs)
        ORDER BY c.score DESC
        LIMIT ?
        """,
        (int(n),),
    )
    return [dict(r) for r in rows]


def _paper_summary(sqlite_path: str) -> str:
    acc = db.fetchone(
        sqlite_path,
        "SELECT ts_kst, cash, nav, note FROM paper_accounts ORDER BY account_id DESC LIMIT 1",
    )
    if acc is None:
        return "[2602_money] 모의투자 계좌 데이터가 없습니다."

    positions = db.fetchall(
        sqlite_path,
        """
        SELECT ticker, name, qty, avg_price, updated_ts_kst
        FROM paper_positions
        ORDER BY updated_ts_kst DESC, ticker
        """,
    )

    lines = [
        "[2602_money] 모의투자 현황",
        f"- 기준: {acc['ts_kst']}",
        f"- NAV: {float(acc['nav']):,.0f}원 / 현금: {float(acc['cash']):,.0f}원",
        f"- note: {acc['note']}",
    ]

    if not positions:
        lines.append("- 보유 포지션: 없음")
        return "\n".join(lines)

    lines.append(f"- 보유 포지션: {len(positions)}개")
    for i, p in enumerate(positions, start=1):
        ticker = str(p["ticker"])
        name = str(p["name"] or ticker)
        qty = int(p["qty"])
        avg = float(p["avg_price"] or 0.0)

        px_row = db.fetchone(
            sqlite_path,
            """
            SELECT price, ts_kst
            FROM price_snapshots
            WHERE ticker=?
            ORDER BY run_id DESC
            LIMIT 1
            """,
            (ticker,),
        )
        cur_px = float(px_row["price"]) if px_row else avg
        eval_amt = cur_px * qty
        pnl = (cur_px - avg) * qty
        pnl_pct = ((cur_px / avg - 1.0) * 100.0) if avg > 0 else 0.0

        cand = db.fetchone(
            sqlite_path,
            """
            SELECT score, rationale, run_id
            FROM candidates
            WHERE ticker=?
            ORDER BY run_id DESC
            LIMIT 1
            """,
            (ticker,),
        )
        buy = db.fetchone(
            sqlite_path,
            """
            SELECT ts_kst, reason
            FROM paper_orders
            WHERE ticker=? AND side='BUY'
            ORDER BY order_id DESC
            LIMIT 1
            """,
            (ticker,),
        )

        lines.append(
            f"{i}) {ticker} {name} | {qty}주 | 평단 {avg:,.0f}원 | 현재 {cur_px:,.0f}원 | 손익 {pnl:+,.0f}원 ({pnl_pct:+.2f}%)"
        )
        lines.append(f"- 평가금액: {eval_amt:,.0f}원")
        if buy:
            lines.append(f"- 최근 매수: {buy['ts_kst']} / 사유: {str(buy['reason'])[:120]}")
        if cand:
            lines.append(f"- 최근 점수: {float(cand['score']):.2f} (run_id={int(cand['run_id'])})")
            lines.append(f"- 분석: {str(cand['rationale'])[:180]}")
        else:
            lines.append("- 분석: 최근 후보 데이터 없음")

    return "\n".join(lines)


def _live_status_summary(settings) -> str:
    enabled = bool(settings.live_enable)
    active = get_live_trading_enabled(settings.sqlite_path, default_on=settings.live_auto_start) if enabled else False
    acc = db.fetchone(
        settings.sqlite_path,
        "SELECT ts_kst, cash, total_eval, total_asset, note FROM live_accounts ORDER BY snap_id DESC LIMIT 1",
    )
    pos_n = db.fetchone(settings.sqlite_path, "SELECT COUNT(*) AS n FROM live_positions")
    today_orders = db.fetchone(
        settings.sqlite_path,
        "SELECT COUNT(*) AS n FROM live_orders WHERE ts_kst LIKE ?",
        (now_kst().strftime("%Y-%m-%d") + "%",),
    )
    today_failed = db.fetchone(
        settings.sqlite_path,
        "SELECT COUNT(*) AS n FROM live_orders WHERE ts_kst LIKE ? AND status='failed'",
        (now_kst().strftime("%Y-%m-%d") + "%",),
    )

    lines = [
        "[2602_money] 실전 자동주문 상태",
        f"- ENV LIVE_ENABLE: {'ON' if enabled else 'OFF'}",
        f"- 실주문 토글: {'ON' if active else 'OFF'} (명령: /실전ON, /실전OFF)",
        f"- 제한: 최대자본 {float(settings.live_max_capital_krw):,.0f}원, 일일 {int(settings.live_max_trades_per_day)}회, 최대보유 {int(settings.live_max_positions)}개",
        f"- 진입기준: score >= {float(settings.live_entry_score_threshold):.1f}, 주문유형 {settings.live_order_type}, 자동매도 {'ON' if settings.live_allow_sell else 'OFF'}",
        f"- 오늘 주문: {int(today_orders['n']) if today_orders else 0}건 (실패 {int(today_failed['n']) if today_failed else 0}건)",
    ]

    if acc is None:
        lines.append("- 최근 계좌 스냅샷: 없음")
    else:
        lines.append(
            f"- 최근 계좌: {acc['ts_kst']} | 자산 {float(acc['total_asset']):,.0f}원 | 현금 {float(acc['cash']):,.0f}원 | 평가 {float(acc['total_eval']):,.0f}원"
        )
        lines.append(f"- 보유 종목수: {int(pos_n['n']) if pos_n else 0}개")
        lines.append(f"- note: {str(acc['note'] or '')[:120]}")
    return "\n".join(lines)


def _handle_message(settings, text: str) -> str | None:
    txt = text.strip()
    if not txt:
        return None

    if txt.startswith("/도움말") or txt.startswith("/help") or txt.startswith("/start"):
        return HELP_TEXT

    if txt.startswith("/상태"):
        eco = collect_ecosystem_status(settings)
        return format_ecosystem_status(now_kst(), eco)

    if txt.startswith("/뉴스"):
        items = build_news_digest(
            settings.briefing_tech_rss_urls,
            settings.briefing_major_rss_urls,
            top_n=settings.briefing_news_count,
            kr_ratio=settings.briefing_kr_ratio,
        )
        return format_news_digest(now_kst(), items)

    if txt.startswith("/최근"):
        items = _recent_candidates(settings.sqlite_path, n=5)
        if not items:
            return "[2602_money] 최근 후보가 없습니다."
        lines = ["[2602_money] 최근 후보 TOP 5"]
        for i, it in enumerate(items, start=1):
            score = float(it.get("score") or 0.0)
            price = int(float(it.get("price") or 0.0))
            lines.append(f"{i}) {it.get('ticker')} {it.get('name')} | score {score:.2f} | {price:,}원")
        return "\n".join(lines)

    if txt.startswith("/모의투자") or txt.startswith("/paper"):
        return _paper_summary(settings.sqlite_path)

    if txt.startswith("/실전ON"):
        if not settings.live_enable:
            return "[2602_money] LIVE_ENABLE=false 입니다. .env에서 LIVE_ENABLE=true 후 재시작하세요."
        set_live_trading_enabled(settings.sqlite_path, True, ts_kst=now_kst())
        return (
            "[2602_money] 실전 자동주문 토글을 ON으로 변경했습니다.\n"
            "다음 hourly 사이클부터 조건 충족 시 주문이 제출될 수 있습니다.\n"
            "중지하려면 /실전OFF 를 사용하세요."
        )

    if txt.startswith("/실전OFF"):
        set_live_trading_enabled(settings.sqlite_path, False, ts_kst=now_kst())
        return "[2602_money] 실전 자동주문 토글을 OFF로 변경했습니다. 다음 사이클부터 주문 제출이 중지됩니다."

    if txt.startswith("/실전상태"):
        if settings.live_enable:
            try:
                provider = None
                if settings.data_provider.lower() == "kis":
                    from src.providers import load_provider

                    provider = load_provider(settings)
                if provider is not None and hasattr(provider, "inquire_balance"):
                    sync_live_snapshot(settings.sqlite_path, provider, now_kst(), note="chat:/실전상태")
            except Exception:
                # 상태 조회 실패 시에도 DB 기준 상태를 우선 반환.
                pass
        return _live_status_summary(settings)

    if txt.startswith("/트레이닝 로그"):
        reports = load_recent_training_reports(settings.sqlite_path, limit=5)
        return format_training_report_log(now_kst(), reports)

    if txt.startswith("/트레이닝") or txt.startswith("/실전준비"):
        report = build_training_report(
            settings.sqlite_path,
            lookback_days=settings.training_lookback_days,
            min_days=settings.training_min_days,
            min_trades=settings.training_min_trades,
            target_return=settings.training_target_return,
            max_drawdown_limit=settings.training_max_drawdown,
            base_risk_per_trade_pct=settings.training_base_risk_per_trade_pct,
            base_daily_loss_pct=settings.training_base_daily_loss_limit_pct,
            base_max_new_positions=settings.training_base_max_new_positions,
            now=now_kst(),
        )
        save_training_report(settings.sqlite_path, report, mode="manual", note=f"cmd:{txt[:40]}")
        return format_training_report(now_kst(), report)

    return None


def main() -> int:
    settings = load_settings()
    db.init_db(settings.sqlite_path)
    notifier = TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id)

    try:
        offset_txt = _state_get(settings.sqlite_path, "telegram_offset", "0")
        try:
            offset = int(offset_txt)
        except Exception:
            offset = 0

        updates = notifier.get_updates(offset=offset + 1, limit=settings.command_poll_limit, timeout=1)
        if not updates:
            return 0

        max_update_id = offset
        allow_chat = str(settings.telegram_chat_id).strip()
        for u in updates:
            uid = int(u.get("update_id") or 0)
            max_update_id = max(max_update_id, uid)

            msg = u.get("message") or {}
            chat = msg.get("chat") or {}
            chat_id = str(chat.get("id") or "")
            if allow_chat and chat_id != allow_chat:
                continue
            text = str(msg.get("text") or "").strip()
            if not text:
                continue

            reply = _handle_message(settings, text)
            if reply:
                notifier.send(reply)

        _state_set(settings.sqlite_path, "telegram_offset", str(max_update_id))
        return 0
    except Exception as exc:
        stack = "\n".join(traceback.format_exc().splitlines()[-5:])
        notifier.send(f"[2602_money] chat-command error\n{type(exc).__name__}: {exc}\n{stack}")
        logger.exception("chat-command failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
