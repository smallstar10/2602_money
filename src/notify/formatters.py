from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any

import pandas as pd


def _safe_float(v: object, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _market_phase(top: pd.DataFrame) -> str:
    hot = float((top["money_value_surge"] >= 3.0).mean())
    flow_pos = float((top["flow_score"] > 0).mean())
    breadth_ok = float((top["sector_breadth"] >= 0.7).mean())
    rotation_ok = float((top["sector_rotation"] >= 0.25).mean())
    trend_ok = float((top["trend_strength"] >= 0.01).mean()) if "trend_strength" in top.columns else 0.0

    if hot >= 0.6 and flow_pos >= 0.6 and (breadth_ok >= 0.5 or rotation_ok >= 0.5) and trend_ok >= 0.5:
        return "자금 유입 확장 국면"
    if hot >= 0.4:
        return "선별적 유입 국면"
    return "혼조/관망 국면"


def _timeframe_hint(top: pd.DataFrame) -> str:
    short = float(((top["atr_regime"] >= 1.35) & (top["rs_5"] > 0.03)).mean())
    eff = top["efficiency_8"] if "efficiency_8" in top.columns else pd.Series([0.0] * len(top), index=top.index)
    swing = float(
        (
            (top["momentum_persistence"] >= 0.55)
            & (top["drawdown_20"] > -0.1)
            & (eff >= 0.35)
        ).mean()
    )
    if short >= 0.5:
        return "당일~1-4시간 중심"
    if swing >= 0.5:
        return "2-5일 스윙 관찰"
    return "당일 + 익일 확인"


def _dominant_sector(top: pd.DataFrame) -> str:
    if "sector" not in top.columns:
        return "분산(특정 섹터 집중 약함)"
    sectors = [str(x) for x in top["sector"].tolist() if str(x) not in ("", "UNKNOWN", "nan")]
    if not sectors:
        return "분산(섹터 정보 제한)"
    c = Counter(sectors)
    name, n = c.most_common(1)[0]
    if n <= 1:
        return "분산(특정 섹터 집중 약함)"
    return f"{name} 집중({n}/{len(top)})"


def _risk_flags(top: pd.DataFrame) -> str:
    neg_flow = [str(r["ticker"]) for _, r in top.iterrows() if _safe_float(r["flow_score"]) < 0]
    overheated = [
        str(r["ticker"])
        for _, r in top.iterrows()
        if _safe_float(r["money_value_surge"]) >= 8.0 or _safe_float(r["atr_regime"]) >= 2.0
    ]
    flags: list[str] = []
    if neg_flow:
        flags.append(f"수급 역행({','.join(neg_flow[:3])})")
    if overheated:
        flags.append(f"과열 변동성({','.join(overheated[:3])})")
    return ", ".join(flags) if flags else "뚜렷한 경고 신호 제한적"


def _candidate_comment(row: pd.Series) -> str:
    money = _safe_float(row.get("money_value_surge"))
    flow = _safe_float(row.get("flow_score"))
    breadth = _safe_float(row.get("sector_breadth"))
    rotation = _safe_float(row.get("sector_rotation"))
    rs5 = _safe_float(row.get("rs_5"))
    atr = _safe_float(row.get("atr_regime"))
    breakout = _safe_float(row.get("breakout_20"))
    trend = _safe_float(row.get("trend_strength"))
    efficiency = _safe_float(row.get("efficiency_8"))

    signals: list[str] = []
    if money >= 5.0:
        signals.append("급격한 거래대금 유입")
    elif money >= 2.0:
        signals.append("거래대금 증가")
    if flow > 0.4:
        signals.append("수급 우호")
    elif flow < -0.2:
        signals.append("수급 역행")
    if breadth >= 0.75 or rotation >= 0.25:
        signals.append("섹터 동조/회전 동반")
    if rs5 > 0.05 and atr >= 1.2:
        signals.append("단기 모멘텀 확장")
    if breakout > 0 and trend > 0.01:
        signals.append("직전 고점 돌파 시도")
    if efficiency >= 0.45:
        signals.append("추세 효율 양호")
    if not signals:
        signals.append("중립 신호 혼재")
    return ", ".join(signals)


def _format_sp500_summary(sp: dict | None) -> list[str]:
    if not sp:
        return []
    return [
        "미국장 컨텍스트(S&P500)",
        f"- 기준일: {sp.get('date')} / 종가: {sp.get('close', 0.0):,.2f}",
        f"- 1일: {sp.get('ret_1d', 0.0):+.2%}, 5일: {sp.get('ret_5d', 0.0):+.2%}, 20일 변동성: {sp.get('vol_20', 0.0):.2%}",
        f"- 레짐: {sp.get('regime', 'N/A')} / 리스크 점수: {sp.get('risk_score', 50.0):.1f}/100",
        "",
    ]


def _format_event_summary(event_ctx: dict | None) -> list[str]:
    if not event_ctx:
        return []
    lines = [
        "이벤트/뉴스 리스크",
        f"- 톤: {event_ctx.get('tone', 'N/A')} / 점수: {event_ctx.get('risk_score', 50.0):.1f}/100 (표본 {event_ctx.get('sample_size', 0)})",
    ]
    events_today = event_ctx.get("events_today") or []
    if events_today:
        lines.append(f"- 오늘 중요 일정: {', '.join(str(x) for x in events_today[:3])}")
    headlines = event_ctx.get("headlines") or []
    if headlines:
        lines.append(f"- 헤드라인: {str(headlines[0])[:90]}")
    lines.append("")
    return lines


def _format_live_summary(live_summary: dict | None) -> list[str]:
    if not live_summary:
        return []
    if not bool(live_summary.get("enabled")):
        return []
    status = str(live_summary.get("status", "N/A"))
    active = bool(live_summary.get("active", False))
    cash = _safe_float(live_summary.get("cash", 0.0))
    asset = _safe_float(live_summary.get("total_asset", 0.0))
    positions = int(_safe_float(live_summary.get("positions", 0)))
    submitted = int(_safe_float(live_summary.get("orders_submitted", 0)))
    failed = int(_safe_float(live_summary.get("orders_failed", 0)))
    buys = int(_safe_float(live_summary.get("buys", 0)))
    sells = int(_safe_float(live_summary.get("sells", 0)))
    threshold = _safe_float(live_summary.get("threshold", 0.0))
    risk_mode = str(live_summary.get("risk_mode", "normal"))
    day_ret = _safe_float(live_summary.get("day_return", 0.0))
    drawdown = _safe_float(live_summary.get("account_drawdown", 0.0))
    cash_ratio = _safe_float(live_summary.get("cash_ratio", 0.0))
    fail_rate = _safe_float(live_summary.get("fail_rate_today", 0.0))
    return [
        "실전 주문 상태",
        f"- 모드: {'ON' if active else 'OFF'} / status={status} / 리스크 {risk_mode} / 진입점수 기준 {threshold:.1f}",
        f"- 계좌 컨디션: 당일 {day_ret:+.2%}, 계좌MDD {drawdown:.2%}, 현금비중 {cash_ratio:.1%}, 실패율 {fail_rate:.1%}",
        f"- 주문: 제출 {submitted}건(매수 {buys}, 매도 {sells}), 실패 {failed}건",
        f"- 계좌: 자산 {asset:,.0f}원 / 현금 {cash:,.0f}원 / 보유 {positions}종목",
        "",
    ]


def format_hourly_message(
    ts: datetime,
    ranked: pd.DataFrame,
    top_n: int,
    sp500: dict | None = None,
    event_ctx: dict | None = None,
    live_summary: dict | None = None,
) -> str:
    header = f"[KST {ts.strftime('%Y-%m-%d %H:00')}] 2602_money 레이더"
    if ranked.empty:
        extra = _format_sp500_summary(sp500)
        extra.extend(_format_event_summary(event_ctx))
        extra.extend(_format_live_summary(live_summary))
        if not extra:
            return header + "\n후보 없음(필터 통과 종목 없음)"
        return "\n".join([header] + extra + ["후보 없음(필터 통과 종목 없음)"])

    top = ranked.head(top_n).copy()
    lines = [header]
    lines.extend(_format_sp500_summary(sp500))
    lines.extend(_format_event_summary(event_ctx))
    lines.extend(_format_live_summary(live_summary))
    lines.extend([
        "해석 요약",
        f"- 국면: {_market_phase(top)}",
        f"- 섹터: {_dominant_sector(top)}",
        f"- 관찰 프레임: {_timeframe_hint(top)}",
        f"- 리스크: {_risk_flags(top)}",
        "",
        "후보 상세(관찰용)",
    ])
    for idx, row in enumerate(top.iterrows(), start=1):
        _, row = row
        ticker = str(row.get("ticker", ""))
        name = str(row.get("name", ticker))
        lines.extend(
            [
                f"{idx}) {ticker} / {name} | score {_safe_float(row.get('score')):.2f}",
                f"- 신호: {_candidate_comment(row)}",
                f"- 수치: 대금 {_safe_float(row.get('money_value_surge')):.2f}x, 거래량 {_safe_float(row.get('volume_surge')):.2f}x, flow {_safe_float(row.get('flow_score')):.2f}, atr {_safe_float(row.get('atr_regime')):.2f}",
                f"- 확장: RS5 {_safe_float(row.get('rs_5')):.2%}, 지속성 {_safe_float(row.get('momentum_persistence')):.2f}, breadth {_safe_float(row.get('sector_breadth')):.2f}, rotation {_safe_float(row.get('sector_rotation')):.3f}",
                f"- 구조: trend {_safe_float(row.get('trend_strength')):.3f}, breakout {_safe_float(row.get('breakout_20')):.2%}, 효율 {_safe_float(row.get('efficiency_8')):.2f}, range-pos {_safe_float(row.get('range_position_20')):.2f}",
                "관찰/무효화: 고점 안착 여부 확인, 직전 저점 이탈 시 추적 종료",
            ]
        )
    lines.append("")
    lines.append("※ 본 메시지는 리서치 자동화 결과이며 매수/매도 추천이 아닙니다.")
    return "\n".join(lines)


def format_nightly_message(ts: datetime, stats: dict) -> str:
    factor_top = stats.get("factor_top", "N/A")
    factor_bottom = stats.get("factor_bottom", "N/A")
    training_level = str(stats.get("training_level", "N/A"))
    training_score = float(stats.get("training_score", 0.0))
    training_ready = bool(stats.get("training_ready", False))
    training_risk = float(stats.get("training_risk_per_trade_pct", 0.0))
    training_day_loss = float(stats.get("training_daily_loss_limit_pct", 0.0))
    training_slots = int(stats.get("training_max_new_positions", 0))
    return (
        f"[KST {ts.strftime('%Y-%m-%d %H:%M')}] 2602_money 야간 리포트\n"
        f"평균수익률(1d): {stats.get('avg_ret_1d', 0.0):.3%}\n"
        f"승률(1d): {stats.get('win_rate_1d', 0.0):.1%}\n"
        f"표본 수: {stats.get('n', 0)}\n"
        f"팩터 상위: {factor_top}\n"
        f"팩터 하위: {factor_bottom}\n"
        f"전략 레짐: {stats.get('regime', 'NEUTRAL')} ({stats.get('regime_update', 'UNCHANGED')})\n"
        f"진입 임계점: {stats.get('entry_score_threshold', 55.0):.1f}, 포지션 스케일: {stats.get('position_scale', 1.0):.2f}\n"
        f"가상매매 NAV: {stats.get('paper_nav', 0.0):,.0f} KRW (일손익 {stats.get('paper_pnl_day', 0.0):+,.0f})\n"
        f"가상매매 체결(오늘): {stats.get('paper_trades_today', 0)}\n"
        f"실전 트레이닝: {training_level} | score {training_score:.1f}/100 | ready={'YES' if training_ready else 'NO'}\n"
        f"트레이닝 리스크 예산: 1회 {training_risk:.2f}% / 일손실 {training_day_loss:.2f}% / 신규포지션 {training_slots}개\n"
        f"전략 실험실: {stats.get('strategy_lab_summary', 'N/A')}\n"
        f"가중치 조정: {stats.get('weight_update', '없음')}"
    )


def _fmt_age(age_min: float | None) -> str:
    if age_min is None:
        return "N/A"
    if age_min < 120:
        return f"{age_min:.0f}분 전"
    return f"{age_min / 60.0:.1f}시간 전"


def _fmt_dt(dt: datetime | None) -> str:
    if dt is None:
        return "N/A"
    return dt.strftime("%Y-%m-%d %H:%M")


def format_ecosystem_status(ts: datetime, eco: dict[str, Any]) -> str:
    m = eco.get("money", {})
    h = eco.get("hotdeal", {})
    b = eco.get("blog", {})
    return (
        f"[KST {ts.strftime('%Y-%m-%d %H:%M')}] 통합 상태 대시보드\n"
        f"[Money_2602]\n"
        f"- 마지막 실행: {_fmt_dt(m.get('last_run_kst'))} ({_fmt_age(m.get('age_min'))})\n"
        f"- 타이머: hourly={m.get('hourly_timer')} nightly={m.get('nightly_timer')} watchdog={m.get('watchdog_timer')}\n"
        f"[Hotdeal]\n"
        f"- 마지막 트래킹: {_fmt_dt(h.get('last_run_kst'))} ({_fmt_age(h.get('age_min'))})\n"
        f"- 최근24h 알림: {int(h.get('alerts_24h', 0))}\n"
        f"- 타이머: tracker={h.get('tracker_timer')} discovery={h.get('discovery_timer')} chat={h.get('chatcmd_timer')}\n"
        f"[Blog]\n"
        f"- 마지막 사이클: {_fmt_dt(b.get('last_run_kst'))} ({_fmt_age(b.get('age_min'))})\n"
        f"- 최근 상태: {b.get('status') or 'N/A'} / fail_code={b.get('fail_code') or '-'}\n"
        f"- 오늘 성공: {int(b.get('daily_success_count', 0))}회 / service={b.get('service')}"
    )


def format_news_digest(ts: datetime, items: list[Any]) -> str:
    lines = [f"[KST {ts.strftime('%Y-%m-%d %H:%M')}] Tech + 주요 뉴스"]
    if not items:
        lines.append("뉴스 수집 실패(또는 항목 없음)")
        return "\n".join(lines)
    for i, it in enumerate(items, start=1):
        cat = "Tech" if str(getattr(it, "category", "")).upper() == "TECH" else "Major"
        lines.append(f"{i}) [{cat}] {getattr(it, 'title', '')}")
        lines.append(f"- {getattr(it, 'url', '')}")
    return "\n".join(lines)


def format_morning_briefing(ts: datetime, eco: dict[str, Any], items: list[Any]) -> str:
    lines = [
        f"[KST {ts.strftime('%Y-%m-%d %H:%M')}] 아침 브리핑",
        "1) 통합 상태",
    ]
    lines.append(format_ecosystem_status(ts, eco))
    lines.append("")
    lines.append(f"2) 오늘 Tech/주요 뉴스 {len(items)}건")
    if not items:
        lines.append("- 뉴스 항목 없음")
    else:
        for i, it in enumerate(items, start=1):
            cat = "Tech" if str(getattr(it, "category", "")).upper() == "TECH" else "Major"
            lines.append(f"{i}) [{cat}] {getattr(it, 'title', '')}")
            lines.append(f"- {getattr(it, 'url', '')}")
    return "\n".join(lines)


def format_evening_report(ts: datetime, eco: dict[str, Any], money_summary: dict[str, Any]) -> str:
    return (
        f"[KST {ts.strftime('%Y-%m-%d %H:%M')}] 저녁 통합 리포트\n"
        f"- Money 오늘 실행: {int(money_summary.get('money_runs_today', 0))}회\n"
        f"- Money 최근 실행: {_fmt_dt(eco.get('money', {}).get('last_run_kst'))}\n"
        f"- Hotdeal 최근24h 알림: {int(eco.get('hotdeal', {}).get('alerts_24h', 0))}건\n"
        f"- Blog 오늘 성공: {int(eco.get('blog', {}).get('daily_success_count', 0))}회\n"
        f"- Money 평균 후보점수(최근): {float(money_summary.get('avg_score_latest', 0.0)):.2f}\n"
        f"- Money 최근 note: {str(eco.get('money', {}).get('note', ''))[:120]}"
    )


def format_training_report(ts: datetime, report: dict[str, Any]) -> str:
    metrics = report.get("metrics", {}) if isinstance(report, dict) else {}
    gates = report.get("gates", []) if isinstance(report, dict) else []
    rp = report.get("risk_plan", {}) if isinstance(report, dict) else {}
    checklist = report.get("checklist", []) if isinstance(report, dict) else []
    lines = [
        f"[KST {ts.strftime('%Y-%m-%d %H:%M')}] 2602_money 실전 트레이닝",
        f"- 준비도: {str(report.get('level_text', report.get('level', 'N/A')))} (score {float(report.get('score', 0.0)):.1f}/100)",
        f"- 실전 진입 판단: {'가능(소액·수동)' if bool(report.get('ready', False)) else '대기(모의·병행)'}",
        f"- 기간/표본: {int(metrics.get('history_days', 0))}일, 모의체결 {int(metrics.get('order_total', 0))}건",
        f"- 성과: 누적 {float(metrics.get('cumulative_return', 0.0)):+.2%}, 최대낙폭 {float(metrics.get('max_drawdown', 0.0)):.2%}, 일간승률 {float(metrics.get('daily_win_rate', 0.0)):.1%}",
        f"- 후행성과(1d): 표본 {int(metrics.get('outcome_n', 0))}, 평균 {float(metrics.get('outcome_avg_ret_1d', 0.0)):+.3%}, 승률 {float(metrics.get('outcome_win_rate_1d', 0.0)):.1%}",
        f"- 권장 리스크 예산: 1회 {float(rp.get('risk_per_trade_pct', 0.0)):.2f}% / 일손실 {float(rp.get('daily_loss_limit_pct', 0.0)):.2f}% / 신규 {int(rp.get('max_new_positions', 0))}개",
        "",
        "게이트 체크",
    ]
    for i, g in enumerate(gates, start=1):
        ok = bool(g.get("pass", False))
        value = g.get("value")
        target = g.get("target")
        if isinstance(value, float) and isinstance(target, float):
            val_txt = f"{value:.2%} / 기준 {target:.2%}"
        else:
            val_txt = f"{value} / 기준 {target}"
        lines.append(f"{i}) {'PASS' if ok else 'FAIL'} - {g.get('label', '-')}: {val_txt}")
    if checklist:
        lines.append("")
        lines.append("실행 체크리스트")
        for i, c in enumerate(checklist, start=1):
            lines.append(f"{i}) {str(c)}")
    lines.append("")
    lines.append("※ 본 메시지는 트레이닝/리서치 보조이며 자동 주문을 실행하지 않습니다.")
    return "\n".join(lines)


def format_training_report_log(ts: datetime, reports: list[dict[str, Any]]) -> str:
    lines = [f"[KST {ts.strftime('%Y-%m-%d %H:%M')}] 트레이닝 리포트 최근 기록"]
    if not reports:
        lines.append("- 저장된 트레이닝 리포트가 없습니다.")
        return "\n".join(lines)
    for i, r in enumerate(reports, start=1):
        metrics = r.get("metrics", {})
        lines.append(
            f"{i}) {r.get('ts_kst')} | {r.get('mode')} | {r.get('level')} {float(r.get('score', 0.0)):.1f}/100 | ready={'Y' if r.get('ready') else 'N'}"
        )
        lines.append(
            f"- 누적 {float(metrics.get('cumulative_return', 0.0)):+.2%}, MDD {float(metrics.get('max_drawdown', 0.0)):.2%}, 체결 {int(metrics.get('order_total', 0))}건"
        )
    return "\n".join(lines)
