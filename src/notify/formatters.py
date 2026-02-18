from __future__ import annotations

from collections import Counter
from datetime import datetime

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


def format_hourly_message(
    ts: datetime,
    ranked: pd.DataFrame,
    top_n: int,
    sp500: dict | None = None,
    event_ctx: dict | None = None,
) -> str:
    header = f"[KST {ts.strftime('%Y-%m-%d %H:00')}] 2602_money 레이더"
    if ranked.empty:
        extra = _format_sp500_summary(sp500)
        extra.extend(_format_event_summary(event_ctx))
        if not extra:
            return header + "\n후보 없음(필터 통과 종목 없음)"
        return "\n".join([header] + extra + ["후보 없음(필터 통과 종목 없음)"])

    top = ranked.head(top_n).copy()
    lines = [header]
    lines.extend(_format_sp500_summary(sp500))
    lines.extend(_format_event_summary(event_ctx))
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
        f"전략 실험실: {stats.get('strategy_lab_summary', 'N/A')}\n"
        f"가중치 조정: {stats.get('weight_update', '없음')}"
    )
