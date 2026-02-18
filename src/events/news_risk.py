from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any
from xml.etree import ElementTree as ET

import requests

from src.core.config import Settings
from src.core.timeutil import KST


NEGATIVE_TERMS = [
    "inflation",
    "rate hike",
    "hawkish",
    "recession",
    "war",
    "crisis",
    "downgrade",
    "default",
    "selloff",
    "plunge",
    "tariff",
    "sanction",
]

POSITIVE_TERMS = [
    "rate cut",
    "cooling inflation",
    "soft landing",
    "upgrade",
    "rally",
    "record high",
    "beat estimates",
]


@dataclass
class Headline:
    title: str
    published: datetime | None
    source: str


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _parse_pubdate(text: str | None) -> datetime | None:
    if not text:
        return None
    try:
        dt = parsedate_to_datetime(text)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=KST)
        return dt.astimezone(KST)
    except Exception:
        return None


def _parse_rss(xml_text: str) -> list[Headline]:
    out: list[Headline] = []
    root = ET.fromstring(xml_text)
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        pub = _parse_pubdate(item.findtext("pubDate"))
        source = (item.findtext("source") or "rss").strip()
        if title:
            out.append(Headline(title=title, published=pub, source=source))
    return out


def _high_impact_today(now_kst: datetime, spec: str) -> list[str]:
    if not spec.strip():
        return []
    events: list[str] = []
    today = now_kst.date().isoformat()
    for token in spec.split(";"):
        token = token.strip()
        if not token or ":" not in token:
            continue
        d, label = token.split(":", 1)
        if d.strip() == today:
            events.append(label.strip() or "high-impact event")
    return events


def build_event_context(settings: Settings, now_kst: datetime, lookback_hours: int = 30) -> dict[str, Any] | None:
    if not settings.event_risk_enable:
        return None

    urls = [u.strip() for u in settings.event_feed_urls.split(",") if u.strip()]
    if not urls:
        return None

    headlines: list[Headline] = []
    for url in urls[:5]:
        try:
            resp = requests.get(url, timeout=8)
            resp.raise_for_status()
            headlines.extend(_parse_rss(resp.text))
        except Exception:
            continue

    if not headlines:
        return {
            "risk_score": 50.0,
            "tone": "중립(피드 수집 제한)",
            "headlines": [],
            "events_today": _high_impact_today(now_kst, settings.high_impact_dates),
        }

    cutoff = now_kst - timedelta(hours=lookback_hours)
    recent = [h for h in headlines if h.published is None or h.published >= cutoff]
    if not recent:
        recent = headlines[:20]

    neg = 0
    pos = 0
    scored: list[tuple[int, Headline]] = []
    for h in recent:
        t = h.title.lower()
        n = sum(1 for w in NEGATIVE_TERMS if w in t)
        p = sum(1 for w in POSITIVE_TERMS if w in t)
        neg += n
        pos += p
        scored.append((n - p, h))

    events_today = _high_impact_today(now_kst, settings.high_impact_dates)
    risk_raw = 50.0
    if recent:
        risk_raw += 35.0 * ((neg - pos) / max(1, len(recent)))
    risk_raw += 8.0 * len(events_today)
    risk_score = _clip(risk_raw, 0.0, 100.0)

    if risk_score >= 67:
        tone = "이벤트 경계(리스크오프)"
    elif risk_score <= 35:
        tone = "이벤트 우호(리스크온)"
    else:
        tone = "중립/혼재"

    scored.sort(key=lambda x: x[0], reverse=True)
    top_titles = [x[1].title for x in scored[:3]]
    return {
        "risk_score": float(risk_score),
        "tone": tone,
        "headlines": top_titles,
        "events_today": events_today,
        "sample_size": len(recent),
    }

