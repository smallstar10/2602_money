from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Iterable
from xml.etree import ElementTree as ET

import requests


@dataclass
class NewsItem:
    title: str
    url: str
    source: str
    published_at: datetime | None
    category: str


def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return parsedate_to_datetime(value)
    except Exception:
        return None


def _feed_name(root: ET.Element, fallback: str) -> str:
    title = root.findtext("./channel/title")
    if title and title.strip():
        return title.strip()
    return fallback


def _fetch_feed(url: str, category: str, timeout: int = 10) -> list[NewsItem]:
    try:
        resp = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
    except Exception:
        return []

    source = _feed_name(root, fallback=url)
    out: list[NewsItem] = []
    for item in root.findall("./channel/item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        if not title or not link:
            continue
        out.append(
            NewsItem(
                title=title,
                url=link,
                source=source,
                published_at=_parse_datetime((item.findtext("pubDate") or "").strip()),
                category=category,
            )
        )
    return out


def _split_csv_urls(raw: str) -> list[str]:
    return [x.strip() for x in (raw or "").split(",") if x.strip()]


def _dedupe(items: Iterable[NewsItem]) -> list[NewsItem]:
    seen: set[tuple[str, str]] = set()
    out: list[NewsItem] = []
    for it in items:
        key = (it.title.strip().lower(), it.url.strip())
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


def build_news_digest(tech_urls_csv: str, major_urls_csv: str, top_n: int = 10) -> list[NewsItem]:
    tech_urls = _split_csv_urls(tech_urls_csv)
    major_urls = _split_csv_urls(major_urls_csv)

    tech: list[NewsItem] = []
    for url in tech_urls:
        tech.extend(_fetch_feed(url, category="TECH"))
    major: list[NewsItem] = []
    for url in major_urls:
        major.extend(_fetch_feed(url, category="MAJOR"))

    tech = _dedupe(tech)
    major = _dedupe(major)
    tech.sort(key=lambda x: x.published_at or datetime.min, reverse=True)
    major.sort(key=lambda x: x.published_at or datetime.min, reverse=True)

    # Balanced mix: prefer half tech + half major, then fill remainder.
    target = max(1, int(top_n))
    half = target // 2
    picked: list[NewsItem] = []
    picked.extend(tech[:half])
    picked.extend(major[:half])

    remain_pool = _dedupe([*tech[half:], *major[half:]])
    remain_pool.sort(key=lambda x: x.published_at or datetime.min, reverse=True)
    if len(picked) < target:
        picked.extend(remain_pool[: target - len(picked)])

    return _dedupe(picked)[:target]
