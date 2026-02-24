from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse
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
    region: str


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


def _contains_hangul(text: str) -> bool:
    for ch in text or "":
        if "\uac00" <= ch <= "\ud7a3":
            return True
    return False


def _infer_feed_region(feed_url: str) -> str:
    low = (feed_url or "").lower()
    if "gl=kr" in low or "ceid=kr:ko" in low or "hl=ko" in low:
        return "KR"
    if "gl=us" in low or "ceid=us:en" in low or "hl=en-us" in low:
        return "US"
    return "OTHER"


def _infer_item_region(title: str, source: str, link: str, feed_region: str) -> str:
    if feed_region in {"KR", "US"}:
        return feed_region

    joined = f"{title} {source}".lower()
    if _contains_hangul(title) or _contains_hangul(source):
        return "KR"

    host = urlparse(link or "").netloc.lower()
    kr_domains = (".kr", "naver.com", "daum.net", "yonhapnews.co.kr", "mk.co.kr", "etnews.com", "zdnet.co.kr")
    us_domains = ("nytimes.com", "wsj.com", "reuters.com", "bloomberg.com", "cnbc.com", "cnn.com", "bbc.com")

    if any(x in host for x in kr_domains):
        return "KR"
    if any(x in host for x in us_domains):
        return "US"
    if "korea" in joined:
        return "KR"
    if "us " in joined or "united states" in joined:
        return "US"
    return "OTHER"


def _fetch_feed(url: str, category: str, timeout: int = 10) -> list[NewsItem]:
    try:
        resp = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
    except Exception:
        return []

    source = _feed_name(root, fallback=url)
    feed_region = _infer_feed_region(url)
    out: list[NewsItem] = []
    for item in root.findall("./channel/item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        if not title or not link:
            continue
        region = _infer_item_region(title, source, link, feed_region)
        out.append(
            NewsItem(
                title=title,
                url=link,
                source=source,
                published_at=_parse_datetime((item.findtext("pubDate") or "").strip()),
                category=category,
                region=region,
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


def _pick_by_region(items: list[NewsItem], target: int, kr_ratio: float) -> list[NewsItem]:
    kr_target = int(round(float(target) * float(kr_ratio)))
    kr_target = max(0, min(target, kr_target))
    us_target = target - kr_target

    kr = [x for x in items if x.region == "KR"]
    us = [x for x in items if x.region == "US"]
    other = [x for x in items if x.region not in {"KR", "US"}]

    picked: list[NewsItem] = []
    picked.extend(kr[:kr_target])
    picked.extend(us[:us_target])

    picked_keys = {(x.title.strip().lower(), x.url.strip()) for x in picked}
    remain = [x for x in [*kr[kr_target:], *us[us_target:], *other] if (x.title.strip().lower(), x.url.strip()) not in picked_keys]
    if len(picked) < target:
        picked.extend(remain[: target - len(picked)])

    picked = _dedupe(picked)
    picked.sort(key=lambda x: x.published_at or datetime.min, reverse=True)
    return picked[:target]


def build_news_digest(tech_urls_csv: str, major_urls_csv: str, top_n: int = 10, kr_ratio: float = 0.9) -> list[NewsItem]:
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

    target = max(1, int(top_n))
    mix = _dedupe([*tech, *major])
    mix.sort(key=lambda x: x.published_at or datetime.min, reverse=True)
    ratio = max(0.0, min(1.0, float(kr_ratio)))
    return _pick_by_region(mix, target=target, kr_ratio=ratio)
