from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")


def now_kst() -> datetime:
    return datetime.now(KST)


def kst_iso(dt: datetime | None = None) -> str:
    target = dt or now_kst()
    return target.isoformat(timespec="seconds")


def within_time_window(now: datetime, start_hm: str, end_hm: str) -> bool:
    start_h, start_m = map(int, start_hm.split(":"))
    end_h, end_m = map(int, end_hm.split(":"))
    current = now.hour * 60 + now.minute
    start = start_h * 60 + start_m
    end = end_h * 60 + end_m
    return start <= current <= end
