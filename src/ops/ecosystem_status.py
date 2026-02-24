from __future__ import annotations

import csv
import json
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")


def _parse_dt(value: str) -> datetime | None:
    if not value:
        return None
    s = value.strip()
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return dt.astimezone(KST)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S",):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=KST)
        except Exception:
            continue
    return None


def _age_minutes(dt: datetime | None, now: datetime) -> float | None:
    if dt is None:
        return None
    return max(0.0, (now - dt).total_seconds() / 60.0)


def _db_fetchone(db_path: str, query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    try:
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        cur = con.execute(query, params)
        row = cur.fetchone()
        con.close()
        if row is None:
            return None
        return {k: row[k] for k in row.keys()}
    except Exception:
        return None


def _unit_state(unit: str, user_mode: bool = True) -> str:
    cmd = ["systemctl"]
    if user_mode:
        cmd.append("--user")
    cmd.extend(["is-active", unit])
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        txt = (proc.stdout or proc.stderr or "").strip()
        if proc.returncode == 0:
            return txt or "active"
        return txt or "inactive"
    except Exception:
        return "unknown"


def _read_blog_stats(csv_path: str) -> dict[str, Any]:
    p = Path(csv_path)
    if not p.exists():
        return {}
    try:
        with p.open("r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        if not rows:
            return {}
        return rows[-1]
    except Exception:
        return {}


def _read_daily_state(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def collect_ecosystem_status(settings) -> dict[str, Any]:
    now = datetime.now(tz=KST)

    money_row = _db_fetchone(
        settings.sqlite_path,
        "SELECT run_id, ts_kst, provider, note FROM runs ORDER BY run_id DESC LIMIT 1",
    )
    money_last = _parse_dt(str(money_row.get("ts_kst", ""))) if money_row else None
    money_age = _age_minutes(money_last, now)

    hot_row = _db_fetchone(
        settings.ecosystem_hotdeal_db_path,
        "SELECT ts_kst, checked, alerted, note FROM tracking_runs ORDER BY run_id DESC LIMIT 1",
    )
    hot_last = _parse_dt(str(hot_row.get("ts_kst", ""))) if hot_row else None
    hot_age = _age_minutes(hot_last, now)
    hot_alerts_24h = _db_fetchone(
        settings.ecosystem_hotdeal_db_path,
        "SELECT COUNT(*) AS n FROM alerts WHERE ts_kst >= datetime('now', '-1 day')",
    )

    blog_last = _read_blog_stats(settings.ecosystem_blog_stats_csv_path)
    blog_last_ts = _parse_dt(str(blog_last.get("timestamp", ""))) if blog_last else None
    blog_age = _age_minutes(blog_last_ts, now)
    blog_daily = _read_daily_state(settings.ecosystem_blog_daily_state_path)

    status = {
        "now_kst": now,
        "money": {
            "last_run_kst": money_last,
            "age_min": money_age,
            "note": str(money_row.get("note", "")) if money_row else "",
            "provider": str(money_row.get("provider", "")) if money_row else "",
            "hourly_timer": _unit_state("2602-money-hourly.timer", user_mode=True),
            "nightly_timer": _unit_state("2602-money-nightly.timer", user_mode=True),
            "watchdog_timer": _unit_state("2602-money-watchdog.timer", user_mode=True),
        },
        "hotdeal": {
            "last_run_kst": hot_last,
            "age_min": hot_age,
            "checked": int(hot_row.get("checked", 0)) if hot_row else 0,
            "alerted": int(hot_row.get("alerted", 0)) if hot_row else 0,
            "note": str(hot_row.get("note", "")) if hot_row else "",
            "alerts_24h": int(hot_alerts_24h.get("n", 0)) if hot_alerts_24h else 0,
            "tracker_timer": _unit_state("hotdeal-tracker.timer", user_mode=True),
            "discovery_timer": _unit_state("hotdeal-discovery.timer", user_mode=True),
            "chatcmd_timer": _unit_state("hotdeal-chatcmd.timer", user_mode=True),
            "nightly_timer": _unit_state("hotdeal-nightly.timer", user_mode=True),
        },
        "blog": {
            "last_run_kst": blog_last_ts,
            "age_min": blog_age,
            "status": str(blog_last.get("status", "")) if blog_last else "",
            "fail_code": str(blog_last.get("fail_code", "")) if blog_last else "",
            "daily_success_count": int(blog_daily.get("success_count", 0)) if blog_daily else 0,
            "daily_target_sent": bool(blog_daily.get("target_sent", False)) if blog_daily else False,
            "service": _unit_state(
                settings.ecosystem_blog_service_unit,
                user_mode=bool(settings.ecosystem_blog_service_user_mode),
            ),
        },
    }
    return status
