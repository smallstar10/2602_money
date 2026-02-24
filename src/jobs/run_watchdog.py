from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core import db
from src.core.config import load_settings
from src.core.logger import get_logger
from src.core.market_calendar import is_krx_open_day
from src.core.timeutil import now_kst, within_time_window
from src.notify.telegram_notify import TelegramNotifier
from src.ops import collect_ecosystem_status

logger = get_logger(__name__)


def _is_active(unit: str) -> bool:
    proc = subprocess.run(["systemctl", "--user", "is-active", unit], capture_output=True, text=True)
    return proc.returncode == 0 and proc.stdout.strip() == "active"


def _start_unit(unit: str) -> bool:
    proc = subprocess.run(["systemctl", "--user", "start", unit], capture_output=True, text=True)
    return proc.returncode == 0


def _restart_user_unit(unit: str) -> bool:
    proc = subprocess.run(["systemctl", "--user", "restart", unit], capture_output=True, text=True)
    return proc.returncode == 0


def _last_run_age_minutes(sqlite_path: str) -> float | None:
    row = db.fetchone(sqlite_path, "SELECT ts_kst FROM runs ORDER BY run_id DESC LIMIT 1")
    if row is None:
        return None
    ts = pd.Timestamp(row["ts_kst"]).to_pydatetime()
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=now_kst().tzinfo)
    age = now_kst() - ts.astimezone(now_kst().tzinfo)
    return age.total_seconds() / 60.0


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
        (key, value, now_kst().isoformat(timespec="seconds")),
    )


def _append_pending_actions(sqlite_path: str, actions: list[str]) -> None:
    if not actions:
        return
    raw = _state_get(sqlite_path, "watchdog_pending_actions", "[]")
    try:
        pending = json.loads(raw)
        if not isinstance(pending, list):
            pending = []
    except Exception:
        pending = []
    ts = now_kst().strftime("%Y-%m-%d %H:%M")
    pending.extend([f"[{ts}] {a}" for a in actions])
    pending = pending[-200:]
    _state_set(sqlite_path, "watchdog_pending_actions", json.dumps(pending, ensure_ascii=False))


def _send_daily_watchdog_summary(settings, notifier: TelegramNotifier) -> None:
    now = now_kst()
    today = now.strftime("%Y-%m-%d")
    last = _state_get(settings.sqlite_path, "watchdog_daily_last_sent", "")
    if last == today:
        return
    if now.hour != 8:
        return

    raw = _state_get(settings.sqlite_path, "watchdog_pending_actions", "[]")
    try:
        pending = json.loads(raw)
        if not isinstance(pending, list):
            pending = []
    except Exception:
        pending = []

    if pending:
        lines = [f"[2602_money watchdog] {today} 08:00 요약", f"- 자동조치 {len(pending)}건"]
        lines.extend(f"- {x}" for x in pending[-20:])
    else:
        lines = [f"[2602_money watchdog] {today} 08:00 요약", "- 자동조치 없음(정상)"]

    notifier.send("\n".join(lines))
    _state_set(settings.sqlite_path, "watchdog_daily_last_sent", today)
    _state_set(settings.sqlite_path, "watchdog_pending_actions", "[]")


def main() -> int:
    settings = load_settings()
    db.init_db(settings.sqlite_path)
    notifier = TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id)

    actions: list[str] = []

    for timer in ["2602-money-hourly.timer", "2602-money-nightly.timer", "2602-money-watchdog.timer"]:
        if not _is_active(timer):
            ok = _start_unit(timer)
            actions.append(f"restart {timer}:{'ok' if ok else 'fail'}")

    now = now_kst()
    in_session = (
        now.weekday() < 5
        and is_krx_open_day(now)
        and within_time_window(now, settings.run_hourly_start, settings.run_hourly_end)
    )

    if in_session:
        age = _last_run_age_minutes(settings.sqlite_path)
        if age is None or age > 130:
            ok = _start_unit("2602-money-hourly.service")
            actions.append(f"kick hourly:{'ok' if ok else 'fail'} age_min={age}")

    if settings.watchdog_enable_external:
        # hotdeal timers (user-level)
        for timer in [
            "hotdeal-discovery.timer",
            "hotdeal-tracker.timer",
            "hotdeal-nightly.timer",
            "hotdeal-chatcmd.timer",
        ]:
            if not _is_active(timer):
                ok = _start_unit(timer)
                actions.append(f"restart {timer}:{'ok' if ok else 'fail'}")

        eco = collect_ecosystem_status(settings)
        hot_age = eco.get("hotdeal", {}).get("age_min")
        if hot_age is None or float(hot_age) > float(settings.watchdog_stale_hotdeal_min):
            ok = _start_unit("hotdeal-tracker.service")
            actions.append(f"kick hotdeal-tracker:{'ok' if ok else 'fail'} age_min={hot_age}")

        blog_unit = settings.ecosystem_blog_service_unit
        blog_mode = "user" if settings.ecosystem_blog_service_user_mode else "system"
        blog_service = str(eco.get("blog", {}).get("service", "unknown"))
        blog_age = eco.get("blog", {}).get("age_min")
        if blog_service != "active":
            if settings.ecosystem_blog_service_user_mode:
                ok = _restart_user_unit(blog_unit)
            else:
                # best effort; when sudo-noninteractive is not available it simply fails.
                proc = subprocess.run(["sudo", "-n", "systemctl", "restart", blog_unit], capture_output=True, text=True)
                ok = proc.returncode == 0
            actions.append(f"restart {blog_unit}({blog_mode}):{'ok' if ok else 'fail'} state={blog_service}")
        elif settings.watchdog_restart_blog_on_stale and (
            blog_age is None or float(blog_age) > float(settings.watchdog_stale_blog_min)
        ):
            if settings.ecosystem_blog_service_user_mode:
                ok = _restart_user_unit(blog_unit)
            else:
                proc = subprocess.run(["sudo", "-n", "systemctl", "restart", blog_unit], capture_output=True, text=True)
                ok = proc.returncode == 0
            actions.append(f"kick {blog_unit}({blog_mode}):{'ok' if ok else 'fail'} age_min={blog_age}")

    if actions:
        _append_pending_actions(settings.sqlite_path, actions)
        logger.info("watchdog actions queued: %s", "; ".join(actions))
    else:
        logger.info("watchdog ok")

    _send_daily_watchdog_summary(settings, notifier)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
