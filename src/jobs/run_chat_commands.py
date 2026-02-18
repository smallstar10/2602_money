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
from src.news import build_news_digest
from src.notify.formatters import format_ecosystem_status, format_news_digest
from src.notify.telegram_notify import TelegramNotifier
from src.ops import collect_ecosystem_status

logger = get_logger(__name__)


HELP_TEXT = (
    "사용 가능한 명령어\n"
    "/상태 - money/hotdeal/blog 통합 상태\n"
    "/뉴스 - Tech + 주요 뉴스 10건\n"
    "/최근 - 최근 스캔 상위 후보\n"
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
