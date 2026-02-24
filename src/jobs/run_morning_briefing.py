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
from src.core.timeutil import now_kst
from src.news import build_news_digest
from src.notify.formatters import format_morning_briefing
from src.notify.telegram_notify import TelegramNotifier
from src.ops import collect_ecosystem_status

logger = get_logger(__name__)


def main() -> int:
    settings = load_settings()
    db.init_db(settings.sqlite_path)
    notifier = TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id)
    ts = now_kst()

    try:
        eco = collect_ecosystem_status(settings)
        items = build_news_digest(
            settings.briefing_tech_rss_urls,
            settings.briefing_major_rss_urls,
            top_n=settings.briefing_news_count,
            kr_ratio=settings.briefing_kr_ratio,
        )
        notifier.send(format_morning_briefing(ts, eco, items))
        logger.info("morning briefing sent: news=%s", len(items))
        return 0
    except Exception as exc:
        stack = "\n".join(traceback.format_exc().splitlines()[-5:])
        notifier.send(f"[2602_money] morning-briefing error\n{type(exc).__name__}: {exc}\n{stack}")
        logger.exception("morning briefing failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
