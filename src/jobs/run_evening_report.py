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
from src.notify.formatters import format_evening_report
from src.notify.telegram_notify import TelegramNotifier
from src.ops import collect_ecosystem_status

logger = get_logger(__name__)


def _money_summary(sqlite_path: str) -> dict:
    out = {"money_runs_today": 0, "avg_score_latest": 0.0}
    row = db.fetchone(
        sqlite_path,
        "SELECT COUNT(*) AS n FROM runs WHERE ts_kst >= datetime('now', '+9 hours', 'start of day')",
    )
    if row:
        out["money_runs_today"] = int(row["n"])
    row2 = db.fetchone(
        sqlite_path,
        """
        SELECT AVG(score) AS s
        FROM candidates
        WHERE run_id = (SELECT MAX(run_id) FROM runs)
        """,
    )
    if row2 and row2["s"] is not None:
        out["avg_score_latest"] = float(row2["s"])
    return out


def main() -> int:
    settings = load_settings()
    db.init_db(settings.sqlite_path)
    notifier = TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id)
    ts = now_kst()

    try:
        eco = collect_ecosystem_status(settings)
        summary = _money_summary(settings.sqlite_path)
        notifier.send(format_evening_report(ts, eco, summary))
        logger.info("evening report sent")
        return 0
    except Exception as exc:
        stack = "\n".join(traceback.format_exc().splitlines()[-5:])
        notifier.send(f"[2602_money] evening-report error\n{type(exc).__name__}: {exc}\n{stack}")
        logger.exception("evening report failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
