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
from src.notify.formatters import format_ecosystem_status, format_training_report
from src.notify.telegram_notify import TelegramNotifier
from src.ops import collect_ecosystem_status
from src.paper.training_coach import build_training_report, save_training_report

logger = get_logger(__name__)


def main() -> int:
    settings = load_settings()
    db.init_db(settings.sqlite_path)
    notifier = TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id)
    ts = now_kst()

    try:
        report = build_training_report(
            settings.sqlite_path,
            lookback_days=settings.training_lookback_days,
            min_days=settings.training_min_days,
            min_trades=settings.training_min_trades,
            target_return=settings.training_target_return,
            max_drawdown_limit=settings.training_max_drawdown,
            base_risk_per_trade_pct=settings.training_base_risk_per_trade_pct,
            base_daily_loss_pct=settings.training_base_daily_loss_limit_pct,
            base_max_new_positions=settings.training_base_max_new_positions,
            now=ts,
        )
        save_training_report(settings.sqlite_path, report, mode="SCHEDULED_INTRADAY", note="scheduled@09:10,12:10,15:10")

        eco = collect_ecosystem_status(settings)
        msg = (
            f"[KST {ts.strftime('%Y-%m-%d %H:%M')}] 중간점검 리포트\n"
            f"- 스케줄: 09:10 / 12:10 / 15:10\n\n"
            f"{format_training_report(ts, report)}\n\n"
            f"{format_ecosystem_status(ts, eco)}"
        )
        notifier.send(msg)
        logger.info("intraday training status sent")
        return 0
    except Exception as exc:
        stack = "\n".join(traceback.format_exc().splitlines()[-5:])
        notifier.send(f"[2602_money] intraday-status error\n{type(exc).__name__}: {exc}\n{stack}")
        logger.exception("intraday training status failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
