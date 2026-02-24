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
from src.feedback.nightly_report import build_factor_diagnostics, build_nightly_stats, build_paper_stats
from src.feedback.outcomes import fill_outcomes
from src.feedback.rebalance import update_strategy_state
from src.feedback.weight_tuner import tune_weights
from src.notify.formatters import format_nightly_message
from src.notify.telegram_notify import TelegramNotifier
from src.paper.training_coach import build_training_report, save_training_report
from src.research.strategy_lab import latest_strategy_lab, run_strategy_lab
from src.scoring.weights import load_active_weights

logger = get_logger(__name__)


def main() -> int:
    settings = load_settings()
    db.init_db(settings.sqlite_path)
    notifier = TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id)

    try:
        inserted = fill_outcomes(settings.sqlite_path)
        base = load_active_weights(settings.sqlite_path)
        _, status = tune_weights(settings.sqlite_path, base_weights=base)

        stats = build_nightly_stats(settings.sqlite_path)
        stats.update(build_factor_diagnostics(settings.sqlite_path))
        stats.update(build_paper_stats(settings.sqlite_path))
        if settings.strategy_lab_enable:
            lab = run_strategy_lab(settings.sqlite_path)
        else:
            lab = latest_strategy_lab(settings.sqlite_path)
        stats["strategy_lab_summary"] = str(lab.get("summary", "N/A"))
        strategy, strategy_update = update_strategy_state(settings.sqlite_path, stats)
        stats["regime"] = strategy["regime"]
        stats["regime_update"] = strategy_update
        stats["entry_score_threshold"] = strategy["entry_score_threshold"]
        stats["position_scale"] = strategy["position_scale"]
        training = build_training_report(
            settings.sqlite_path,
            lookback_days=settings.training_lookback_days,
            min_days=settings.training_min_days,
            min_trades=settings.training_min_trades,
            target_return=settings.training_target_return,
            max_drawdown_limit=settings.training_max_drawdown,
            base_risk_per_trade_pct=settings.training_base_risk_per_trade_pct,
            base_daily_loss_pct=settings.training_base_daily_loss_limit_pct,
            base_max_new_positions=settings.training_base_max_new_positions,
            now=now_kst(),
        )
        save_training_report(settings.sqlite_path, training, mode="nightly", note="nightly-summary")
        rp = training.get("risk_plan", {})
        stats["training_level"] = str(training.get("level_text", training.get("level", "N/A")))
        stats["training_score"] = float(training.get("score", 0.0))
        stats["training_ready"] = bool(training.get("ready", False))
        stats["training_risk_per_trade_pct"] = float(rp.get("risk_per_trade_pct", 0.0))
        stats["training_daily_loss_limit_pct"] = float(rp.get("daily_loss_limit_pct", 0.0))
        stats["training_max_new_positions"] = int(rp.get("max_new_positions", 0))
        stats["weight_update"] = status
        msg = format_nightly_message(now_kst(), stats)
        notifier.send(msg)
        logger.info("nightly run done: outcomes upsert=%s", inserted)
        return 0

    except Exception as exc:
        stack = "\n".join(traceback.format_exc().splitlines()[-5:])
        notifier.send(f"[2602_money] nightly error\n{type(exc).__name__}: {exc}\n{stack}")
        logger.exception("nightly failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
