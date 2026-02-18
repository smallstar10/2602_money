from __future__ import annotations

import tarfile
import traceback
from datetime import datetime, timedelta
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.config import ensure_parent_dir, load_settings
from src.core.logger import get_logger
from src.core.timeutil import now_kst
from src.notify.telegram_notify import TelegramNotifier

logger = get_logger(__name__)


def _backup_targets(settings) -> list[Path]:
    project_root = ROOT
    money_db = Path(settings.sqlite_path)
    if not money_db.is_absolute():
        money_db = project_root / money_db
    return [
        money_db,
        Path(settings.ecosystem_hotdeal_db_path),
        Path(settings.ecosystem_blog_stats_csv_path),
        Path(settings.ecosystem_blog_daily_state_path),
    ]


def _prune_old(backups_dir: Path, retention_days: int, now_ts: datetime) -> int:
    cutoff = now_ts - timedelta(days=max(1, int(retention_days)))
    removed = 0
    for p in backups_dir.glob("money2602-backup-*.tar.gz"):
        try:
            mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=now_ts.tzinfo)
            if mtime < cutoff:
                p.unlink(missing_ok=True)
                removed += 1
        except Exception:
            continue
    return removed


def main() -> int:
    settings = load_settings()
    notifier = TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id)
    now_ts = now_kst()

    try:
        backups_dir = Path(settings.backup_dir)
        if not backups_dir.is_absolute():
            backups_dir = ROOT / backups_dir
        ensure_parent_dir(str(backups_dir / "dummy.txt"))
        backups_dir.mkdir(parents=True, exist_ok=True)

        backup_name = f"money2602-backup-{now_ts.strftime('%Y%m%d-%H%M%S')}.tar.gz"
        backup_path = backups_dir / backup_name

        saved = 0
        with tarfile.open(backup_path, "w:gz") as tar:
            for p in _backup_targets(settings):
                if not p.exists() or not p.is_file():
                    continue
                try:
                    arc = p.relative_to(Path("/home/hyeonbin"))
                except Exception:
                    arc = Path("misc") / p.name
                tar.add(str(p), arcname=str(arc))
                saved += 1

        removed = _prune_old(backups_dir, settings.backup_retention_days, now_ts)
        notifier.send(
            "[2602_money backup]\n"
            f"- 파일: {backup_path}\n"
            f"- 포함 파일 수: {saved}\n"
            f"- 오래된 백업 정리: {removed}개"
        )
        logger.info("backup done: %s files=%s removed=%s", backup_path, saved, removed)
        return 0
    except Exception as exc:
        stack = "\n".join(traceback.format_exc().splitlines()[-5:])
        notifier.send(f"[2602_money] backup error\n{type(exc).__name__}: {exc}\n{stack}")
        logger.exception("backup failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
