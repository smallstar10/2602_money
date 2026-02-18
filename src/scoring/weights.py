from __future__ import annotations

import json

from src.core import db
from src.core.timeutil import kst_iso
from src.scoring.schema import DEFAULT_WEIGHTS


def load_active_weights(sqlite_path: str) -> dict[str, float]:
    row = db.fetchone(sqlite_path, "SELECT version, weights_json FROM weights WHERE active=1 ORDER BY version DESC LIMIT 1")
    if row is None:
        return DEFAULT_WEIGHTS.copy()
    return json.loads(row["weights_json"])


def activate_new_weights(sqlite_path: str, weights: dict[str, float]) -> int:
    db.execute(sqlite_path, "UPDATE weights SET active=0 WHERE active=1")
    return db.execute(
        sqlite_path,
        "INSERT INTO weights(ts_kst, weights_json, active) VALUES (?,?,1)",
        (kst_iso(), json.dumps(weights, ensure_ascii=True)),
    )
