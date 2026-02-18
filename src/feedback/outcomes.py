from __future__ import annotations

from datetime import timedelta

import pandas as pd

from src.core import db
from src.core.timeutil import now_kst


def _eligible(ts_kst: pd.Timestamp, horizon: str) -> bool:
    delta = {"1h": timedelta(hours=1), "4h": timedelta(hours=4), "1d": timedelta(days=1)}[horizon]
    return now_kst() - ts_kst.to_pydatetime().astimezone(now_kst().tzinfo) >= delta


def _later_snapshot_price(sqlite_path: str, ticker: str, target_ts_iso: str) -> float | None:
    row = db.fetchone(
        sqlite_path,
        """
        SELECT price
        FROM price_snapshots
        WHERE ticker=?
          AND ts_kst >= ?
        ORDER BY ts_kst ASC
        LIMIT 1
        """,
        (ticker, target_ts_iso),
    )
    if row is None:
        return None
    return float(row["price"])


def fill_outcomes(sqlite_path: str, fallback_latest_price_map: dict[str, float] | None = None) -> int:
    fallback_latest_price_map = fallback_latest_price_map or {}
    rows = db.fetchall(
        sqlite_path,
        """
        SELECT r.run_id, r.ts_kst, c.ticker, c.price
        FROM runs r
        JOIN candidates c ON c.run_id = r.run_id
        """,
    )
    inserted = 0
    for row in rows:
        ticker = row["ticker"]
        ts = pd.Timestamp(row["ts_kst"])
        price_then = float(row["price"] or 0)
        if price_then <= 0:
            continue

        for horizon in ["1h", "4h", "1d"]:
            if not _eligible(ts, horizon):
                continue

            target_ts = (ts.to_pydatetime().astimezone(now_kst().tzinfo) + {"1h": timedelta(hours=1), "4h": timedelta(hours=4), "1d": timedelta(days=1)}[horizon]).isoformat(timespec="seconds")
            later = _later_snapshot_price(sqlite_path, ticker, target_ts)
            if later is None and ticker in fallback_latest_price_map:
                later = float(fallback_latest_price_map[ticker])
            if later is None:
                continue

            ret = (later / price_then) - 1.0
            prev = db.fetchone(
                sqlite_path,
                """
                SELECT ret, price_then, price_later
                FROM outcomes
                WHERE run_id=? AND ticker=? AND horizon=?
                """,
                (row["run_id"], ticker, horizon),
            )
            if prev is not None:
                same_ret = abs(float(prev["ret"] or 0.0) - ret) < 1e-12
                same_then = abs(float(prev["price_then"] or 0.0) - price_then) < 1e-12
                same_later = abs(float(prev["price_later"] or 0.0) - later) < 1e-12
                if same_ret and same_then and same_later:
                    continue
            db.execute(
                sqlite_path,
                """
                INSERT OR REPLACE INTO outcomes(run_id, ticker, horizon, ret, price_then, price_later)
                VALUES (?,?,?,?,?,?)
                """,
                (row["run_id"], ticker, horizon, ret, price_then, later),
            )
            inserted += 1
    return inserted
