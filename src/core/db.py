from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from typing import Any, Iterable

from src.core.config import ensure_parent_dir
from src.scoring.schema import DEFAULT_WEIGHTS


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runs (
  run_id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_kst TEXT NOT NULL,
  provider TEXT NOT NULL,
  universe TEXT NOT NULL,
  top_n INTEGER NOT NULL,
  note TEXT
);

CREATE TABLE IF NOT EXISTS candidates (
  run_id INTEGER NOT NULL,
  ticker TEXT NOT NULL,
  name TEXT,
  score REAL NOT NULL,
  price REAL,
  features_json TEXT,
  rationale TEXT,
  FOREIGN KEY(run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS outcomes (
  run_id INTEGER NOT NULL,
  ticker TEXT NOT NULL,
  horizon TEXT NOT NULL,
  ret REAL,
  price_then REAL,
  price_later REAL,
  PRIMARY KEY (run_id, ticker, horizon)
);

CREATE TABLE IF NOT EXISTS weights (
  version INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_kst TEXT NOT NULL,
  weights_json TEXT NOT NULL,
  active INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS user_feedback (
  ts_kst TEXT NOT NULL,
  ticker TEXT NOT NULL,
  action TEXT NOT NULL,
  note TEXT
);

CREATE TABLE IF NOT EXISTS paper_accounts (
  account_id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_kst TEXT NOT NULL,
  cash REAL NOT NULL,
  nav REAL NOT NULL,
  note TEXT
);

CREATE TABLE IF NOT EXISTS paper_positions (
  ticker TEXT PRIMARY KEY,
  name TEXT,
  qty INTEGER NOT NULL,
  avg_price REAL NOT NULL,
  updated_ts_kst TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_orders (
  order_id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_kst TEXT NOT NULL,
  side TEXT NOT NULL,
  ticker TEXT NOT NULL,
  name TEXT,
  qty INTEGER NOT NULL,
  price REAL NOT NULL,
  reason TEXT,
  run_id INTEGER
);

CREATE TABLE IF NOT EXISTS live_accounts (
  snap_id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_kst TEXT NOT NULL,
  cash REAL NOT NULL,
  total_eval REAL NOT NULL,
  total_asset REAL NOT NULL,
  note TEXT
);

CREATE TABLE IF NOT EXISTS live_positions (
  ticker TEXT PRIMARY KEY,
  name TEXT,
  qty INTEGER NOT NULL,
  avg_price REAL NOT NULL,
  last_price REAL NOT NULL,
  eval_amount REAL NOT NULL,
  pnl_amount REAL NOT NULL,
  pnl_pct REAL NOT NULL,
  updated_ts_kst TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS live_orders (
  order_id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_kst TEXT NOT NULL,
  side TEXT NOT NULL,
  ticker TEXT NOT NULL,
  name TEXT,
  qty INTEGER NOT NULL,
  price REAL NOT NULL,
  order_no TEXT,
  status TEXT NOT NULL,
  reason TEXT,
  run_id INTEGER
);

CREATE TABLE IF NOT EXISTS price_snapshots (
  run_id INTEGER NOT NULL,
  ts_kst TEXT NOT NULL,
  ticker TEXT NOT NULL,
  price REAL NOT NULL,
  PRIMARY KEY (run_id, ticker)
);

CREATE TABLE IF NOT EXISTS strategy_state (
  state_id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_kst TEXT NOT NULL,
  regime TEXT NOT NULL,
  entry_score_threshold REAL NOT NULL,
  position_scale REAL NOT NULL,
  note TEXT,
  active INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS strategy_experiments (
  exp_id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_kst TEXT NOT NULL,
  params_json TEXT NOT NULL,
  metrics_json TEXT NOT NULL,
  objective REAL NOT NULL,
  active INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS bot_state (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_ts_kst TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS training_reports (
  report_id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_kst TEXT NOT NULL,
  mode TEXT NOT NULL,
  score REAL NOT NULL,
  level TEXT NOT NULL,
  ready INTEGER NOT NULL,
  metrics_json TEXT NOT NULL,
  checklist_json TEXT NOT NULL,
  note TEXT
);
"""

DEFAULT_WEIGHTS_JSON = json.dumps(DEFAULT_WEIGHTS, ensure_ascii=True)


@contextmanager
def get_conn(sqlite_path: str):
    ensure_parent_dir(sqlite_path)
    conn = sqlite3.connect(sqlite_path)
    try:
        conn.row_factory = sqlite3.Row
        yield conn
    finally:
        conn.close()


def init_db(sqlite_path: str) -> None:
    with get_conn(sqlite_path) as conn:
        conn.executescript(SCHEMA_SQL)
        cur = conn.execute("SELECT version, weights_json FROM weights WHERE active=1 ORDER BY version DESC LIMIT 1")
        row = cur.fetchone()
        active_cnt = 1 if row else 0
        if active_cnt == 0:
            conn.execute(
                "INSERT INTO weights(ts_kst, weights_json, active) VALUES (datetime('now', '+9 hours'), ?, 1)",
                (DEFAULT_WEIGHTS_JSON,),
            )
        else:
            defaults = json.loads(DEFAULT_WEIGHTS_JSON)
            loaded = json.loads(row["weights_json"])
            patched = loaded.copy()
            changed = False
            for k, v in defaults.items():
                if k not in patched:
                    patched[k] = v
                    changed = True
            if changed:
                s = sum(float(x) for x in patched.values()) or 1.0
                norm = {k: float(v) / s for k, v in patched.items()}
                conn.execute("UPDATE weights SET weights_json=? WHERE version=?", (json.dumps(norm, ensure_ascii=True), row["version"]))

        acc = conn.execute("SELECT COUNT(*) FROM paper_accounts").fetchone()[0]
        if int(acc) == 0:
            conn.execute(
                "INSERT INTO paper_accounts(ts_kst, cash, nav, note) VALUES (datetime('now', '+9 hours'), ?, ?, ?)",
                (1000000.0, 1000000.0, "paper-init"),
            )

        state = conn.execute("SELECT COUNT(*) FROM strategy_state WHERE active=1").fetchone()[0]
        if int(state) == 0:
            conn.execute(
                """
                INSERT INTO strategy_state(
                    ts_kst, regime, entry_score_threshold, position_scale, note, active
                ) VALUES (datetime('now', '+9 hours'), 'NEUTRAL', 55.0, 1.0, 'init', 1)
                """
            )
        conn.commit()


def execute(sqlite_path: str, query: str, params: Iterable[Any] = ()) -> int:
    with get_conn(sqlite_path) as conn:
        cur = conn.execute(query, tuple(params))
        conn.commit()
        return cur.lastrowid


def executemany(sqlite_path: str, query: str, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    with get_conn(sqlite_path) as conn:
        conn.executemany(query, rows)
        conn.commit()


def fetchall(sqlite_path: str, query: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
    with get_conn(sqlite_path) as conn:
        cur = conn.execute(query, tuple(params))
        return cur.fetchall()


def fetchone(sqlite_path: str, query: str, params: Iterable[Any] = ()) -> sqlite3.Row | None:
    with get_conn(sqlite_path) as conn:
        cur = conn.execute(query, tuple(params))
        return cur.fetchone()
