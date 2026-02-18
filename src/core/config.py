from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str
    telegram_chat_id: str
    data_provider: str
    kis_app_key: str
    kis_app_secret: str
    kis_account_no: str
    kis_is_paper: bool
    universe: str
    top_n: int
    run_hourly_start: str
    run_hourly_end: str
    sqlite_path: str
    min_value_krw: float
    max_abs_return_1h: float
    analyst_backend: str
    analyst_model: str
    analyst_enable: bool
    paper_enable: bool
    paper_initial_cash: float
    paper_max_trades_per_day: int
    paper_max_positions: int
    paper_fee_bps: float
    paper_slippage_bps: float
    sp500_enable: bool
    event_risk_enable: bool
    event_feed_urls: str
    high_impact_dates: str
    strategy_lab_enable: bool


def load_settings() -> Settings:
    load_dotenv()
    return Settings(
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
        data_provider=os.getenv("DATA_PROVIDER", "kis").strip(),
        kis_app_key=os.getenv("KIS_APP_KEY", ""),
        kis_app_secret=os.getenv("KIS_APP_SECRET", ""),
        kis_account_no=os.getenv("KIS_ACCOUNT_NO", ""),
        kis_is_paper=os.getenv("KIS_IS_PAPER", "true").lower() == "true",
        universe=os.getenv("UNIVERSE", "KOSPI,KOSDAQ"),
        top_n=int(os.getenv("TOP_N", "5")),
        run_hourly_start=os.getenv("RUN_HOURLY_START", "08:00"),
        run_hourly_end=os.getenv("RUN_HOURLY_END", "17:00"),
        sqlite_path=os.getenv("SQLITE_PATH", "data/money.db"),
        min_value_krw=float(os.getenv("MIN_VALUE_KRW", "1000000000")),
        max_abs_return_1h=float(os.getenv("MAX_ABS_RETURN_1H", "0.2")),
        analyst_backend=os.getenv("ANALYST_BACKEND", "ollama"),
        analyst_model=os.getenv("ANALYST_MODEL", "gemma3:12b"),
        analyst_enable=os.getenv("ANALYST_ENABLE", "false").lower() == "true",
        paper_enable=os.getenv("PAPER_ENABLE", "true").lower() == "true",
        paper_initial_cash=float(os.getenv("PAPER_INITIAL_CASH", "1000000")),
        paper_max_trades_per_day=int(os.getenv("PAPER_MAX_TRADES_PER_DAY", "10")),
        paper_max_positions=int(os.getenv("PAPER_MAX_POSITIONS", "3")),
        paper_fee_bps=float(os.getenv("PAPER_FEE_BPS", "1.5")),
        paper_slippage_bps=float(os.getenv("PAPER_SLIPPAGE_BPS", "3.0")),
        sp500_enable=os.getenv("SP500_ENABLE", "true").lower() == "true",
        event_risk_enable=os.getenv("EVENT_RISK_ENABLE", "true").lower() == "true",
        event_feed_urls=os.getenv(
            "EVENT_FEED_URLS",
            "https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EGSPC&region=US&lang=en-US,"
            "https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EKS11&region=US&lang=en-US",
        ),
        high_impact_dates=os.getenv("HIGH_IMPACT_DATES", ""),
        strategy_lab_enable=os.getenv("STRATEGY_LAB_ENABLE", "true").lower() == "true",
    )


def ensure_parent_dir(path_str: str) -> None:
    Path(path_str).parent.mkdir(parents=True, exist_ok=True)
