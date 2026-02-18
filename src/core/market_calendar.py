from __future__ import annotations

from datetime import datetime

import exchange_calendars as xcals
import pandas as pd

_XKRX = xcals.get_calendar("XKRX")


def is_krx_open_day(dt: datetime) -> bool:
    session_label = pd.Timestamp(dt.date())
    return bool(_XKRX.is_session(session_label))
