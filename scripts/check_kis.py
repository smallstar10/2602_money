from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.config import load_settings
from src.providers import load_provider


if __name__ == "__main__":
    s = load_settings()
    p = load_provider(s)

    universe = p.get_universe(s.universe)
    print(f"universe_count={len(universe)}")
    sample = [x["ticker"] for x in universe[:3]]
    print("sample_tickers=", sample)

    ohlcv = p.get_latest_ohlcv(sample, interval="60m")
    print(f"ohlcv_rows={len(ohlcv)}")

    flows = p.get_investor_flow(sample)
    print(f"flow_rows={len(flows)}")
