from __future__ import annotations

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.config import load_settings
from src.providers import load_provider


if __name__ == "__main__":
    settings = load_settings()
    provider = load_provider(settings)
    uni = provider.get_universe(settings.universe)

    out = Path("data/universe_snapshot.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "name", "market"])
        writer.writeheader()
        writer.writerows(uni)
    print(f"saved: {out} rows={len(uni)}")
