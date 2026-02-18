from __future__ import annotations

import json
from typing import Any

import requests

from src.core.config import Settings


def _build_prompt(row: dict[str, Any]) -> str:
    payload = {
        "ticker": row.get("ticker"),
        "name": row.get("name"),
        "score": row.get("score"),
        "money_value_surge": row.get("money_value_surge"),
        "volume_surge": row.get("volume_surge"),
        "flow_score": row.get("flow_score"),
        "atr_regime": row.get("atr_regime"),
        "sector_breadth": row.get("sector_breadth"),
        "return_1h": row.get("return_1h"),
        "ma_trend": row.get("ma_trend"),
        "rs_5": row.get("rs_5"),
        "momentum_persistence": row.get("momentum_persistence"),
        "drawdown_20": row.get("drawdown_20"),
        "volatility_shock": row.get("volatility_shock"),
        "trend_strength": row.get("trend_strength"),
        "breakout_20": row.get("breakout_20"),
        "range_position_20": row.get("range_position_20"),
        "efficiency_8": row.get("efficiency_8"),
    }
    return (
        "당신은 한국주식 이벤트드리븐 애널리스트다. 투자 권유 문구 없이, 관찰/무효화 관점으로 2문장만 작성하라. "
        "첫 문장은 왜 자금이 몰릴 수 있는지, 둘째 문장은 무효화 조건을 써라.\n"
        f"입력={json.dumps(payload, ensure_ascii=False)}"
    )


def build_analyst_note(settings: Settings, row: dict[str, Any]) -> str | None:
    if not settings.analyst_enable:
        return None
    if settings.analyst_backend.lower() != "ollama":
        return None

    prompt = _build_prompt(row)
    try:
        resp = requests.post(
            "http://127.0.0.1:11434/api/generate",
            json={
                "model": settings.analyst_model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.2},
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        text = str(data.get("response", "")).strip()
        return text if text else None
    except Exception:
        return None
