from __future__ import annotations

DEFAULT_WEIGHTS: dict[str, float] = {
    "money_value_surge": 0.18,
    "flow_score": 0.14,
    "atr_regime": 0.10,
    "sector_breadth": 0.06,
    "sector_rotation": 0.07,
    "buzz_score": 0.03,
    "rs_5": 0.09,
    "momentum_persistence": 0.07,
    "drawdown_20": 0.05,
    "volatility_shock": 0.02,
    "trend_strength": 0.08,
    "breakout_20": 0.05,
    "range_position_20": 0.03,
    "efficiency_8": 0.03,
}

# Stored in candidates.features_json and reused by nightly tuner/report.
FEATURE_EXPORT_KEYS: tuple[str, ...] = (
    "money_value_surge",
    "volume_surge",
    "ma_trend",
    "atr_regime",
    "return_1h",
    "rs_5",
    "momentum_persistence",
    "drawdown_20",
    "volatility_shock",
    "trend_strength",
    "breakout_20",
    "range_position_20",
    "efficiency_8",
    "sector_breadth",
    "sector_rotation",
    "flow_score",
    "buzz_score",
)

# Raw feature -> scale range -> normalized score key name
SCORE_SCALE_MAP: dict[str, tuple[float, float, str]] = {
    "money_value_surge": (0.8, 3.0, "f_money"),
    "flow_score": (-1.0, 1.0, "f_flow"),
    "atr_regime": (0.7, 1.8, "f_atr"),
    "sector_breadth": (0.2, 0.9, "f_sector"),
    "sector_rotation": (-0.15, 0.35, "f_rotation"),
    "buzz_score": (0.0, 1.0, "f_buzz"),
    "rs_5": (-0.05, 0.12, "f_rs"),
    "momentum_persistence": (0.2, 0.9, "f_persist"),
    "drawdown_20": (-0.18, 0.0, "f_drawdown"),
    "volatility_shock": (0.7, 1.6, "f_volshock"),
    "trend_strength": (-0.03, 0.08, "f_trend"),
    "breakout_20": (-0.05, 0.06, "f_breakout"),
    "range_position_20": (0.2, 1.0, "f_rangepos"),
    "efficiency_8": (0.1, 0.85, "f_efficiency"),
}

