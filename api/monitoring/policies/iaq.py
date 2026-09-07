"""Canonical Indoor Air Quality threshold policy shared by API consumers."""

from __future__ import annotations

from typing import Literal


IAQPolicyMetric = Literal["co2", "pm25"]
IAQThresholdWindow = Literal["short", "long"]

IAQ_POLICY_VERSION = "indoor-environment-iaq-v1"

_IAQ_POLICY = {
    "co2": {
        "label": "CO2",
        "unit": "ppm",
        "thresholds": {
            "short": {"value": 750.0, "period_ids": ("1m", "1h")},
            "long": {"value": 800.0, "period_ids": ("24h", "7d", "14d")},
        },
    },
    "pm25": {
        "label": "PM2.5",
        "unit": "µg/m³",
        "thresholds": {
            "short": {"value": 10.0, "period_ids": ("1m", "1h")},
            "long": {"value": 15.0, "period_ids": ("24h", "7d", "14d")},
        },
    },
}

IAQ_THRESHOLD_WINDOW_LABELS = {
    "short": {
        "en": "configured short-window threshold",
        "el": "ρυθμισμένο όριο βραχέος παραθύρου",
    },
    "long": {
        "en": "configured aggregated-period threshold",
        "el": "ρυθμισμένο όριο συγκεντρωτικής περιόδου",
    },
}


def get_iaq_threshold(metric: IAQPolicyMetric, window: IAQThresholdWindow) -> float:
    return float(_IAQ_POLICY[metric]["thresholds"][window]["value"])


def get_iaq_threshold_values(metric: IAQPolicyMetric) -> dict[str, float]:
    return {
        window: float(config["value"])
        for window, config in _IAQ_POLICY[metric]["thresholds"].items()
    }


def get_iaq_metric_descriptor(metric: IAQPolicyMetric) -> dict[str, str]:
    policy = _IAQ_POLICY[metric]
    return {"label": str(policy["label"]), "unit": str(policy["unit"])}


def build_iaq_policy_payload() -> dict:
    return {
        "version": IAQ_POLICY_VERSION,
        "metrics": {
            metric: {
                "label": policy["label"],
                "unit": policy["unit"],
                "thresholds": {
                    window: {
                        "value": float(config["value"]),
                        "period_ids": list(config["period_ids"]),
                    }
                    for window, config in policy["thresholds"].items()
                },
            }
            for metric, policy in _IAQ_POLICY.items()
        },
    }
