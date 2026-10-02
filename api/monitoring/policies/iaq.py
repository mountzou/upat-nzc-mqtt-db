"""Canonical Indoor Air Quality threshold policy shared by API consumers."""

from __future__ import annotations

from typing import Literal


IAQPolicyMetric = Literal["co2", "pm25"]
IAQPeriodId = Literal["1m", "1h", "24h", "7d", "14d"]

IAQ_POLICY_VERSION = "indoor-environment-iaq-v2"

_IAQ_POLICY = {
    "co2": {
        "label": "CO2",
        "unit": "ppm",
        "thresholds": {
            "1m": 750.0,
            "1h": 750.0,
            "24h": 800.0,
            "7d": 800.0,
            "14d": 800.0,
        },
    },
    "pm25": {
        "label": "PM2.5",
        "unit": "µg/m³",
        "thresholds": {
            "1m": 10.0,
            "1h": 10.0,
            "24h": 15.0,
            "7d": 15.0,
            "14d": 15.0,
        },
    },
}

IAQ_THRESHOLD_PERIOD_LABELS = {
    period_id: {
        "en": (
            "configured short-window threshold"
            if period_id in {"1m", "1h"}
            else "configured aggregated-period threshold"
        ),
        "el": (
            "ρυθμισμένο όριο βραχέος παραθύρου"
            if period_id in {"1m", "1h"}
            else "ρυθμισμένο όριο συγκεντρωτικής περιόδου"
        ),
    }
    for period_id in ("1m", "1h", "24h", "7d", "14d")
}


def get_iaq_threshold(metric: IAQPolicyMetric, period_id: IAQPeriodId) -> float:
    return float(_IAQ_POLICY[metric]["thresholds"][period_id])


def get_iaq_threshold_values(metric: IAQPolicyMetric) -> dict[IAQPeriodId, float]:
    return dict(_IAQ_POLICY[metric]["thresholds"])


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
                "thresholds": get_iaq_threshold_values(metric),
            }
            for metric, policy in _IAQ_POLICY.items()
        },
    }
