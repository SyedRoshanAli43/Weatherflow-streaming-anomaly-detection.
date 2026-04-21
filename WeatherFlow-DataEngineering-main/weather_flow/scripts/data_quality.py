"""
Real-time data quality monitoring for weather stream events.

The goal is to detect and record quality issues (missing values, schema drift,
physically implausible values) early in the pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import json


def _utc_now_iso() -> str:
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _is_number(v: Any) -> bool:
    """Return True if `v` can be interpreted as a finite float."""
    try:
        if v is None:
            return False
        float(v)
        return True
    except (TypeError, ValueError):
        return False


@dataclass
class DataQualityIssue:
    """Single data quality issue record."""

    issue_type: str
    field: Optional[str]
    message: str

    def to_dict(self) -> Dict[str, Any]:
        """Serialize issue to a JSON-friendly dict."""
        return {"type": self.issue_type, "field": self.field, "message": self.message}


@dataclass
class DataQualityMonitor:
    """
    Validate weather events and emit quality issue events to JSONL.

    The monitor is intentionally conservative: it does not mutate the event;
    it only returns a list of issues, and optionally writes them to disk.
    """

    required_fields: List[str]
    bounds: Dict[str, Dict[str, float]]
    quality_events_path: Path

    def __post_init__(self) -> None:
        self.quality_events_path.parent.mkdir(parents=True, exist_ok=True)

    def validate(self, event: Dict[str, Any]) -> List[DataQualityIssue]:
        """
        Validate a single event.

        Args:
            event: Weather event dict.

        Returns:
            List of `DataQualityIssue`. Empty list means "passes checks".
        """
        issues: List[DataQualityIssue] = []

        for field in self.required_fields:
            if field not in event:
                issues.append(
                    DataQualityIssue(
                        issue_type="missing_field",
                        field=field,
                        message=f"Required field `{field}` is missing.",
                    )
                )
            elif event.get(field) is None:
                issues.append(
                    DataQualityIssue(
                        issue_type="null_value",
                        field=field,
                        message=f"Required field `{field}` is null.",
                    )
                )

        # Numeric plausibility bounds
        self._check_bounds(event, issues)
        return issues

    def emit(self, event: Dict[str, Any], issues: List[DataQualityIssue]) -> None:
        """
        Persist quality issues (if any) as JSONL.

        Args:
            event: The original event.
            issues: List of issues detected.
        """
        if not issues:
            return

        record = {
            "ts_utc": _utc_now_iso(),
            "city": event.get("city"),
            "source": event.get("source"),
            "event_timestamp": event.get("timestamp"),
            "issues": [i.to_dict() for i in issues],
        }

        with self.quality_events_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def _check_bounds(self, event: Dict[str, Any], issues: List[DataQualityIssue]) -> None:
        """Check basic plausibility bounds for core numeric fields."""
        mapping: List[Tuple[str, str]] = [
            ("temperature", "temperature_c"),
            ("humidity", "humidity_pct"),
            ("pressure", "pressure_hpa"),
            ("wind_speed", "wind_speed_mps"),
        ]

        for field, bounds_key in mapping:
            if field not in event:
                continue

            v = event.get(field)
            if v is None:
                continue
            if not _is_number(v):
                issues.append(
                    DataQualityIssue(
                        issue_type="type_mismatch",
                        field=field,
                        message=f"Expected numeric for `{field}`, got {type(v).__name__}.",
                    )
                )
                continue

            b = self.bounds.get(bounds_key) or {}
            mn = b.get("min")
            mx = b.get("max")
            if mn is None or mx is None:
                continue

            fv = float(v)
            if fv < float(mn) or fv > float(mx):
                issues.append(
                    DataQualityIssue(
                        issue_type="out_of_bounds",
                        field=field,
                        message=f"`{field}`={fv} outside [{mn}, {mx}]",
                    )
                )

