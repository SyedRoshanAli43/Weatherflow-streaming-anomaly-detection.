"""
Real-time anomaly detection for weather streams.

Implements two approaches:
- Rolling Z-score (default): dependency-free, fast, interpretable.
- Isolation Forest (optional): requires scikit-learn; better for multivariate anomalies.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple
import json
import math


def _utc_now_iso() -> str:
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _safe_float(v: Any) -> Optional[float]:
    """Convert to float if possible; otherwise return None."""
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


@dataclass
class AnomalyResult:
    """Represents anomaly detection output for a single event."""

    is_anomaly: bool
    method: str
    score: Optional[float]
    feature_scores: Dict[str, Optional[float]]

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to JSON-friendly dict."""
        return {
            "is_anomaly": self.is_anomaly,
            "method": self.method,
            "score": self.score,
            "feature_scores": self.feature_scores,
        }


class RollingZScoreDetector:
    """
    Rolling Z-score anomaly detector per stream key.

    Keeps a bounded window of recent values for each feature. For each event,
    computes z-scores and flags an anomaly if any feature exceeds threshold.
    """

    def __init__(
        self,
        *,
        features: List[str],
        window_size: int = 240,
        threshold: float = 3.5,
        min_samples: int = 30,
    ) -> None:
        self.features = features
        self.window_size = int(window_size)
        self.threshold = float(threshold)
        self.min_samples = int(min_samples)
        self._windows: Dict[Tuple[str, str], Dict[str, Deque[float]]] = {}

    def score(self, event: Dict[str, Any]) -> AnomalyResult:
        """Score an event and update state."""
        key = (str(event.get("city", "unknown")), str(event.get("source", "unknown")))
        if key not in self._windows:
            self._windows[key] = {f: deque(maxlen=self.window_size) for f in self.features}

        feature_z: Dict[str, Optional[float]] = {}
        max_abs_z = 0.0
        any_z = False

        for f in self.features:
            x = _safe_float(event.get(f))
            if x is None:
                feature_z[f] = None
                continue

            w = self._windows[key][f]
            z = self._zscore(x, w)
            feature_z[f] = z

            if z is not None:
                any_z = True
                max_abs_z = max(max_abs_z, abs(z))

            w.append(x)

        if not any_z:
            return AnomalyResult(False, "zscore", None, feature_z)

        is_anom = max_abs_z >= self.threshold
        return AnomalyResult(is_anom, "zscore", max_abs_z, feature_z)

    def _zscore(self, x: float, window: Iterable[float]) -> Optional[float]:
        """Compute z-score of x vs current window."""
        vals = list(window)
        if len(vals) < self.min_samples:
            return None
        mu = sum(vals) / len(vals)
        var = sum((v - mu) ** 2 for v in vals) / max(1, (len(vals) - 1))
        sd = math.sqrt(var)
        if sd <= 1e-12:
            return 0.0
        return (x - mu) / sd


class IsolationForestDetector:
    """
    Optional Isolation Forest detector (requires scikit-learn).

    Fits a per-stream model after collecting `min_samples` events.
    """

    def __init__(
        self,
        *,
        features: List[str],
        contamination: float = 0.01,
        min_samples: int = 200,
    ) -> None:
        self.features = features
        self.contamination = contamination
        self.min_samples = int(min_samples)

        self._buffers: Dict[Tuple[str, str], List[List[float]]] = {}
        self._models: Dict[Tuple[str, str], Any] = {}

    def score(self, event: Dict[str, Any]) -> AnomalyResult:
        """Score an event; trains lazily per stream key."""
        key = (str(event.get("city", "unknown")), str(event.get("source", "unknown")))
        x = self._vectorize(event)
        if x is None:
            return AnomalyResult(False, "isolation_forest", None, {})

        model = self._models.get(key)
        if model is None:
            buf = self._buffers.setdefault(key, [])
            buf.append(x)
            if len(buf) >= self.min_samples:
                model = self._fit(buf)
                self._models[key] = model
                self._buffers[key] = []
            return AnomalyResult(False, "isolation_forest", None, {})

        # sklearn returns -1 for anomaly, 1 for inlier
        pred = int(model.predict([x])[0])
        score = float(model.decision_function([x])[0])
        is_anom = pred == -1
        return AnomalyResult(is_anom, "isolation_forest", score, {})

    def _vectorize(self, event: Dict[str, Any]) -> Optional[List[float]]:
        """Extract numeric feature vector in fixed order."""
        vec: List[float] = []
        for f in self.features:
            v = _safe_float(event.get(f))
            if v is None:
                return None
            vec.append(v)
        return vec

    def _fit(self, samples: List[List[float]]) -> Any:
        """Fit sklearn IsolationForest."""
        try:
            from sklearn.ensemble import IsolationForest  # type: ignore
        except Exception as exc:
            raise ImportError(
                "IsolationForest requires scikit-learn. Either install it or use `zscore`."
            ) from exc

        model = IsolationForest(
            n_estimators=200,
            contamination=self.contamination,
            random_state=42,
            n_jobs=-1,
        )
        model.fit(samples)
        return model


@dataclass
class AnomalyEventLogger:
    """Writes anomaly events to JSONL."""

    anomaly_events_path: Path

    def __post_init__(self) -> None:
        self.anomaly_events_path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, event: Dict[str, Any], result: AnomalyResult) -> None:
        """Persist anomaly events (only when anomalous)."""
        if not result.is_anomaly:
            return

        record = {
            "ts_utc": _utc_now_iso(),
            "city": event.get("city"),
            "source": event.get("source"),
            "event_timestamp": event.get("timestamp"),
            "anomaly": result.to_dict(),
        }
        with self.anomaly_events_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

