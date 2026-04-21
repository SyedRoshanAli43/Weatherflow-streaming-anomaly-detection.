"""
Lightweight streaming metrics logger.

Writes JSONL metrics suitable for plotting and downstream analysis.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
import json
import time


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
class MetricsLogger:
    """
    Track throughput (msg/s) and end-to-end latency (ms).

    This class is designed to be called on every processed message, but only writes
    to disk every `log_every_n` messages.
    """

    metrics_path: Path
    log_every_n: int = 25

    _count: int = 0
    _window_start_s: float = 0.0
    _lat_sum_ms: float = 0.0
    _lat_count: int = 0
    _last_offset: Optional[int] = None

    def __post_init__(self) -> None:
        self.metrics_path.parent.mkdir(parents=True, exist_ok=True)
        self._window_start_s = time.time()

    def observe(
        self,
        event: Dict[str, Any],
        *,
        kafka_partition: Optional[int] = None,
        kafka_offset: Optional[int] = None,
    ) -> None:
        """
        Observe a processed event and update aggregates.

        Latency is computed as (now - event_timestamp) when `event["timestamp"]`
        is ISO-8601 and parseable; otherwise it is omitted.
        """
        self._count += 1
        self._last_offset = kafka_offset if kafka_offset is not None else self._last_offset

        event_ts_ms = self._extract_event_timestamp_ms(event)
        if event_ts_ms is not None:
            now_ms = time.time() * 1000.0
            lat_ms = max(0.0, now_ms - event_ts_ms)
            self._lat_sum_ms += lat_ms
            self._lat_count += 1

        if self._count % max(1, self.log_every_n) == 0:
            self.flush(kafka_partition=kafka_partition, kafka_offset=kafka_offset)

    def flush(
        self,
        *,
        kafka_partition: Optional[int] = None,
        kafka_offset: Optional[int] = None,
    ) -> None:
        """Write a metrics record and reset the rolling window."""
        now_s = time.time()
        elapsed = max(1e-6, now_s - self._window_start_s)
        throughput = self.log_every_n / elapsed

        avg_latency_ms = (
            (self._lat_sum_ms / self._lat_count) if self._lat_count > 0 else None
        )

        record: Dict[str, Any] = {
            "ts_utc": _utc_now_iso(),
            "window_messages": self.log_every_n,
            "window_seconds": elapsed,
            "throughput_mps": throughput,
            "avg_latency_ms": avg_latency_ms,
            "kafka_partition": kafka_partition,
            "kafka_offset": kafka_offset if kafka_offset is not None else self._last_offset,
        }

        with self.metrics_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

        self._window_start_s = now_s
        self._lat_sum_ms = 0.0
        self._lat_count = 0

    @staticmethod
    def _extract_event_timestamp_ms(event: Dict[str, Any]) -> Optional[float]:
        """
        Parse event["timestamp"] (ISO-8601) into epoch milliseconds.

        Returns None if missing or parse fails.
        """
        ts = event.get("timestamp")
        if not ts or not isinstance(ts, str):
            return None

        # Be tolerant to timestamps without timezone.
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp() * 1000.0
        except Exception:
            return None

