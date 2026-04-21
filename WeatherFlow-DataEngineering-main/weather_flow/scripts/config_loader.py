"""
Configuration loader for WeatherFlow.

This module centralizes configuration in `weather_flow/config.yaml` and environment
variables, removing the need for scattered hardcoded constants.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
import os

try:
    import yaml
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "Missing dependency `pyyaml`. Install it via `pip install pyyaml`."
    ) from exc


def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep-merge two dictionaries.

    Values from `overlay` override values in `base`.
    """
    out: Dict[str, Any] = dict(base)
    for k, v in overlay.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def default_config_path() -> Path:
    """Return the default config file path inside the repository."""
    # .../weather_flow/scripts/config_loader.py -> .../weather_flow/config.yaml
    return Path(__file__).resolve().parents[1] / "config.yaml"


def load_yaml_config(path: Optional[str | Path] = None) -> Dict[str, Any]:
    """
    Load YAML configuration from disk.

    Args:
        path: Optional override path. If not provided, uses env var
            WEATHERFLOW_CONFIG_PATH, else defaults to `weather_flow/config.yaml`.

    Returns:
        Parsed configuration dict (empty if file missing).
    """
    config_path = Path(
        path
        or os.environ.get("WEATHERFLOW_CONFIG_PATH", str(default_config_path()))
    ).expanduser()

    if not config_path.exists():
        return {}

    with config_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        raise ValueError(f"Config must be a mapping, got {type(data)} at {config_path}")
    return data


def env_overrides() -> Dict[str, Any]:
    """
    Build a minimal overlay config from environment variables.

    This keeps secrets out of `config.yaml` while still allowing container-friendly
    configuration via Docker Compose.
    """
    overlay: Dict[str, Any] = {}

    kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if kafka_bootstrap:
        overlay = _deep_merge(
            overlay,
            {"kafka": {"bootstrap_servers": [s.strip() for s in kafka_bootstrap.split(",") if s.strip()]}},
        )

    ingestion_interval = os.environ.get("WEATHERFLOW_INGESTION_INTERVAL_SECONDS")
    if ingestion_interval:
        try:
            overlay = _deep_merge(
                overlay, {"ingestion": {"interval_seconds": int(ingestion_interval)}}
            )
        except ValueError:
            pass

    return overlay


def get_config(path: Optional[str | Path] = None) -> Dict[str, Any]:
    """
    Load the effective configuration.

    Precedence: YAML file < environment overrides.
    """
    base = load_yaml_config(path=path)
    return _deep_merge(base, env_overrides())


def get_path(cfg: Dict[str, Any], key: str, default: str) -> Path:
    """
    Resolve a storage path from config relative to repository root.

    Args:
        cfg: Loaded configuration.
        key: Config key path under `storage` (e.g., "processed_dir").
        default: Default relative path.

    Returns:
        Absolute `Path`.
    """
    storage = cfg.get("storage", {}) or {}
    rel = storage.get(key, default)

    # Repo root is .../weather_flow
    repo_root = Path(__file__).resolve().parents[1]
    return (repo_root / rel).resolve()

