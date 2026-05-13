"""Worker detection service — binary + model detection for Kronos Pipeline.

Provides:
  - GET /api/worker-info — returns installed binaries and available models
  - Model rotation on 429 errors (auto-retry with next model)
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

LITELLM_CONFIG_PATH = Path(os.environ.get(
    "LITELLM_CONFIG",
    "/etc/litellm/config.yaml",
))


def detect_binaries() -> dict[str, Any]:
    """Detect installed CLIs."""
    claude_path = shutil.which("claude")
    claudio_path = shutil.which("claudio")
    claudio_dir = Path.home() / ".claudio"

    return {
        "claude": {
            "installed": claude_path is not None,
            "path": claude_path,
        },
        "claudio": {
            "installed": claudio_path is not None,
            "path": claudio_path,
            "config_dir": str(claudio_dir) if claudio_dir.exists() else None,
        },
    }


def parse_litellm_models() -> list[dict[str, Any]]:
    """Parse litellm config YAML and return available models."""
    try:
        import yaml
    except ImportError:
        return _parse_litellm_fallback()

    if not LITELLM_CONFIG_PATH.exists():
        return []

    try:
        with open(LITELLM_CONFIG_PATH) as f:
            data = yaml.safe_load(f)
    except Exception:
        return _parse_litellm_fallback()

    models = []
    for entry in (data or {}).get("model_list", []):
        name = entry.get("model_name", "")
        params = entry.get("litellm_params", {})
        if name:
            models.append({
                "name": name,
                "provider": params.get("model", "").split("/")[0] if params.get("model") else None,
                "api_base": params.get("api_base"),
            })
    return models


def _parse_litellm_fallback() -> list[dict[str, Any]]:
    """Fallback: parse YAML with regex if pyyaml not installed."""
    import re
    if not LITELLM_CONFIG_PATH.exists():
        return []
    text = LITELLM_CONFIG_PATH.read_text()
    models = []
    current = {}
    for line in text.split("\n"):
        m = re.match(r"^\s+-\s+model_name:\s+(.+)", line)
        if m:
            if current.get("name"):
                models.append(current)
            current = {"name": m.group(1).strip()}
            continue
        m = re.match(r"^\s+model:\s+(.+)", line)
        if m and current:
            current["provider"] = m.group(1).strip().split("/")[0]
        m = re.match(r"^\s+api_base:\s+(.+)", line)
        if m and current:
            current["api_base"] = m.group(1).strip()
    if current.get("name"):
        models.append(current)
    return models


def get_worker_info() -> dict[str, Any]:
    """Full worker info response."""
    binaries = detect_binaries()
    models = parse_litellm_models() if binaries.get("claudio", {}).get("installed") else []

    # Also detect litellm proxy status
    litellm_running = False
    try:
        r = subprocess.run(
            ["systemctl", "is-active", "litellm-proxy"],
            capture_output=True, text=True, timeout=3,
        )
        litellm_running = r.stdout.strip() == "active"
    except Exception:
        pass

    return {
        "binaries": binaries,
        "models": models,
        "litellm_proxy_running": litellm_running,
        "default_binary": "claude" if binaries.get("claude", {}).get("installed") else "claudio",
    }
