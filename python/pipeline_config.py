"""Pipeline configuration — single source of truth for all tuneable params.

Every other module imports from here. No magic numbers scattered across files.
Override any value via environment variables.
"""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# ─── Paths ───────────────────────────────────────────────────────────────

DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", str(ROOT / "data")))
CONFIG_DIR = Path(os.environ.get("PIPELINE_CONFIG_DIR", str(ROOT / "config")))
LOGS_DIR = Path(os.environ.get("PIPELINE_LOGS_DIR", str(ROOT / "logs")))
AGENT_WORK_DIR = DATA_DIR / "agent-work"
ARTIFACTS_DIR = DATA_DIR / "artifacts"
MINI_CLAUDE_MD = CONFIG_DIR / "agent_claude_md.txt"

# ─── Network ─────────────────────────────────────────────────────────────

SDK_HOST = os.environ.get("PIPELINE_HOST", "0.0.0.0")
SDK_PORT = int(os.environ.get("PIPELINE_PORT", "8199"))
SDK_URL = os.environ.get("PIPELINE_SDK_URL", f"http://localhost:{SDK_PORT}")

# ─── Pipeline graph ─────────────────────────────────────────────────────

PIPELINE_STEPS = [
    "wide_research",
    "prompt_enhancer",
    "planner",
    "executor",
    "validator_1",
    "validator_2",
]

STEP_STATUS_MAP = {
    "wide_research": "researching",
    "prompt_enhancer": "enhancing",
    "planner": "planning",
    "executor": "executing",
    "validator_1": "validating_1",
    "validator_2": "validating_2",
}

# ─── Retry / limits ─────────────────────────────────────────────────────

MAX_RETRIES = int(os.environ.get("PIPELINE_MAX_RETRIES", "3"))

# ─── Per-agent defaults ─────────────────────────────────────────────────

AGENT_DEFAULTS: dict[str, dict] = {
    "wide_research": {
        "timeout_seconds": 600,
        "max_turns": 25,
        "effort": "high",
    },
    "prompt_enhancer": {
        "timeout_seconds": 300,
        "max_turns": 15,
        "effort": "high",
    },
    "planner": {
        "timeout_seconds": 600,
        "max_turns": 30,
        "effort": "high",
    },
    "executor": {
        "timeout_seconds": 900,
        "max_turns": 50,
        "effort": "high",
    },
    "validator_1": {
        "timeout_seconds": 600,
        "max_turns": 20,
        "effort": "high",
    },
    "validator_2": {
        "timeout_seconds": 900,
        "max_turns": 30,
        "effort": "high",
    },
}

# ─── Claude binary ──────────────────────────────────────────────────────

CLAUDE_BIN = os.environ.get("CLAUDE_BIN", "claude")

# ─── Disallowed tools (safety guard) ────────────────────────────────────

DISALLOWED_TOOLS = [
    "Bash(mysql *)",
    "Bash(mysqladmin *)",
    "Bash(mysqldump *)",
    "Bash(systemctl restart*)",
    "Bash(systemctl stop*)",
    "Bash(systemctl start*)",
    "Bash(systemctl reload*)",
    "Bash(systemctl kill*)",
    "Bash(systemctl enable*)",
    "Bash(systemctl disable*)",
    "Bash(systemctl mask*)",
    "Bash(systemctl unmask*)",
    "Bash(systemctl daemon-reload*)",
    "Bash(rm -rf*)",
    "Bash(rm -fr*)",
    "Bash(dd *)",
    "Bash(mkfs*)",
    "Bash(shutdown*)",
    "Bash(reboot*)",
    "Bash(poweroff*)",
    "Bash(chmod 777*)",
    "Bash(git push --force*)",
    "Bash(git push -f*)",
    "Bash(gh pr merge*)",
]

# ─── Inbox listener ─────────────────────────────────────────────────────

INBOX_POLL_INTERVAL = int(os.environ.get("PIPELINE_INBOX_POLL_INTERVAL", "60"))
INBOX_IMAP_HOST = os.environ.get("PIPELINE_IMAP_HOST", "")
INBOX_IMAP_USER = os.environ.get("PIPELINE_IMAP_USER", "")
INBOX_IMAP_PASS = os.environ.get("PIPELINE_IMAP_PASS", "")
INBOX_MAILGUN_WEBHOOK_KEY = os.environ.get("PIPELINE_MAILGUN_WEBHOOK_KEY", "")

# ─── WebSocket ───────────────────────────────────────────────────────────

WS_PING_INTERVAL = int(os.environ.get("PIPELINE_WS_PING_INTERVAL", "30"))

# ─── Logging ─────────────────────────────────────────────────────────────

LOG_LEVEL = os.environ.get("PIPELINE_LOG_LEVEL", "info")
