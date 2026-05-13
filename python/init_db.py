"""Idempotent DB bootstrap. Safe to re-run."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import agent_db


def main() -> int:
    agent_db.init()
    stats = agent_db.summary_stats()
    print(f"Pipeline DB ready: {agent_db.DB_PATH}")
    for k, v in stats.items():
        print(f"  {k:>22}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
