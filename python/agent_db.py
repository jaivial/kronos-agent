"""SQLite data layer for Kronos Agent Pipeline.

Self-contained SQLite database for task flows, pipeline steps, artifacts,
and event logging. Independent of Kronos brain's DB — this is the
pipeline's own store.
"""
from __future__ import annotations

import contextlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.environ.get("PIPELINE_DB", ROOT / "data" / "pipeline.db"))

SCHEMA = """
-- A task flow is one submitted task going through the full pipeline.
CREATE TABLE IF NOT EXISTS task_flows (
    id                TEXT PRIMARY KEY,           -- UUID
    source            TEXT NOT NULL DEFAULT 'api', -- api|inbox|kanban|cron
    project_path      TEXT NOT NULL,              -- dynamic: set at submit, e.g. /var/www/kraken
    qdrant_collection TEXT,                       -- Qdrant collection for the project (NULL = no index)
    original_prompt   TEXT NOT NULL,
    enhanced_prompt   TEXT,                       -- after prompt-enhancer
    status            TEXT NOT NULL DEFAULT 'received',
                      -- received|researching|enhancing|planning|executing|
                      -- validating_1|validating_2|done|failed
    current_step      TEXT NOT NULL DEFAULT 'wide_research',
    retry_count       INTEGER NOT NULL DEFAULT 0,
    priority          TEXT NOT NULL DEFAULT 'normal',  -- critical|high|normal|low
    target_repo       TEXT,                       -- Backend|Dashboard|both (set by research)
    created_at        TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at        TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at      TEXT,
    error_reason      TEXT,
    metadata_json     TEXT,                       -- arbitrary extras
    react_flow_json   TEXT                        -- React Flow graph (nodes + edges)
);

CREATE INDEX IF NOT EXISTS task_flows_status_idx ON task_flows(status);
CREATE INDEX IF NOT EXISTS task_flows_created_idx ON task_flows(created_at);

-- Each pipeline step gets a row. Artifacts stored on disk, referenced here.
CREATE TABLE IF NOT EXISTS task_steps (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    task_flow_id      TEXT NOT NULL REFERENCES task_flows(id),
    step_name         TEXT NOT NULL,
                      -- wide_research|prompt_enhancer|planner|executor|validator_1|validator_2
    status            TEXT NOT NULL DEFAULT 'pending',
                      -- pending|running|done|failed|skipped
    started_at        TEXT,
    completed_at      TEXT,
    duration_seconds  REAL,
    agent_pid         INTEGER,
    claude_session_id TEXT,
    turns_used        INTEGER DEFAULT 0,
    cost_usd          REAL DEFAULT 0,
    result_text       TEXT,                       -- truncated to 50KB
    artifact_path     TEXT,                       -- path to JSON/diagram on disk
    error_text        TEXT,
    created_at        TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS task_steps_flow_idx ON task_steps(task_flow_id, step_name);

-- Event log for WebSocket broadcaster (mirrors qabot kanban_events pattern).
CREATE TABLE IF NOT EXISTS pipeline_events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    task_flow_id  TEXT NOT NULL REFERENCES task_flows(id),
    kind          TEXT NOT NULL,
                  -- step_started|step_completed|step_failed|flow_done|flow_failed|qdrant_warning
    payload       TEXT NOT NULL,                  -- JSON
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS pipeline_events_flow_idx
    ON pipeline_events(task_flow_id, id DESC);

-- Custom skills that users upload for specific agents.
CREATE TABLE IF NOT EXISTS agent_skills (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,              -- human-readable slug
    description TEXT NOT NULL DEFAULT '',
    content     TEXT NOT NULL,              -- full skill text/markdown
    agent_name  TEXT NOT NULL,              -- which agent this applies to (or 'all')
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS agent_skills_name_agent_idx
    ON agent_skills(name, agent_name);

-- Subagents: user-defined specializations of a parent agent.
CREATE TABLE IF NOT EXISTS subagents (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,              -- unique slug
    parent_agent TEXT NOT NULL,             -- which agent this extends
    description TEXT NOT NULL DEFAULT '',
    system_prompt_override TEXT,            -- optional: override parent's role prompt
    config_json TEXT DEFAULT '{}',          -- extra config (timeout, max_turns, etc.)
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS subagents_name_idx ON subagents(name);

-- Link subagents to skills.
CREATE TABLE IF NOT EXISTS subagent_skills (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    subagent_id INTEGER NOT NULL REFERENCES subagents(id) ON DELETE CASCADE,
    skill_id    INTEGER NOT NULL REFERENCES agent_skills(id) ON DELETE CASCADE,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS subagent_skills_unique_idx
    ON subagent_skills(subagent_id, skill_id);

-- Outbound webhooks: external app registers to receive pipeline events.
CREATE TABLE IF NOT EXISTS outbound_webhooks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    url         TEXT NOT NULL,
    secret      TEXT,                       -- HMAC secret for signing payloads
    events      TEXT NOT NULL DEFAULT '[]', -- JSON array of event kinds to subscribe to
    is_active   INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@contextlib.contextmanager
def connect() -> Iterator[sqlite3.Connection]:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, isolation_level=None, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    try:
        yield conn
    finally:
        conn.close()


def init() -> None:
    """Bootstrap the schema. Idempotent."""
    with connect() as c:
        c.executescript(SCHEMA)
        # Migrate: add react_flow_json if column missing
        cols = [r[1] for r in c.execute("PRAGMA table_info(task_flows)").fetchall()]
        if "react_flow_json" not in cols:
            c.execute("ALTER TABLE task_flows ADD COLUMN react_flow_json TEXT")
        if "claude_bin" not in cols:
            c.execute("ALTER TABLE task_flows ADD COLUMN claude_bin TEXT")
        if "title" not in cols:
            c.execute("ALTER TABLE task_flows ADD COLUMN title TEXT")
        step_cols = [r[1] for r in c.execute("PRAGMA table_info(task_steps)").fetchall()]
        if "result_summary" not in step_cols:
            c.execute("ALTER TABLE task_steps ADD COLUMN result_summary TEXT")
        if "model_name" not in cols:
            c.execute("ALTER TABLE task_flows ADD COLUMN model_name TEXT")
        if "tried_models" not in cols:
            c.execute("ALTER TABLE task_flows ADD COLUMN tried_models TEXT DEFAULT '[]'")


# ─── Task Flows ───────────────────────────────────────────────────────────


def create_task_flow(
    *,
    prompt: str,
    project_path: str,
    source: str = "api",
    qdrant_collection: str | None = None,
    priority: str = "normal",
    metadata: dict | None = None,
    current_step: str = "wide_research",
    claude_bin: str | None = None,
    model_name: str | None = None,
) -> str:
    """Create a new task flow. Returns the flow UUID."""
    import uuid
    flow_id = uuid.uuid4().hex[:16]
    with connect() as c:
        c.execute(
            """
            INSERT INTO task_flows
              (id, source, project_path, qdrant_collection, original_prompt,
               priority, current_step, metadata_json, claude_bin, model_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (flow_id, source, project_path, qdrant_collection, prompt,
             priority, current_step,
             json.dumps(metadata or {}),
             claude_bin, model_name),
        )
        _emit_event(c, flow_id, "flow_created",
                    {"id": flow_id, "source": source,
                     "project_path": project_path})
    return flow_id


def get_flow(flow_id: str) -> dict[str, Any] | None:
    with connect() as c:
        row = c.execute(
            "SELECT * FROM task_flows WHERE id = ?", (flow_id,)
        ).fetchone()
        return dict(row) if row else None


def get_flow_state(flow_id: str) -> dict[str, Any] | None:
    """Full flow + all steps + latest events."""
    flow = get_flow(flow_id)
    if not flow:
        return None
    steps = list_steps(flow_id)
    events = recent_events_for_flow(flow_id, limit=20)
    flow["steps"] = steps
    flow["recent_events"] = events
    return flow


def update_flow_status(flow_id: str, status: str,
                       current_step: str | None = None) -> bool:
    with connect() as c:
        sets = ["status = ?", "updated_at = ?"]
        args: list[Any] = [status, now()]
        if current_step is not None:
            sets.append("current_step = ?")
            args.append(current_step)
        if status == "done":
            sets.append("completed_at = ?")
            args.append(now())
        args.append(flow_id)
        cur = c.execute(
            f"UPDATE task_flows SET {', '.join(sets)} WHERE id = ?", args
        )
        return cur.rowcount > 0


def update_flow_retry(flow_id: str, retry_count: int) -> bool:
    with connect() as c:
        cur = c.execute(
            "UPDATE task_flows SET retry_count = ?, updated_at = ? WHERE id = ?",
            (retry_count, now(), flow_id),
        )
        return cur.rowcount > 0


def fail_flow(flow_id: str, reason: str) -> bool:
    with connect() as c:
        cur = c.execute(
            """
            UPDATE task_flows
               SET status = 'failed',
                   error_reason = ?,
                   updated_at = ?,
                   completed_at = ?
             WHERE id = ?
            """,
            (reason[:2000], now(), now(), flow_id),
        )
        return cur.rowcount > 0


def write_react_flow(flow_id: str, graph: dict) -> bool:
    """Store React Flow JSON (nodes + edges) directly in SQLite."""
    with connect() as c:
        cur = c.execute(
            "UPDATE task_flows SET react_flow_json = ?, updated_at = ? WHERE id = ?",
            (json.dumps(graph, default=str, ensure_ascii=False), now(), flow_id),
        )
        return cur.rowcount > 0


def read_react_flow(flow_id: str) -> dict | None:
    """Read React Flow JSON for a flow. Returns parsed dict or None."""
    with connect() as c:
        row = c.execute(
            "SELECT react_flow_json FROM task_flows WHERE id = ?", (flow_id,)
        ).fetchone()
        if not row or not row["react_flow_json"]:
            return None
        try:
            return json.loads(row["react_flow_json"])
        except json.JSONDecodeError:
            return None


def update_flow_title(flow_id: str, title: str) -> bool:
    with connect() as c:
        cur = c.execute(
            "UPDATE task_flows SET title = ?, updated_at = ? WHERE id = ?",
            (title[:200], now(), flow_id),
        )
        return cur.rowcount > 0


def update_step_summary(step_id: int, summary: str) -> bool:
    with connect() as c:
        cur = c.execute(
            "UPDATE task_steps SET result_summary = ? WHERE id = ?",
            (summary[:500], step_id),
        )
        return cur.rowcount > 0


def get_tried_models(flow_id: str) -> list[str]:
    with connect() as c:
        row = c.execute(
            "SELECT tried_models FROM task_flows WHERE id = ?", (flow_id,)
        ).fetchone()
        if not row or not row["tried_models"]:
            return []
        try:
            return json.loads(row["tried_models"])
        except (json.JSONDecodeError, TypeError):
            return []


def add_tried_model(flow_id: str, model: str) -> bool:
    tried = get_tried_models(flow_id)
    if model not in tried:
        tried.append(model)
    with connect() as c:
        cur = c.execute(
            "UPDATE task_flows SET tried_models = ?, updated_at = ? WHERE id = ?",
            (json.dumps(tried), now(), flow_id),
        )
        return cur.rowcount > 0


def update_flow_model(flow_id: str, model: str) -> bool:
    with connect() as c:
        cur = c.execute(
            "UPDATE task_flows SET model_name = ?, updated_at = ? WHERE id = ?",
            (model, now(), flow_id),
        )
        return cur.rowcount > 0


def list_task_flows(
    *, status: str | None = None, limit: int = 20,
) -> list[dict[str, Any]]:
    where = ""
    args: list[Any] = []
    if status:
        where = "WHERE status = ?"
        args.append(status)
    args.append(limit)
    with connect() as c:
        rows = c.execute(
            f"""
            SELECT id, source, project_path, title, status, current_step,
                   retry_count, priority, created_at, updated_at, completed_at
              FROM task_flows {where}
             ORDER BY created_at DESC LIMIT ?
            """,
            args,
        ).fetchall()
        return [dict(r) for r in rows]


# ─── Task Steps ───────────────────────────────────────────────────────────


def start_step(flow_id: str, step_name: str, agent_pid: int) -> int:
    with connect() as c:
        cur = c.execute(
            """
            INSERT INTO task_steps
              (task_flow_id, step_name, status, started_at, agent_pid)
            VALUES (?, ?, 'running', ?, ?)
            """,
            (flow_id, step_name, now(), agent_pid),
        )
        step_id = int(cur.lastrowid)
        _emit_event(c, flow_id, "step_started",
                    {"step": step_name, "step_id": step_id})
        return step_id


def complete_step(
    step_id: int,
    *,
    result_text: str | None = None,
    artifact_path: str | None = None,
    session_id: str | None = None,
    turns: int = 0,
    cost: float = 0.0,
) -> bool:
    with connect() as c:
        row = c.execute(
            "SELECT task_flow_id, step_name, started_at FROM task_steps WHERE id = ?",
            (step_id,),
        ).fetchone()
        if not row:
            return False

        duration = None
        if row["started_at"]:
            try:
                started = datetime.fromisoformat(row["started_at"])
                duration = (datetime.now(timezone.utc) - started.replace(tzinfo=timezone.utc)).total_seconds()
            except Exception:
                pass

        # Truncate result_text to 50KB
        if result_text and len(result_text) > 50000:
            result_text = result_text[:50000] + "\n...[truncated]"

        c.execute(
            """
            UPDATE task_steps
               SET status = 'done',
                   completed_at = ?,
                   duration_seconds = ?,
                   claude_session_id = ?,
                   turns_used = ?,
                   cost_usd = ?,
                   result_text = ?,
                   artifact_path = ?
             WHERE id = ?
            """,
            (now(), duration, session_id, turns, cost,
             result_text, artifact_path, step_id),
        )
        _emit_event(c, row["task_flow_id"], "step_completed",
                    {"step": row["step_name"], "step_id": step_id,
                     "duration": duration, "turns": turns, "cost": cost})
        return True


def fail_step(step_id: int, error_text: str | None = None,
              turns: int = 0, cost: float = 0.0) -> bool:
    with connect() as c:
        row = c.execute(
            "SELECT task_flow_id, step_name, started_at FROM task_steps WHERE id = ?",
            (step_id,),
        ).fetchone()
        if not row:
            return False

        duration = None
        if row["started_at"]:
            try:
                started = datetime.fromisoformat(row["started_at"])
                duration = (datetime.now(timezone.utc) - started.replace(tzinfo=timezone.utc)).total_seconds()
            except Exception:
                pass

        c.execute(
            """
            UPDATE task_steps
               SET status = 'failed',
                   completed_at = ?,
                   duration_seconds = ?,
                   turns_used = ?,
                   cost_usd = ?,
                   error_text = ?
             WHERE id = ?
            """,
            (now(), duration, turns, cost,
             (error_text or "")[:5000], step_id),
        )
        _emit_event(c, row["task_flow_id"], "step_failed",
                    {"step": row["step_name"], "step_id": step_id,
                     "error": (error_text or "")[:300]})
        return True


def list_steps(flow_id: str) -> list[dict[str, Any]]:
    with connect() as c:
        rows = c.execute(
            """
            SELECT id, step_name, status, started_at, completed_at,
                   duration_seconds, turns_used, cost_usd, artifact_path,
                   error_text
              FROM task_steps
             WHERE task_flow_id = ?
             ORDER BY id
            """,
            (flow_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_step(flow_id: str, step_name: str) -> dict[str, Any] | None:
    with connect() as c:
        row = c.execute(
            """
            SELECT * FROM task_steps
             WHERE task_flow_id = ? AND step_name = ?
             ORDER BY id DESC LIMIT 1
            """,
            (flow_id, step_name),
        ).fetchone()
        return dict(row) if row else None


# ─── Artifacts ────────────────────────────────────────────────────────────


ARTIFACTS_DIR = ROOT / "data" / "artifacts"


def write_artifact(flow_id: str, key: str, data: dict | str) -> str:
    """Write an artifact to disk. Returns the file path."""
    import hashlib
    h = hashlib.sha1(f"{flow_id}/{key}".encode()).hexdigest()[:8]
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    if isinstance(data, dict):
        filename = f"{flow_id}_{key}_{h}.json"
        path = ARTIFACTS_DIR / filename
        path.write_text(json.dumps(data, indent=2, default=str, ensure_ascii=False),
                        encoding="utf-8")
    else:
        filename = f"{flow_id}_{key}_{h}.txt"
        path = ARTIFACTS_DIR / filename
        path.write_text(str(data), encoding="utf-8")

    return str(path)


def read_artifact(path: str) -> dict | str | None:
    """Read an artifact from disk."""
    p = Path(path)
    if not p.exists():
        return None
    text = p.read_text(encoding="utf-8")
    if p.suffix == ".json":
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    return text


# ─── Events ───────────────────────────────────────────────────────────────


def _emit_event(c: sqlite3.Connection, flow_id: str, kind: str,
                payload: dict) -> int:
    cur = c.execute(
        "INSERT INTO pipeline_events (task_flow_id, kind, payload) VALUES (?, ?, ?)",
        (flow_id, kind, json.dumps(payload, default=str)),
    )
    return int(cur.lastrowid)


def emit_event(flow_id: str, kind: str, payload: dict) -> int:
    with connect() as c:
        c.execute("BEGIN IMMEDIATE")
        try:
            evid = _emit_event(c, flow_id, kind, payload)
            c.execute("COMMIT")
            return evid
        except Exception:
            c.execute("ROLLBACK")
            raise


def recent_events(after_id: int = 0, limit: int = 100) -> list[dict[str, Any]]:
    with connect() as c:
        rows = c.execute(
            """
            SELECT id, task_flow_id, kind, payload, created_at
              FROM pipeline_events
             WHERE id > ?
             ORDER BY id ASC LIMIT ?
            """,
            (after_id, limit),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            try:
                d["payload"] = json.loads(d["payload"]) if d["payload"] else {}
            except json.JSONDecodeError:
                d["payload"] = {"_raw": d["payload"]}
            out.append(d)
        return out


def recent_events_for_flow(flow_id: str, limit: int = 20) -> list[dict[str, Any]]:
    with connect() as c:
        rows = c.execute(
            """
            SELECT id, kind, payload, created_at
              FROM pipeline_events
             WHERE task_flow_id = ?
             ORDER BY id DESC LIMIT ?
            """,
            (flow_id, limit),
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            try:
                d["payload"] = json.loads(d["payload"]) if d["payload"] else {}
            except json.JSONDecodeError:
                d["payload"] = {"_raw": d["payload"]}
            out.append(d)
        return out


def latest_event_id() -> int:
    with connect() as c:
        row = c.execute("SELECT MAX(id) AS mx FROM pipeline_events").fetchone()
        return int(row["mx"] or 0)


# ─── Orphan sweep (on service startup) ────────────────────────────────────


def sweep_orphans() -> int:
    """Mark steps still in 'running' as failed (service restarted mid-step)."""
    orphaned_flows: set[str] = set()
    with connect() as c:
        rows = c.execute(
            "SELECT id, task_flow_id, step_name FROM task_steps WHERE status = 'running'"
        ).fetchall()
        for r in rows:
            c.execute(
                "UPDATE task_steps SET status = 'failed', "
                "completed_at = ?, error_text = 'orphaned: service restart' WHERE id = ?",
                (now(), r["id"]),
            )
            _emit_event(c, r["task_flow_id"], "step_failed",
                        {"step": r["step_name"], "reason": "orphaned"})
            orphaned_flows.add(r["task_flow_id"])
        # Also mark parent flows that are stuck and have no running steps
        if orphaned_flows:
            for fid in orphaned_flows:
                remaining = c.execute(
                    "SELECT COUNT(*) FROM task_steps WHERE task_flow_id = ? AND status = 'running'",
                    (fid,),
                ).fetchone()[0]
                if remaining == 0:
                    # No more running steps — mark flow as failed
                    c.execute(
                        "UPDATE task_flows SET status = 'failed', "
                        "error_reason = 'orphaned steps', "
                        "updated_at = ?, completed_at = ? WHERE id = ? AND status NOT IN ('done','failed')",
                        (now(), now(), fid),
                    )
        return len(rows)


# ─── Summary stats ────────────────────────────────────────────────────────


def summary_stats() -> dict[str, Any]:
    with connect() as c:
        return {
            "total_flows": c.execute("SELECT COUNT(*) FROM task_flows").fetchone()[0],
            "active_flows": c.execute(
                "SELECT COUNT(*) FROM task_flows "
                "WHERE status NOT IN ('done','failed')"
            ).fetchone()[0],
            "done_flows": c.execute(
                "SELECT COUNT(*) FROM task_flows WHERE status = 'done'"
            ).fetchone()[0],
            "failed_flows": c.execute(
                "SELECT COUNT(*) FROM task_flows WHERE status = 'failed'"
            ).fetchone()[0],
            "total_steps": c.execute("SELECT COUNT(*) FROM task_steps").fetchone()[0],
            "total_skills": c.execute("SELECT COUNT(*) FROM agent_skills").fetchone()[0],
            "total_subagents": c.execute("SELECT COUNT(*) FROM subagents").fetchone()[0],
            "total_webhooks": c.execute("SELECT COUNT(*) FROM outbound_webhooks WHERE is_active = 1").fetchone()[0],
            "latest_event_id": latest_event_id(),
        }


# ─── Agent Skills ─────────────────────────────────────────────────────────


def create_skill(*, name: str, description: str, content: str,
                 agent_name: str) -> int:
    with connect() as c:
        cur = c.execute(
            """INSERT INTO agent_skills (name, description, content, agent_name)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(name, agent_name) DO UPDATE SET
                 description = excluded.description,
                 content = excluded.content,
                 updated_at = datetime('now')""",
            (name, description, content, agent_name),
        )
        return int(cur.lastrowid)


def get_skill(skill_id: int) -> dict[str, Any] | None:
    with connect() as c:
        row = c.execute("SELECT * FROM agent_skills WHERE id = ?", (skill_id,)).fetchone()
        return dict(row) if row else None


def list_skills(agent_name: str | None = None) -> list[dict[str, Any]]:
    with connect() as c:
        if agent_name:
            rows = c.execute(
                "SELECT * FROM agent_skills WHERE agent_name = ? OR agent_name = 'all' ORDER BY id",
                (agent_name,),
            ).fetchall()
        else:
            rows = c.execute("SELECT * FROM agent_skills ORDER BY id").fetchall()
        return [dict(r) for r in rows]


def delete_skill(skill_id: int) -> bool:
    with connect() as c:
        cur = c.execute("DELETE FROM agent_skills WHERE id = ?", (skill_id,))
        return cur.rowcount > 0


# ─── Subagents ────────────────────────────────────────────────────────────


def create_subagent(*, name: str, parent_agent: str, description: str = "",
                    system_prompt_override: str | None = None,
                    config: dict | None = None) -> int:
    with connect() as c:
        cur = c.execute(
            """INSERT INTO subagents (name, parent_agent, description,
                   system_prompt_override, config_json)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(name) DO UPDATE SET
                 parent_agent = excluded.parent_agent,
                 description = excluded.description,
                 system_prompt_override = excluded.system_prompt_override,
                 config_json = excluded.config_json,
                 updated_at = datetime('now')""",
            (name, parent_agent, description,
             system_prompt_override, json.dumps(config or {})),
        )
        return int(cur.lastrowid)


def get_subagent(subagent_id: int) -> dict[str, Any] | None:
    with connect() as c:
        row = c.execute("SELECT * FROM subagents WHERE id = ?", (subagent_id,)).fetchone()
        if not row:
            return None
        d = dict(row)
        d["config"] = json.loads(d.get("config_json") or "{}")
        # Load linked skills
        skills = c.execute(
            """SELECT s.* FROM agent_skills s
               JOIN subagent_skills ss ON ss.skill_id = s.id
               WHERE ss.subagent_id = ?""",
            (subagent_id,),
        ).fetchall()
        d["skills"] = [dict(s) for s in skills]
        return d


def list_subagents(parent_agent: str | None = None) -> list[dict[str, Any]]:
    with connect() as c:
        if parent_agent:
            rows = c.execute(
                "SELECT * FROM subagents WHERE parent_agent = ? ORDER BY id",
                (parent_agent,),
            ).fetchall()
        else:
            rows = c.execute("SELECT * FROM subagents ORDER BY id").fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["config"] = json.loads(d.get("config_json") or "{}")
            out.append(d)
        return out


def delete_subagent(subagent_id: int) -> bool:
    with connect() as c:
        cur = c.execute("DELETE FROM subagents WHERE id = ?", (subagent_id,))
        return cur.rowcount > 0


def link_skill_to_subagent(subagent_id: int, skill_id: int) -> bool:
    with connect() as c:
        try:
            c.execute(
                "INSERT OR IGNORE INTO subagent_skills (subagent_id, skill_id) VALUES (?, ?)",
                (subagent_id, skill_id),
            )
            return True
        except Exception:
            return False


def unlink_skill_from_subagent(subagent_id: int, skill_id: int) -> bool:
    with connect() as c:
        cur = c.execute(
            "DELETE FROM subagent_skills WHERE subagent_id = ? AND skill_id = ?",
            (subagent_id, skill_id),
        )
        return cur.rowcount > 0


# ─── Outbound Webhooks ───────────────────────────────────────────────────


def create_webhook(*, url: str, secret: str | None = None,
                   events: list[str] | None = None) -> int:
    with connect() as c:
        cur = c.execute(
            "INSERT INTO outbound_webhooks (url, secret, events) VALUES (?, ?, ?)",
            (url, secret, json.dumps(events or [])),
        )
        return int(cur.lastrowid)


def list_webhooks(active_only: bool = True) -> list[dict[str, Any]]:
    with connect() as c:
        q = "SELECT * FROM outbound_webhooks"
        if active_only:
            q += " WHERE is_active = 1"
        rows = c.execute(q + " ORDER BY id").fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["events"] = json.loads(d.get("events") or "[]")
            out.append(d)
        return out


def delete_webhook(webhook_id: int) -> bool:
    with connect() as c:
        cur = c.execute("DELETE FROM outbound_webhooks WHERE id = ?", (webhook_id,))
        return cur.rowcount > 0


def get_webhooks_for_event(event_kind: str) -> list[dict[str, Any]]:
    """Get active webhooks subscribed to a specific event kind."""
    all_hooks = list_webhooks(active_only=True)
    return [
        h for h in all_hooks
        if not h["events"] or event_kind in h["events"]
    ]
