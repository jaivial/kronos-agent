"""Kronos Agent Pipeline SDK — FastAPI service.

The router. Receives tasks, walks them through the pipeline graph by spawning
fresh `claude -p` agents, and broadcasts events via WebSocket.

Pipeline graph:
  wide_research → prompt_enhancer → planner → executor → validator_1 → validator_2 → done

Each step:
  1. SDK updates DB status
  2. SDK spawns agent via agent_spawner.spawn_agent() in a background thread
  3. Agent calls agent_step_complete MCP tool → POSTs back to /internal/step-complete
  4. SDK reads pipeline graph → spawns next agent (or retries/fails)
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
import time
import urllib.request
import uuid
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
import agent_db
import agent_spawner
from pipeline_config import (
    SDK_HOST, SDK_PORT, MAX_RETRIES,
    PIPELINE_STEPS, STEP_STATUS_MAP, AGENT_DEFAULTS,
)
import inbox_listener

PORT = SDK_PORT

# ─── FastAPI app ─────────────────────────────────────────────────────────

app = FastAPI(title="Kronos Pipeline SDK", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# WebSocket connections: set of active ws
_ws_clients: set[WebSocket] = set()
_ws_lock = threading.Lock()


# ─── WebSocket broadcaster ───────────────────────────────────────────────


async def _broadcast(event: dict) -> None:
    """Push event to all connected WebSocket clients."""
    msg = json.dumps(event, default=str)
    dead: list[WebSocket] = []
    with _ws_lock:
        for ws in _ws_clients:
            try:
                await ws.send_text(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            _ws_clients.discard(ws)


def _broadcast_sync(event: dict) -> None:
    """Thread-safe sync wrapper for broadcast + webhook dispatch."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.run_coroutine_threadsafe(_broadcast(event), loop)
    except RuntimeError:
        pass
    # Fire outbound webhooks in background
    event_kind = event.get("type", "")
    if event_kind:
        t = threading.Thread(
            target=_fire_webhooks, args=(event_kind, event),
            name=f"webhook-{event_kind}", daemon=True,
        )
        t.start()


# ─── Pipeline graph logic ────────────────────────────────────────────────


def _next_step(current: str, result: str) -> str | None:
    """Determine next step from pipeline graph.

    Returns step name, or None if pipeline is done.
    On validator fail → retry executor (up to MAX_RETRIES).
    """
    if result in ("fail", "failed"):
        # Validators can retry executor
        if current in ("validator_1", "validator_2"):
            return "executor"
        # Other steps just fail
        return None

    # Success path: advance to next step
    try:
        idx = PIPELINE_STEPS.index(current)
    except ValueError:
        return None
    if idx + 1 >= len(PIPELINE_STEPS):
        return None  # pipeline complete
    return PIPELINE_STEPS[idx + 1]


# ─── Agent spawn runner (background thread) ──────────────────────────────


def _run_agent(flow_id: str, step_name: str) -> None:
    """Spawn an agent in a background thread. Handles success/failure/retry."""
    try:
        _run_agent_inner(flow_id, step_name)
    except Exception as exc:
        import traceback
        print(f"[pipeline] _run_agent CRASH: {exc}", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        # Mark step as failed so the flow doesn't hang
        try:
            steps = agent_db.list_steps(flow_id)
            active = [s for s in steps if s["step_name"] == step_name and s["status"] == "running"]
            if active:
                agent_db.fail_step(active[0]["id"], error_text=f"_run_agent crash: {exc}")
        except Exception:
            pass


def _run_agent_inner(flow_id: str, step_name: str) -> None:
    """Spawn an agent in a background thread. Handles success/failure/retry."""
    flow = agent_db.get_flow(flow_id)
    if not flow:
        return

    project_path = flow["project_path"]
    claude_bin = flow.get("claude_bin")  # per-flow binary override
    print(f"[pipeline] Spawning {step_name} for flow {flow_id[:8]} project={project_path} bin={claude_bin or 'default'}", flush=True)

    # Build MCP config for this agent
    mcp_config = agent_spawner.build_mcp_config(step_name, flow_id, project_path)
    user_prompt = agent_spawner.build_user_prompt(flow_id, step_name, project_path)
    print(f"[pipeline] MCP config: {mcp_config} exists={mcp_config.exists()}", flush=True)

    # Check if this is a retry — build override context
    prompt_override = None
    retry_count = flow.get("retry_count", 0)
    if retry_count > 0:
        steps = agent_db.list_steps(flow_id)
        prev = [s for s in steps if s["step_name"] == step_name and s["status"] == "failed"]
        if prev:
            last_fail = prev[-1]
            prompt_override = (
                f"RETRY #{retry_count} for step '{step_name}'.\n"
                f"Previous attempt failed: {last_fail.get('error_text', 'unknown')[:2000]}\n"
                f"Review the previous attempt's output and try a different approach.\n"
            )

    config = agent_spawner.AgentSpawnConfig(
        agent_name=step_name,
        task_flow_id=flow_id,
        project_path=project_path,
        mcp_config_path=mcp_config,
        user_prompt=user_prompt,
        prompt_override=prompt_override,
        timeout_seconds=600 if step_name != "validator_2" else 900,
        max_turns=40 if step_name != "wide_research" else 25,
        claude_bin=claude_bin,
    )

    # Update flow status
    status = STEP_STATUS_MAP.get(step_name, "running")
    agent_db.update_flow_status(flow_id, status, current_step=step_name)
    _broadcast_sync({
        "type": "step_starting",
        "flow_id": flow_id,
        "step": step_name,
        "status": status,
    })

    # Start step in DB
    step_id = agent_db.start_step(flow_id, step_name, os.getpid())

    # Spawn agent
    result = agent_spawner.spawn_agent(config)

    # Record result
    if result.ok:
        agent_db.complete_step(
            step_id,
            result_text=result.result_text,
            session_id=result.session_id,
            turns=result.num_turns,
            cost=result.total_cost_usd,
        )
        _broadcast_sync({
            "type": "step_completed",
            "flow_id": flow_id,
            "step": step_name,
            "turns": result.num_turns,
            "cost": result.total_cost_usd,
        })
    else:
        agent_db.fail_step(
            step_id,
            error_text=result.stderr_tail[:5000] if result.stderr_tail else "agent exited non-zero",
            turns=result.num_turns,
            cost=result.total_cost_usd,
        )
        _broadcast_sync({
            "type": "step_failed",
            "flow_id": flow_id,
            "step": step_name,
            "error": result.stderr_tail[:500] if result.stderr_tail else "unknown",
        })

    # Cleanup MCP config
    try:
        mcp_config.unlink(missing_ok=True)
    except Exception:
        pass


# ─── Pipeline advancement ────────────────────────────────────────────────


def _read_plan_phases(flow_id: str) -> list[dict] | None:
    """Read the plan artifact and return phases list, or None."""
    import glob
    pattern = str(agent_db.ARTIFACTS_DIR / f"{flow_id}_plan_*.json")
    matches = glob.glob(pattern)
    if not matches:
        return None
    data = agent_db.read_artifact(matches[-1])
    if isinstance(data, dict) and "phases" in data:
        return data["phases"]
    return None


def _run_executor_phases(flow_id: str, project_path: str) -> None:
    """Run each plan phase as a separate executor subprocess.

    Reads the plan artifact, iterates phases, spawns one executor per phase.
    After all phases complete, advances to validator_1.
    """
    flow = agent_db.get_flow(flow_id)
    claude_bin = (flow or {}).get("claude_bin")
    phases = _read_plan_phases(flow_id)
    if not phases:
        # No plan found — run single executor (legacy fallback)
        _run_agent(flow_id, "executor")
        return

    _broadcast_sync({
        "type": "multi_phase_execution",
        "flow_id": flow_id,
        "total_phases": len(phases),
    })

    for phase in phases:
        phase_num = phase.get("phase", 0)
        step_name = f"executor_phase_{phase_num}"

        mcp_config = agent_spawner.build_mcp_config("executor", flow_id, project_path)
        user_prompt = (
            f"PIPELINE TASK\n"
            f"  task_flow_id: {flow_id}\n"
            f"  your_step: executor (phase {phase_num} of {len(phases)})\n"
            f"  project_path: {project_path}\n"
            f"  phase_number: {phase_num}\n\n"
            f"Execute ONLY phase {phase_num}. "
            f"Description: {phase.get('description', '')}\n"
            f"Skill: {phase.get('skill', 'unknown')}\n"
            f"Files: {', '.join(phase.get('files', []))}\n"
            f"Acceptance: {'; '.join(phase.get('acceptance', []))}\n"
            f"Context from previous phases: {phase.get('context', 'first phase')}\n\n"
            f"Read the task flow via db_read_flow MCP tool for full details.\n"
            f"Call agent_step_complete when done.\n"
        )

        config = agent_spawner.AgentSpawnConfig(
            agent_name="executor",
            task_flow_id=flow_id,
            project_path=project_path,
            mcp_config_path=mcp_config,
            user_prompt=user_prompt,
            phase_number=phase_num,
            timeout_seconds=AGENT_DEFAULTS["executor"]["timeout_seconds"],
            max_turns=AGENT_DEFAULTS["executor"]["max_turns"],
            claude_bin=claude_bin,
        )

        agent_db.update_flow_status(flow_id, "executing", current_step=step_name)
        _broadcast_sync({
            "type": "step_starting",
            "flow_id": flow_id,
            "step": step_name,
            "phase": phase_num,
            "total_phases": len(phases),
        })

        step_id = agent_db.start_step(flow_id, step_name, os.getpid())
        result = agent_spawner.spawn_agent(config)

        if result.ok:
            agent_db.complete_step(
                step_id,
                result_text=result.result_text,
                session_id=result.session_id,
                turns=result.num_turns,
                cost=result.total_cost_usd,
            )
            _broadcast_sync({
                "type": "step_completed",
                "flow_id": flow_id,
                "step": step_name,
                "phase": phase_num,
                "turns": result.num_turns,
                "cost": result.total_cost_usd,
            })
        else:
            agent_db.fail_step(
                step_id,
                error_text=result.stderr_tail[:5000] if result.stderr_tail else "executor phase failed",
                turns=result.num_turns,
                cost=result.total_cost_usd,
            )
            _broadcast_sync({
                "type": "step_failed",
                "flow_id": flow_id,
                "step": step_name,
                "phase": phase_num,
                "error": result.stderr_tail[:500] if result.stderr_tail else "unknown",
            })
            # Phase failed — retry or fail the flow
            flow = agent_db.get_flow(flow_id)
            retry_count = (flow or {}).get("retry_count", 0)
            if retry_count < MAX_RETRIES:
                agent_db.update_flow_retry(flow_id, retry_count + 1)
                # Re-run this phase
                continue
            else:
                agent_db.fail_flow(flow_id, f"Phase {phase_num} failed after {MAX_RETRIES} retries")
                return

        try:
            mcp_config.unlink(missing_ok=True)
        except Exception:
            pass

    # All phases done — advance to validator_1
    _advance_pipeline(flow_id, "executor", "done")


def _advance_pipeline(flow_id: str, step_name: str, result: str,
                      artifact_keys: list[str] | None = None) -> None:
    """Called after a step completes. Decides next action."""
    flow = agent_db.get_flow(flow_id)
    if not flow:
        return

    # Link artifacts to latest step
    if artifact_keys:
        steps = agent_db.list_steps(flow_id)
        for s in reversed(steps):
            if s["step_name"] == step_name and s["status"] == "done":
                import glob
                for key in artifact_keys:
                    pattern = str(agent_db.ARTIFACTS_DIR / f"{flow_id}_{key}_*.json")
                    matches = glob.glob(pattern)
                    if matches:
                        agent_db.emit_event(flow_id, "artifact_linked", {
                            "step": step_name, "key": key, "path": matches[-1],
                        })
                break

    # Special: after planner completes, start multi-phase executor loop
    if step_name == "planner" and result in ("done", "pass"):
        project_path = flow["project_path"]
        t = threading.Thread(
            target=_run_executor_phases,
            args=(flow_id, project_path),
            name=f"executor-phases-{flow_id[:8]}",
            daemon=True,
        )
        t.start()
        return

    # Special: after executor completes (single-phase fallback), go to validator
    if step_name == "executor" and result in ("done", "pass"):
        next_step = "validator_1"
        t = threading.Thread(
            target=_run_agent,
            args=(flow_id, next_step),
            name=f"agent-{next_step}-{flow_id[:8]}",
            daemon=True,
        )
        t.start()
        return

    next_step = _next_step(step_name, result)

    if next_step is None:
        if result in ("fail", "failed"):
            retry_count = flow.get("retry_count", 0)
            if retry_count < MAX_RETRIES:
                agent_db.update_flow_retry(flow_id, retry_count + 1)
                next_step = "executor"
                _broadcast_sync({
                    "type": "flow_retrying",
                    "flow_id": flow_id,
                    "retry": retry_count + 1,
                    "restarting_from": "executor",
                })
            else:
                agent_db.fail_flow(flow_id, f"Max retries ({MAX_RETRIES}) exceeded at {step_name}")
                _broadcast_sync({
                    "type": "flow_failed",
                    "flow_id": flow_id,
                    "reason": f"Max retries exceeded at {step_name}",
                })
                return
        else:
            agent_db.update_flow_status(flow_id, "done")
            agent_db.emit_event(flow_id, "flow_done", {"steps_completed": len(PIPELINE_STEPS)})
            _broadcast_sync({
                "type": "flow_done",
                "flow_id": flow_id,
            })
            return

    t = threading.Thread(
        target=_run_agent,
        args=(flow_id, next_step),
        name=f"agent-{next_step}-{flow_id[:8]}",
        daemon=True,
    )
    t.start()


# ─── API models ──────────────────────────────────────────────────────────


class SubmitRequest(BaseModel):
    prompt: str
    project_path: str = "/var/www/kraken/Dashboard"
    source: str = "api"
    qdrant_collection: str | None = None
    priority: str = "normal"
    metadata: dict | None = None
    claude_bin: str | None = None  # "claude" | "claudio" — defaults to auto-detect


class StepCompleteRequest(BaseModel):
    flow_id: str
    agent_name: str
    result: str
    artifact_keys: list[str] = []


# ─── Endpoints ───────────────────────────────────────────────────────────


@app.post("/api/submit")
async def submit_task(req: SubmitRequest) -> dict:
    """Submit a new task to the pipeline."""
    flow_id = agent_db.create_task_flow(
        prompt=req.prompt,
        project_path=req.project_path,
        source=req.source,
        qdrant_collection=req.qdrant_collection,
        priority=req.priority,
        metadata=req.metadata,
        claude_bin=req.claude_bin,
    )
    # Start first agent in background
    t = threading.Thread(
        target=_run_agent,
        args=(flow_id, "wide_research"),
        name=f"agent-wide_research-{flow_id[:8]}",
        daemon=True,
    )
    t.start()

    _broadcast_sync({"type": "flow_created", "flow_id": flow_id})

    return {"flow_id": flow_id, "status": "received", "step": "wide_research"}


@app.post("/internal/step-complete")
async def step_complete(req: StepCompleteRequest) -> dict:
    """Called by agent_mcp when an agent finishes. Advances pipeline."""
    # Advance in a thread so we don't block the HTTP response
    t = threading.Thread(
        target=_advance_pipeline,
        args=(req.flow_id, req.agent_name, req.result, req.artifact_keys),
        name=f"advance-{req.agent_name}-{req.flow_id[:8]}",
        daemon=True,
    )
    t.start()
    return {"status": "ok", "advancing": True}


@app.post("/internal/heartbeat")
async def heartbeat(req: dict) -> dict:
    """Agent heartbeat. Could track liveness for timeout detection."""
    return {"status": "ok"}


@app.get("/api/flows")
async def list_flows(status: str | None = None, limit: int = 20) -> dict:
    flows = agent_db.list_task_flows(status=status, limit=limit)
    return {"flows": flows}


@app.get("/api/flows/{flow_id}")
async def get_flow(flow_id: str) -> dict:
    state = agent_db.get_flow_state(flow_id)
    if not state:
        return {"error": "not found"}
    return state


@app.get("/api/stats")
async def stats() -> dict:
    return agent_db.summary_stats()


@app.get("/api/events")
async def events(after_id: int = 0, limit: int = 100) -> dict:
    return {"events": agent_db.recent_events(after_id=after_id, limit=limit)}


@app.get("/api/flows/{flow_id}/react-flow")
async def get_react_flow(flow_id: str) -> dict:
    """Return React Flow graph for external rendering."""
    graph = agent_db.read_react_flow(flow_id)
    if graph is None:
        return {"nodes": [], "edges": []}
    return graph


@app.post("/api/retry/{flow_id}")
async def retry_flow(flow_id: str) -> dict:
    """Manually retry a failed flow from executor."""
    flow = agent_db.get_flow(flow_id)
    if not flow:
        return {"error": "not found"}
    if flow["status"] not in ("failed",):
        return {"error": f"cannot retry flow in status '{flow['status']}'"}

    retry_count = flow.get("retry_count", 0) + 1
    agent_db.update_flow_retry(flow_id, retry_count)
    agent_db.update_flow_status(flow_id, "received", current_step="executor")

    t = threading.Thread(
        target=_run_agent,
        args=(flow_id, "executor"),
        name=f"agent-executor-retry-{flow_id[:8]}",
        daemon=True,
    )
    t.start()

    return {"flow_id": flow_id, "retry": retry_count, "status": "executing"}


@app.delete("/api/flows/{flow_id}")
async def cancel_flow(flow_id: str) -> dict:
    """Cancel a running flow."""
    flow = agent_db.get_flow(flow_id)
    if not flow:
        return {"error": "not found"}
    agent_db.fail_flow(flow_id, "cancelled by user")
    _broadcast_sync({"type": "flow_cancelled", "flow_id": flow_id})
    return {"flow_id": flow_id, "status": "cancelled"}


# ─── Mailgun inbound webhook ────────────────────────────────────────────


@app.post("/api/inbox/mailgun")
async def mailgun_webhook(body: dict) -> dict:
    """Receive inbound email from Mailgun. Parses and submits task."""
    return inbox_listener.handle_mailgun_webhook(body)


# ─── Skills CRUD ─────────────────────────────────────────────────────────


class SkillCreateRequest(BaseModel):
    name: str
    description: str = ""
    content: str
    agent_name: str  # specific agent name or "all"


@app.get("/api/skills")
async def get_skills(agent_name: str | None = None) -> dict:
    return {"skills": agent_db.list_skills(agent_name=agent_name)}


@app.post("/api/skills")
async def create_skill(req: SkillCreateRequest) -> dict:
    skill_id = agent_db.create_skill(
        name=req.name, description=req.description,
        content=req.content, agent_name=req.agent_name,
    )
    return {"id": skill_id, "name": req.name, "agent_name": req.agent_name}


@app.get("/api/skills/{skill_id}")
async def get_skill(skill_id: int) -> dict:
    skill = agent_db.get_skill(skill_id)
    if not skill:
        return {"error": "not found"}
    return skill


@app.delete("/api/skills/{skill_id}")
async def delete_skill(skill_id: int) -> dict:
    ok = agent_db.delete_skill(skill_id)
    return {"deleted": ok}


# ─── Subagents CRUD ──────────────────────────────────────────────────────


class SubagentCreateRequest(BaseModel):
    name: str
    parent_agent: str
    description: str = ""
    system_prompt_override: str | None = None
    config: dict | None = None


class SubagentLinkRequest(BaseModel):
    skill_ids: list[int]


@app.get("/api/subagents")
async def get_subagents(parent_agent: str | None = None) -> dict:
    return {"subagents": agent_db.list_subagents(parent_agent=parent_agent)}


@app.post("/api/subagents")
async def create_subagent(req: SubagentCreateRequest) -> dict:
    sub_id = agent_db.create_subagent(
        name=req.name, parent_agent=req.parent_agent,
        description=req.description,
        system_prompt_override=req.system_prompt_override,
        config=req.config,
    )
    return {"id": sub_id, "name": req.name}


@app.get("/api/subagents/{subagent_id}")
async def get_subagent(subagent_id: int) -> dict:
    sa = agent_db.get_subagent(subagent_id)
    if not sa:
        return {"error": "not found"}
    return sa


@app.delete("/api/subagents/{subagent_id}")
async def delete_subagent(subagent_id: int) -> dict:
    ok = agent_db.delete_subagent(subagent_id)
    return {"deleted": ok}


@app.post("/api/subagents/{subagent_id}/skills")
async def link_skills(subagent_id: int, req: SubagentLinkRequest) -> dict:
    linked = 0
    for sid in req.skill_ids:
        if agent_db.link_skill_to_subagent(subagent_id, sid):
            linked += 1
    return {"linked": linked}


@app.delete("/api/subagents/{subagent_id}/skills/{skill_id}")
async def unlink_skill(subagent_id: int, skill_id: int) -> dict:
    ok = agent_db.unlink_skill_from_subagent(subagent_id, skill_id)
    return {"unlinked": ok}


# ─── Webhooks CRUD ───────────────────────────────────────────────────────


class WebhookCreateRequest(BaseModel):
    url: str
    secret: str | None = None
    events: list[str] = []  # empty = all events


@app.get("/api/webhooks")
async def get_webhooks() -> dict:
    return {"webhooks": agent_db.list_webhooks(active_only=False)}


@app.post("/api/webhooks")
async def create_webhook(req: WebhookCreateRequest) -> dict:
    wh_id = agent_db.create_webhook(
        url=req.url, secret=req.secret, events=req.events,
    )
    return {"id": wh_id, "url": req.url}


@app.delete("/api/webhooks/{webhook_id}")
async def delete_webhook(webhook_id: int) -> dict:
    ok = agent_db.delete_webhook(webhook_id)
    return {"deleted": ok}


# ─── Webhook dispatcher ──────────────────────────────────────────────────


def _fire_webhooks(event_kind: str, payload: dict) -> None:
    """Fire outbound webhooks for an event. Non-blocking."""
    hooks = agent_db.get_webhooks_for_event(event_kind)
    if not hooks:
        return
    import hashlib
    import hmac
    body = json.dumps({"event": event_kind, "payload": payload}, default=str)
    for h in hooks:
        headers = {"Content-Type": "application/json"}
        if h.get("secret"):
            sig = hmac.new(h["secret"].encode(), body.encode(), hashlib.sha256).hexdigest()
            headers["X-Kronos-Signature"] = f"sha256={sig}"
        try:
            req = urllib.request.Request(
                h["url"], data=body.encode(), headers=headers, method="POST",
            )
            urllib.request.urlopen(req, timeout=5)
        except Exception:
            pass  # fire and forget


# ─── Enhanced step detail ────────────────────────────────────────────────


@app.get("/api/flows/{flow_id}/steps/{step_id}")
async def get_step_detail(flow_id: str, step_id: int) -> dict:
    """Get a single step with its artifact data loaded."""
    with agent_db.connect() as c:
        row = c.execute(
            "SELECT * FROM task_steps WHERE id = ? AND task_flow_id = ?",
            (step_id, flow_id),
        ).fetchone()
        if not row:
            return {"error": "not found"}
        d = dict(row)
        if d.get("artifact_path"):
            d["artifact_data"] = agent_db.read_artifact(d["artifact_path"])
        return d


# ─── WebSocket ───────────────────────────────────────────────────────────


@app.websocket("/ws")
async def ws_events(ws: WebSocket) -> None:
    await ws.accept()
    with _ws_lock:
        _ws_clients.add(ws)
    last_id = agent_db.latest_event_id()
    try:
        while True:
            # Client can send ping or just hold connection
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_text("pong")
    except WebSocketDisconnect:
        pass
    finally:
        with _ws_lock:
            _ws_clients.discard(ws)


# ─── Startup / shutdown ──────────────────────────────────────────────────


@app.on_event("startup")
async def on_startup() -> None:
    agent_db.init()
    orphans = agent_db.sweep_orphans()
    if orphans:
        print(f"[pipeline] Swept {orphans} orphaned steps")
    inbox_listener.start_inbox_listener()


# ─── Main ────────────────────────────────────────────────────────────────


def main() -> None:
    import uvicorn
    print(f"[pipeline] Starting SDK on :{PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")


if __name__ == "__main__":
    main()
