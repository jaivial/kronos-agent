"""SQLite MCP server — data access channel for agents.

Exposes tools to read flow state, write artifacts, and run read-only queries
against the pipeline SQLite database. Stdio JSON-RPC 2.0 server.

Env vars:
  KRONOS_TASK_FLOW_ID — scope all reads to this flow
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
import agent_db

FLOW_ID = os.environ.get("KRONOS_TASK_FLOW_ID", "")


# ─── JSON-RPC helpers ────────────────────────────────────────────────────

def read_msg() -> dict | None:
    line = sys.stdin.readline()
    if not line:
        return None
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None


def write_msg(msg: dict) -> None:
    sys.stdout.write(json.dumps(msg) + "\n")
    sys.stdout.flush()


def result_resp(result: any, rid: any) -> dict:
    return {"jsonrpc": "2.0", "id": rid, "result": result}


def error_resp(code: int, message: str, rid: any = None) -> dict:
    return {"jsonrpc": "2.0", "id": rid, "error": {"code": code, "message": message}}


# ─── Tool definitions ────────────────────────────────────────────────────

TOOLS = [
    {
        "name": "db_read_flow",
        "description": (
            "Read the full task flow state: flow metadata, all steps, "
            "recent events, and any artifacts referenced by steps."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "include_artifacts": {
                    "type": "boolean",
                    "default": True,
                    "description": "Include artifact file contents in response",
                },
            },
        },
    },
    {
        "name": "db_write_artifact",
        "description": (
            "Write an artifact (JSON or text) to disk. Returns the file path. "
            "Use this to save research results, plans, execution summaries, etc."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "key": {
                    "type": "string",
                    "description": "Artifact key: research_context, enhanced_prompt, plan, execution_summary, door1_result, door2_result",
                },
                "data": {
                    "description": "Artifact content — JSON object or string",
                },
            },
            "required": ["key", "data"],
        },
    },
    {
        "name": "db_query",
        "description": (
            "Run a read-only SQL query against the pipeline DB. "
            "SELECT only. Useful for custom lookups."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "SQL query (SELECT only)",
                },
            },
            "required": ["sql"],
        },
    },
    {
        "name": "db_list_flows",
        "description": "List recent task flows. Optional status filter.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "description": "Filter by status: received, researching, enhancing, planning, executing, validating_1, validating_2, done, failed",
                },
                "limit": {
                    "type": "integer",
                    "default": 20,
                },
            },
        },
    },
    {
        "name": "db_write_react_flow",
        "description": (
            "Store a React Flow graph (nodes + edges) in the database. "
            "The JSON will be available via GET /api/flows/{id}/react-flow "
            "for external apps to render."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "nodes": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": "React Flow nodes array",
                },
                "edges": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": "React Flow edges array",
                },
            },
            "required": ["nodes"],
        },
    },
]


# ─── Tool implementations ───────────────────────────────────────────────

def _read_flow(params: dict) -> dict:
    flow_id = FLOW_ID
    if not flow_id:
        return {"error": "KRONOS_TASK_FLOW_ID env var not set"}
    state = agent_db.get_flow_state(flow_id)
    if not state:
        return {"error": f"Flow {flow_id} not found"}
    # Optionally load artifact contents
    if params.get("include_artifacts", True):
        for step in state.get("steps", []):
            ap = step.get("artifact_path")
            if ap:
                step["artifact_data"] = agent_db.read_artifact(ap)
    return state


def _write_artifact(params: dict) -> dict:
    flow_id = FLOW_ID
    if not flow_id:
        return {"error": "KRONOS_TASK_FLOW_ID env var not set"}
    key = params.get("key", "")
    data = params.get("data")
    if not key or data is None:
        return {"error": "key and data are required"}
    path = agent_db.write_artifact(flow_id, key, data)
    return {"path": path, "key": key}


def _query(params: dict) -> dict:
    sql = params.get("sql", "").strip()
    if not sql:
        return {"error": "sql is required"}
    # Enforce read-only
    upper = sql.upper().lstrip()
    if not upper.startswith("SELECT") and not upper.startswith("PRAGMA"):
        return {"error": "Only SELECT / PRAGMA queries allowed"}
    try:
        with agent_db.connect() as c:
            rows = c.execute(sql).fetchall()
            return {"rows": [dict(r) for r in rows]}
    except Exception as e:
        return {"error": str(e)}


def _list_flows(params: dict) -> dict:
    status = params.get("status")
    limit = params.get("limit", 20)
    flows = agent_db.list_task_flows(status=status, limit=limit)
    return {"flows": flows}


def _write_react_flow(params: dict) -> dict:
    flow_id = FLOW_ID
    if not flow_id:
        return {"error": "KRONOS_TASK_FLOW_ID env var not set"}
    graph = {
        "nodes": params.get("nodes", []),
        "edges": params.get("edges", []),
    }
    ok = agent_db.write_react_flow(flow_id, graph)
    return {"ok": ok, "flow_id": flow_id}


TOOL_IMPLS = {
    "db_read_flow": _read_flow,
    "db_write_artifact": _write_artifact,
    "db_query": _query,
    "db_list_flows": _list_flows,
    "db_write_react_flow": _write_react_flow,
}


# ─── JSON-RPC dispatch ──────────────────────────────────────────────────

def handle_request(msg: dict) -> dict | None:
    method = msg.get("method", "")
    rid = msg.get("id")
    params = msg.get("params", {})

    if method == "initialize":
        return result_resp({
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "kronos-sqlite-mcp", "version": "1.0.0"},
        }, rid)

    if method == "notifications/initialized":
        return None

    if method == "tools/list":
        return result_resp({"tools": TOOLS}, rid)

    if method == "tools/call":
        tool_name = params.get("name", "")
        tool_args = params.get("arguments", {})
        impl = TOOL_IMPLS.get(tool_name)
        if not impl:
            return error_resp(-32601, f"Unknown tool: {tool_name}", rid)
        try:
            out = impl(tool_args)
            return result_resp({"content": [{"type": "text", "text": json.dumps(out, default=str)}]}, rid)
        except Exception as e:
            return result_resp({
                "content": [{"type": "text", "text": json.dumps({"error": str(e)})}],
                "isError": True,
            }, rid)

    return error_resp(-32601, f"Unknown method: {method}", rid)


# ─── Main loop ───────────────────────────────────────────────────────────

def main() -> None:
    while True:
        msg = read_msg()
        if msg is None:
            break
        resp = handle_request(msg)
        if resp is not None:
            write_msg(resp)


if __name__ == "__main__":
    main()
