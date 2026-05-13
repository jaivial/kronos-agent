"""Kronos Pipeline MCP — submit and monitor tasks from any Claude session.

Install: add to your .mcp.json or .claude/settings.local.json:
  {
    "mcpServers": {
      "kronos-pipeline": {
        "command": "python3",
        "args": ["/var/www/kraken/Kronos-Pipeline/python/kronos_pipeline_mcp.py"],
        "env": {}
      }
    }
  }

Tools:
  kronos_submit     — submit a new task, pipeline starts immediately
  kronos_status     — get flow status + steps
  kronos_list       — list recent flows
  kronos_cancel     — cancel a running flow
  kronos_retry      — retry a failed flow
  kronos_react_flow — get the impact graph for a flow
  kronos_step_detail— get a step's result/artifact
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request
import urllib.error
from pathlib import Path

SDK_URL = os.environ.get("PIPELINE_SDK_URL", "http://localhost:8199")


# ─── HTTP helpers ────────────────────────────────────────────────────────


def _get(path: str) -> dict:
    try:
        with urllib.request.urlopen(f"{SDK_URL}{path}", timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.URLError as e:
        return {"error": f"SDK unreachable: {e}"}
    except json.JSONDecodeError:
        return {"error": "invalid JSON from SDK"}


def _post(path: str, data: dict) -> dict:
    body = json.dumps(data).encode()
    req = urllib.request.Request(
        f"{SDK_URL}{path}", data=body,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.URLError as e:
        return {"error": f"SDK unreachable: {e}"}


def _delete(path: str) -> dict:
    req = urllib.request.Request(f"{SDK_URL}{path}", method="DELETE")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.URLError as e:
        return {"error": f"SDK unreachable: {e}"}


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
        "name": "kronos_submit",
        "description": (
            "Submit a task to the Kronos multi-agent pipeline. "
            "The pipeline will run: wide_research → prompt_enhancer → planner → "
            "executor → validator_1 → validator_2. Returns a flow_id for tracking."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "prompt": {
                    "type": "string",
                    "description": "The task description — what you want done",
                },
                "project_path": {
                    "type": "string",
                    "description": "Absolute path to the target project",
                    "default": "/var/www/kraken/Dashboard",
                },
                "priority": {
                    "type": "string",
                    "enum": ["critical", "high", "normal", "low"],
                    "default": "normal",
                },
            },
            "required": ["prompt"],
        },
    },
    {
        "name": "kronos_status",
        "description": (
            "Get the full status of a task flow: current step, all steps with "
            "their statuses, events, and artifact data."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "flow_id": {"type": "string", "description": "Flow ID from kronos_submit"},
            },
            "required": ["flow_id"],
        },
    },
    {
        "name": "kronos_list",
        "description": "List recent task flows. Optional status filter.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "description": "Filter: received|researching|enhancing|planning|executing|validating_1|validating_2|done|failed",
                },
                "limit": {"type": "integer", "default": 10},
            },
        },
    },
    {
        "name": "kronos_cancel",
        "description": "Cancel a running task flow.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "flow_id": {"type": "string"},
            },
            "required": ["flow_id"],
        },
    },
    {
        "name": "kronos_retry",
        "description": "Retry a failed task flow from the executor step.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "flow_id": {"type": "string"},
            },
            "required": ["flow_id"],
        },
    },
    {
        "name": "kronos_react_flow",
        "description": "Get the React Flow impact graph for a flow (nodes + edges).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "flow_id": {"type": "string"},
            },
            "required": ["flow_id"],
        },
    },
    {
        "name": "kronos_step_detail",
        "description": "Get detailed result of a specific step including artifact data.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "flow_id": {"type": "string"},
                "step_id": {"type": "integer"},
            },
            "required": ["flow_id", "step_id"],
        },
    },
    {
        "name": "kronos_stats",
        "description": "Get pipeline summary stats.",
        "inputSchema": {"type": "object", "properties": {}},
    },
]


# ─── Tool implementations ───────────────────────────────────────────────


def _submit(args: dict) -> dict:
    prompt = args.get("prompt", "")
    if not prompt:
        return {"error": "prompt is required"}
    data = {
        "prompt": prompt,
        "project_path": args.get("project_path", "/var/www/kraken/Dashboard"),
        "priority": args.get("priority", "normal"),
    }
    res = _post("/api/submit", data)
    if "error" in res:
        return res
    return {
        "flow_id": res.get("flow_id"),
        "status": res.get("status"),
        "step": res.get("step"),
        "message": f"Task submitted. Track with kronos_status(flow_id=\"{res.get('flow_id')}\")",
    }


def _status(args: dict) -> dict:
    fid = args.get("flow_id", "")
    if not fid:
        return {"error": "flow_id is required"}
    return _get(f"/api/flows/{fid}")


def _list(args: dict) -> dict:
    status = args.get("status")
    limit = args.get("limit", 10)
    path = f"/api/flows?limit={limit}"
    if status:
        path += f"&status={status}"
    return _get(path)


def _cancel(args: dict) -> dict:
    fid = args.get("flow_id", "")
    if not fid:
        return {"error": "flow_id is required"}
    return _delete(f"/api/flows/{fid}")


def _retry(args: dict) -> dict:
    fid = args.get("flow_id", "")
    if not fid:
        return {"error": "flow_id is required"}
    return _post(f"/api/retry/{fid}", {})


def _react_flow(args: dict) -> dict:
    fid = args.get("flow_id", "")
    if not fid:
        return {"error": "flow_id is required"}
    return _get(f"/api/flows/{fid}/react-flow")


def _step_detail(args: dict) -> dict:
    fid = args.get("flow_id", "")
    sid = args.get("step_id")
    if not fid or sid is None:
        return {"error": "flow_id and step_id are required"}
    return _get(f"/api/flows/{fid}/steps/{sid}")


def _stats(args: dict) -> dict:
    return _get("/api/stats")


TOOL_IMPLS = {
    "kronos_submit": _submit,
    "kronos_status": _status,
    "kronos_list": _list,
    "kronos_cancel": _cancel,
    "kronos_retry": _retry,
    "kronos_react_flow": _react_flow,
    "kronos_step_detail": _step_detail,
    "kronos_stats": _stats,
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
            "serverInfo": {"name": "kronos-pipeline-mcp", "version": "1.0.0"},
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
            return result_resp({"content": [{"type": "text", "text": json.dumps(out, default=str, indent=2)}]}, rid)
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
