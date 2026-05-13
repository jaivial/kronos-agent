"""Agent MCP server — the notification channel.

Exposes `agent_step_complete` which POSTs to the SDK callback URL, triggering
the next pipeline step. Runs as a stdio JSON-RPC 2.0 server.

Env vars:
  KRONOS_TASK_FLOW_ID — current flow
  KRONOS_AGENT_NAME   — current agent role
  PIPELINE_SDK_URL    — SDK base URL (default http://localhost:8199)
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request
import urllib.error
from pathlib import Path

SDK_URL = os.environ.get("PIPELINE_SDK_URL", "http://localhost:8199")
FLOW_ID = os.environ.get("KRONOS_TASK_FLOW_ID", "")
AGENT_NAME = os.environ.get("KRONOS_AGENT_NAME", "unknown")

# ─── JSON-RPC helpers ────────────────────────────────────────────────────

def read_msg() -> dict | None:
    """Read one JSON-RPC message from stdin (line-delimited)."""
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


def error_resp(code: int, message: str, rid: any = None) -> dict:
    return {"jsonrpc": "2.0", "id": rid, "error": {"code": code, "message": message}}


def result_resp(result: any, rid: any) -> dict:
    return {"jsonrpc": "2.0", "id": rid, "result": result}


# ─── Tool definitions ────────────────────────────────────────────────────

TOOLS = [
    {
        "name": "agent_step_complete",
        "description": (
            "Signal that your step is done. Triggers the SDK to advance the "
            "pipeline. Call this when finished, then STOP."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "result": {
                    "type": "string",
                    "description": "Result summary: 'done', 'pass', 'fail', 'inconclusive'",
                },
                "artifact_keys": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Artifact keys written via db_write_artifact",
                },
            },
            "required": ["result"],
        },
    },
    {
        "name": "agent_heartbeat",
        "description": "Send a heartbeat to indicate this agent is still alive.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "message": {"type": "string"},
            },
        },
    },
]


# ─── Tool implementations ───────────────────────────────────────────────

def _step_complete(params: dict) -> dict:
    """POST to SDK /internal/step-complete."""
    payload = {
        "flow_id": FLOW_ID,
        "agent_name": AGENT_NAME,
        "result": params.get("result", "done"),
        "artifact_keys": params.get("artifact_keys", []),
    }
    url = f"{SDK_URL}/internal/step-complete"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8")
            return {"status": "ok", "http_code": resp.status, "response": body}
    except urllib.error.URLError as e:
        # SDK not running? Write fallback signal to disk so spawner can detect it.
        fallback = Path(os.environ.get("PIPELINE_DATA_DIR", "/var/www/kraken/Kronos-Pipeline/data"))
        fallback.mkdir(parents=True, exist_ok=True)
        flag = fallback / f"step-complete-{FLOW_ID}-{AGENT_NAME}.json"
        flag.write_text(json.dumps(payload, indent=2))
        return {
            "status": "fallback",
            "message": f"SDK unreachable ({e}), wrote fallback to {flag}",
        }


def _heartbeat(params: dict) -> dict:
    url = f"{SDK_URL}/internal/heartbeat"
    payload = {"flow_id": FLOW_ID, "agent_name": AGENT_NAME}
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return {"status": "ok"}
    except urllib.error.URLError:
        return {"status": "sdk_unreachable"}


TOOL_IMPLS = {
    "agent_step_complete": _step_complete,
    "agent_heartbeat": _heartbeat,
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
            "serverInfo": {"name": "kronos-agent-mcp", "version": "1.0.0"},
        }, rid)

    if method == "notifications/initialized":
        return None  # no response for notifications

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
            return result_resp({"content": [{"type": "text", "text": json.dumps(out)}]}, rid)
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
