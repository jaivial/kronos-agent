#!/usr/bin/env bash
# kronos-agent — status check
set -euo pipefail

echo "=== kronos-agent status ==="

# Service
if systemctl is-active --quiet kronos-pipeline 2>/dev/null; then
    echo "  Service:  active"
else
    echo "  Service:  INACTIVE"
fi

# API
if curl -sf http://localhost:8199/api/stats > /tmp/kronos-stats.json 2>/dev/null; then
    echo "  API:      http://localhost:8199 (responding)"
    python3 -c "
import json
with open('/tmp/kronos-stats.json') as f:
    d = json.load(f)
print(f'  Flows:    {d[\"total_flows\"]} total, {d[\"active_flows\"]} active, {d[\"done_flows\"]} done')
print(f'  Steps:    {d[\"total_steps\"]}')
print(f'  Skills:   {d.get(\"total_skills\",0)}, Subagents: {d.get(\"total_subagents\",0)}, Webhooks: {d.get(\"total_webhooks\",0)}')
" 2>/dev/null
else
    echo "  API:      NOT responding"
fi

# MCP
if python3 -c "
import json, subprocess, sys
r = subprocess.run(['python3', '/var/www/kraken/Kronos-Pipeline/python/kronos_pipeline_mcp.py'],
    input='{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}',
    capture_output=True, text=True, timeout=5)
d = json.loads(r.stdout.strip())
print(f'  MCP:      {d[\"result\"][\"serverInfo\"][\"name\"]} v{d[\"result\"][\"serverInfo\"][\"version\"]}')
" 2>/dev/null; then
    true
else
    echo "  MCP:      ERROR"
fi

echo ""
