#!/usr/bin/env bash
# kronos-agent — postinstall setup
# Installs: systemd service, bootstraps DB, registers MCP globally
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# If running from node_modules, walk up to the real project root (where .git lives)
if echo "$ROOT" | grep -q "node_modules"; then
    REAL_ROOT="$(cd "$ROOT/../.." && pwd)"
    if [ -d "$REAL_ROOT/.git" ]; then
        ROOT="$REAL_ROOT"
    fi
fi

echo "=== kronos-agent installer ==="
echo "Root: $ROOT"

# ── 1. Bootstrap DB ──────────────────────────────────────────────────────
echo ""
echo "[1/5] Bootstrapping pipeline database..."
python3 "$ROOT/python/init_db.py"

# ── 2. Install Python deps ───────────────────────────────────────────────
echo ""
echo "[2/5] Checking Python dependencies..."
python3 -c "import fastapi, uvicorn, pydantic" 2>/dev/null || {
    echo "  Installing fastapi + uvicorn + pydantic..."
    pip install -q fastapi uvicorn pydantic
}
echo "  Python deps OK"

# ── 3. Install systemd service ────────────────────────────────────────────
echo ""
echo "[3/5] Installing systemd service..."
sed "s|/var/www/kraken/Kronos-Pipeline|$ROOT|g" "$ROOT/deploy/kronos-pipeline.service" \
    > /etc/systemd/system/kronos-pipeline.service
systemctl daemon-reload
systemctl enable kronos-pipeline 2>/dev/null || true
systemctl restart kronos-pipeline 2>/dev/null || true
sleep 2
if systemctl is-active --quiet kronos-pipeline; then
    echo "  kronos-pipeline.service: active"
else
    echo "  WARNING: service not active. Run: systemctl status kronos-pipeline"
fi

# ── 4. Register MCP in Claude Code ───────────────────────────────────────
echo ""
echo "[4/5] Registering MCP in ~/.claude/settings.json..."
MCP_ENTRY="{\"command\":\"python3\",\"args\":[\"$ROOT/python/kronos_pipeline_mcp.py\"],\"env\":{}}"

register_mcp() {
    local settings_file="$1"
    if [ ! -f "$settings_file" ]; then
        return
    fi
    # Use python to safely merge JSON
    python3 -c "
import json, sys
path = '$settings_file'
mcp_entry = $MCP_ENTRY
with open(path) as f:
    d = json.load(f)
if 'mcpServers' not in d:
    d['mcpServers'] = {}
d['mcpServers']['kronos-pipeline'] = mcp_entry
with open(path, 'w') as f:
    json.dump(d, f, indent=2)
print(f'  Registered in {path}')
"
}

register_mcp "$HOME/.claude/settings.json"
register_mcp "$HOME/.claudio/settings.json"

# ── 5. Smoke test ────────────────────────────────────────────────────────
echo ""
echo "[5/5] Smoke testing..."
sleep 1
if curl -sf http://localhost:8199/api/stats > /dev/null 2>&1; then
    echo "  SDK responding on http://localhost:8199"
else
    echo "  WARNING: SDK not responding on :8199"
fi

# MCP binary test
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' | \
    python3 "$ROOT/python/kronos_pipeline_mcp.py" 2>/dev/null | \
    python3 -c "import sys,json; d=json.loads(sys.stdin.readline()); print(f'  MCP: {d[\"result\"][\"serverInfo\"][\"name\"]} v{d[\"result\"][\"serverInfo\"][\"version\"]}')" 2>/dev/null || \
    echo "  WARNING: MCP binary test failed"

echo ""
echo "=== Install complete ==="
echo ""
echo "  Service:   systemctl status kronos-pipeline"
echo "  Logs:      journalctl -u kronos-pipeline -f"
echo "  API:       http://localhost:8199/api/stats"
echo "  Submit:    kronos_submit(prompt=\"your task\")"
echo "  Repo:      https://github.com/jaivial/kronos-agent"
echo ""
echo "  Restart your Claude session for MCP tools to appear."
