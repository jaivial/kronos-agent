#!/usr/bin/env bash
# kronos-agent — uninstall
set -euo pipefail

echo "=== kronos-agent uninstaller ==="

# Stop service
if systemctl is-active --quiet kronos-pipeline 2>/dev/null; then
    systemctl stop kronos-pipeline
    echo "  Stopped kronos-pipeline.service"
fi
if [ -f /etc/systemd/system/kronos-pipeline.service ]; then
    rm /etc/systemd/system/kronos-pipeline.service
    systemctl daemon-reload
    echo "  Removed systemd unit"
fi

# Remove MCP from settings
remove_mcp() {
    local settings_file="$1"
    [ -f "$settings_file" ] || return
    python3 -c "
import json
path = '$settings_file'
with open(path) as f:
    d = json.load(f)
if 'mcpServers' in d and 'kronos-pipeline' in d['mcpServers']:
    del d['mcpServers']['kronos-pipeline']
    if not d['mcpServers']:
        del d['mcpServers']
    with open(path, 'w') as f:
        json.dump(d, f, indent=2)
    print(f'  Removed from {path}')
"
}

remove_mcp "$HOME/.claude/settings.json"
remove_mcp "$HOME/.claudio/settings.json"

echo ""
echo "=== Uninstall complete ==="
echo "  Data preserved in data/ — delete manually if needed."
