#!/usr/bin/env bash
# Kronos Pipeline — one-shot deploy
# Usage: sudo bash deploy/go-live.sh
set -euo pipefail

ROOT="/var/www/kraken/Kronos-Pipeline"
cd "$ROOT"

echo "=== Kronos Pipeline Deploy ==="

# 1. Create dirs
mkdir -p data/agent-work data/artifacts logs config

# 2. Bootstrap DB
echo "[1/5] Bootstrapping database..."
python3 python/init_db.py

# 3. Check deps
echo "[2/5] Checking Python deps..."
python3 -c "import fastapi, uvicorn, pydantic" 2>/dev/null || {
    echo "Installing fastapi + uvicorn..."
    pip install fastapi uvicorn pydantic
}

# 4. Smoke test SDK (start, hit /api/stats, kill)
echo "[3/5] Smoke testing SDK..."
timeout 8 python3 python/agent_pipeline.py &
SDK_PID=$!
sleep 3
if curl -sf http://localhost:8199/api/stats > /dev/null; then
    echo "  SDK responds OK"
else
    echo "  FAIL: SDK not responding on :8199"
    kill $SDK_PID 2>/dev/null || true
    exit 1
fi
kill $SDK_PID 2>/dev/null || true
wait $SDK_PID 2>/dev/null || true

# 5. Install systemd unit
echo "[4/5] Installing systemd service..."
cp deploy/kronos-pipeline.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable kronos-pipeline

echo "[5/5] Starting service..."
systemctl restart kronos-pipeline
sleep 2
systemctl status kronos-pipeline --no-pager

echo ""
echo "=== Deploy complete ==="
echo "  Service:  systemctl status kronos-pipeline"
echo "  Logs:     journalctl -u kronos-pipeline -f"
echo "  API:      http://localhost:8199/api/stats"
echo "  Submit:   curl -X POST http://localhost:8199/api/submit -H 'Content-Type: application/json' -d '{\"prompt\":\"...\",\"project_path\":\"...\"}'"
