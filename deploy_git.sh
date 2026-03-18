#!/bin/bash
# deploy_git.sh - Git-based deployment for vinosomm.ai admin panel
# Usage: bash deploy_git.sh
# Run this on the VPS to pull latest code from GitHub and deploy

set -e

VINOSOMM="/opt/vinosomm"
REPO_URL="https://github.com/tiborstefanclever/vinosomm-transfer.git"
TEMP_DIR="/tmp/vinosomm-deploy-$(date +%s)"

echo "=== vinosomm.ai Git Deploy ==="
echo "Timestamp: $(date)"
echo ""

# 1. Clone/pull latest from GitHub
echo "[1] Fetching latest code from GitHub..."
if [ -d "/opt/vinosomm-transfer" ]; then
    cd /opt/vinosomm-transfer
    git pull origin main
    REPO_DIR="/opt/vinosomm-transfer"
else
    git clone "$REPO_URL" "$TEMP_DIR"
    REPO_DIR="$TEMP_DIR"
fi
echo "    Source: $REPO_DIR"

# 2. Backup existing files
echo "[2] Backing up current files..."
if [ -f "$VINOSOMM/admin/index.html" ]; then
    cp "$VINOSOMM/admin/index.html" "$VINOSOMM/admin/index.html.bak.$(date +%Y%m%d-%H%M%S)"
    echo "    Backed up admin/index.html"
fi
if [ -f "$VINOSOMM/app/admin_api.py" ]; then
    cp "$VINOSOMM/app/admin_api.py" "$VINOSOMM/app/admin_api.py.bak.$(date +%Y%m%d-%H%M%S)"
    echo "    Backed up app/admin_api.py"
fi

# 3. Deploy files
echo "[3] Deploying files..."
cp "$REPO_DIR/index.html" "$VINOSOMM/admin/index.html"
echo "    Deployed admin/index.html ($(wc -c < "$VINOSOMM/admin/index.html") bytes)"

if [ -f "$REPO_DIR/admin_api.py" ]; then
    cp "$REPO_DIR/admin_api.py" "$VINOSOMM/app/admin_api.py"
    echo "    Deployed app/admin_api.py ($(wc -c < "$VINOSOMM/app/admin_api.py") bytes)"
fi

# 4. Restart services
echo "[4] Restarting services..."
cd "$VINOSOMM"
docker compose restart caddy 2>/dev/null || echo "    Warning: caddy restart failed"
docker compose restart api 2>/dev/null || echo "    Warning: api restart skipped"

# 5. Verify
echo "[5] Verifying..."
sleep 2
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost/admin/ 2>/dev/null || echo "000")
echo "    Admin panel HTTP: $HTTP_CODE"

API_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost/admin/api/auth/login -X POST -H "Content-Type: application/json" -d '{"username":"test","password":"test"}' 2>/dev/null || echo "000")
echo "    API endpoint HTTP: $API_CODE"

# Cleanup temp dir
if [ -d "$TEMP_DIR" ]; then
    rm -rf "$TEMP_DIR"
fi

echo ""
echo "=== DEPLOY COMPLETE ==="
echo "Admin panel: http://72.62.63.125/admin/"
echo "Git commit: $(cd "$REPO_DIR" 2>/dev/null && git rev-parse --short HEAD 2>/dev/null || echo 'unknown')"
