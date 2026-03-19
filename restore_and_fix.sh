#!/bin/bash
# Run on VPS: bash restore_and_fix.sh
# Restores admin_api.py from backup and applies wines API fixes

set -e
cd /opt/vinosomm

BAK="app/admin_api.py.bak"
LIVE="app/admin_api.py"

# Step 1: Restore from backup
if [ -f "$BAK" ] && [ $(wc -c < "$BAK") -gt 50000 ]; then
  echo "Restoring from backup ($(wc -c < $BAK) bytes)..."
  cp "$BAK" "$LIVE"
  echo "Restored."
else
  echo "ERROR: No valid backup found at $BAK"
  exit 1
fi

# Step 2: Apply fixes inline using sed/python
echo "Applying fixes..."
python3 -c "
code = open('$LIVE').read()
# Fix 1: Add Body import
code = code.replace(
    'from fastapi import APIRouter, Depends, HTTPException, status, Query, Header',
    'from fastapi import APIRouter, Depends, HTTPException, status, Query, Header, Body',
    1
)
# Fix 2: Add Body() to POST/PUT
for old, new in [
    ('async def create_wine(payload: Dict[str, Any], auth', 'async def create_wine(payload: Dict[str, Any] = Body(...), auth'),
    ('async def update_wine(wine_id: int, payload: Dict[str, Any], auth', 'async def update_wine(wine_id: int, payload: Dict[str, Any] = Body(...), auth'),
    ('async def create_vineyard(payload: Dict[str, Any], auth', 'async def create_vineyard(payload: Dict[str, Any] = Body(...), auth'),
    ('async def update_vineyard(vineyard_id: int, payload: Dict[str, Any], auth', 'async def update_vineyard(vineyard_id: int, payload: Dict[str, Any] = Body(...), auth'),
]:
    code = code.replace(old, new, 1)
open('$LIVE', 'w').write(code)
print(f'Patched {len(code)} bytes')
"

# Step 3: Add missing DB columns
echo "Running DB migration..."
docker compose exec -T postgres psql -U vinosomm -d vinosomm << 'SQL'
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='wines' AND column_name='needs_review') THEN
    ALTER TABLE wines ADD COLUMN needs_review BOOLEAN DEFAULT FALSE;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='wines' AND column_name='review_notes') THEN
    ALTER TABLE wines ADD COLUMN review_notes TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='wines' AND column_name='updated_at') THEN
    ALTER TABLE wines ADD COLUMN updated_at TIMESTAMP;
  END IF;
END $$;
SQL
echo "Migration done."

# Step 4: Rebuild API container
echo "Rebuilding API..."
docker compose up -d --build api
sleep 5

# Step 5: Verify
echo "Verifying..."
TOKEN=$(curl -s -X POST http://localhost/admin/api/auth/login -H "Content-Type: application/json" -d '{"username":"admin","password":"vinosomm2026"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
echo "Vineyards: $(curl -s -H "Authorization: Bearer $TOKEN" http://localhost/admin/api/vineyards | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'{d[\"total\"]} vineyards')")"
echo "Wines: $(curl -s -o /dev/null -w %{http_code} -H "Authorization: Bearer $TOKEN" http://localhost/admin/api/wines)"
echo "Dashboard: $(curl -s -o /dev/null -w %{http_code} -H "Authorization: Bearer $TOKEN" http://localhost/admin/api/dashboard)"
echo "=== DONE ==="
