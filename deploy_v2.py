#!/usr/bin/env python3
"""Deploy updated admin panel v2 - fixes Celery integration, adds all missing pages."""
import os, sys, shutil, hashlib

VINOSOMM = "/opt/vinosomm"

def backup_and_write(src_content, dest_path):
    """Backup existing file and write new content."""
    if os.path.exists(dest_path):
        bak = dest_path + ".bak"
        shutil.copy2(dest_path, bak)
        print(f"  Backed up {dest_path} -> {bak}")
    with open(dest_path, "w") as f:
        f.write(src_content)
    print(f"  Written {dest_path} ({len(src_content)} bytes)")

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Read source files from same directory as this script
    admin_api_path = os.path.join(script_dir, "admin_api.py")
    index_html_path = os.path.join(script_dir, "index.html")

    if not os.path.exists(admin_api_path):
        print("ERROR: admin_api.py not found in script directory")
        sys.exit(1)
    if not os.path.exists(index_html_path):
        print("ERROR: index.html not found in script directory")
        sys.exit(1)

    with open(admin_api_path) as f:
        admin_api = f.read()
    with open(index_html_path) as f:
        index_html = f.read()

    print(f"Source admin_api.py: {len(admin_api)} bytes")
    print(f"Source index.html: {len(index_html)} bytes")

    # 1. Deploy admin_api.py
    print("\n[1] Deploying admin_api.py...")
    backup_and_write(admin_api, os.path.join(VINOSOMM, "app", "admin_api.py"))

    # 2. Deploy index.html
    print("\n[2] Deploying index.html...")
    backup_and_write(index_html, os.path.join(VINOSOMM, "admin", "index.html"))

    # 3. Add needs_review and review_notes columns if missing
    print("\n[3] Adding new DB columns via migration...")
    migrate_sql = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='wines' AND column_name='needs_review') THEN
        ALTER TABLE wines ADD COLUMN needs_review BOOLEAN DEFAULT FALSE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='wines' AND column_name='review_notes') THEN
        ALTER TABLE wines ADD COLUMN review_notes TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='tasks' AND column_name='celery_task_id') THEN
        ALTER TABLE tasks ADD COLUMN celery_task_id VARCHAR(200);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS admin_settings (
    id SERIAL PRIMARY KEY,
    key VARCHAR(100) UNIQUE NOT NULL,
    value TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);
"""
    migrate_path = os.path.join(VINOSOMM, "migrate_v2.sql")
    with open(migrate_path, "w") as f:
        f.write(migrate_sql)
    print(f"  Written migration SQL to {migrate_path}")

    # Run migration via docker compose exec
    import subprocess
    print("  Running migration...")
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "vinosomm", "-d", "vinosomm", "-f", "/dev/stdin"],
        input=migrate_sql, capture_output=True, text=True, cwd=VINOSOMM
    )
    if result.returncode == 0:
        print("  Migration OK")
    else:
        print(f"  Migration output: {result.stdout}")
        print(f"  Migration errors: {result.stderr}")
        # Try alternative: use docker compose exec with echo
        print("  Trying alternative migration method...")
        result2 = subprocess.run(
            ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "vinosomm", "-d", "vinosomm"],
            input=migrate_sql, capture_output=True, text=True, cwd=VINOSOMM
        )
        print(f"  Alt result: {result2.stdout[:200]}")

    # 4. Rebuild containers
    print("\n[4] Rebuilding API container...")
    result = subprocess.run(
        ["docker", "compose", "up", "-d", "--build", "api", "celery-worker", "celery-beat"],
        capture_output=True, text=True, cwd=VINOSOMM
    )
    print(result.stdout[-500:] if result.stdout else "")
    print(result.stderr[-500:] if result.stderr else "")

    # 5. Restart caddy to pick up new static files
    print("\n[5] Restarting Caddy...")
    subprocess.run(["docker", "compose", "restart", "caddy"], cwd=VINOSOMM)

    # 6. Wait and check
    import time
    time.sleep(5)
    print("\n[6] Checking container status...")
    result = subprocess.run(["docker", "compose", "ps"], capture_output=True, text=True, cwd=VINOSOMM)
    print(result.stdout)

    print("\n=== DEPLOYMENT COMPLETE ===")
    print("Admin panel: http://72.62.63.125/admin/")
    print("Login: admin / vinosomm2026")

if __name__ == "__main__":
    main()
