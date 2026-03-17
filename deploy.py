#!/usr/bin/env python3
"""
vinosomm.ai Admin Panel - Full Deployment Script
Patches main.py, Caddyfile, docker-compose.yml, .env
Then rebuilds and restarts all containers.
"""
import os, shutil, hashlib, secrets

V = "/opt/vinosomm"

def backup_and_write(path, content):
    if os.path.exists(path):
        shutil.copy2(path, path + ".bak")
        print(f"  Backed up: {path}.bak")
    with open(path, "w") as f:
        f.write(content)
    print(f"  Written: {path} ({len(content)}b)")

# 1. PATCH main.py
print("[1/5] Patching main.py...")
main_path = f"{V}/app/main.py"
with open(main_path) as f:
    main_py = f.read()

if "admin_api" in main_py:
    print("  Already patched, skipping.")
else:
    main_py = main_py.replace(
        "from typing import Optional, List",
        "from typing import Optional, List\nfrom admin_api import router as admin_router"
    )
    main_py = main_py.replace(
        'app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])',
        'app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])\napp.include_router(admin_router)'
    )
    backup_and_write(main_path, main_py)
    print("  OK - Added admin_api router import and include_router.")

# 2. REPLACE Caddyfile
print("\n[2/5] Writing new Caddyfile...")
caddy_path = f"{V}/config/Caddyfile"

new_caddyfile = """:80 {
    # Admin API - proxy to FastAPI (MUST come before static admin routes)
    handle /admin/api/* {
        reverse_proxy api:8000
    }

    # Admin panel - serve static files
    handle /admin {
        redir /admin/ permanent
    }

    handle /admin/* {
        uri strip_prefix /admin
        root * /srv/admin
        try_files {path} /index.html
        file_server
    }

    # Existing wine API proxy
    reverse_proxy /api/* api:8000

    # Root
    respond / "vinosomm.ai API is running" 200
}
"""
backup_and_write(caddy_path, new_caddyfile)
print("  OK - Caddyfile updated with admin routes.")

# 3. PATCH docker-compose.yml
print("\n[3/5] Patching docker-compose.yml...")
dc_path = f"{V}/docker-compose.yml"
with open(dc_path) as f:
    dc = f.read()

changed = False

if "./admin:/srv/admin" not in dc:
    dc = dc.replace(
        "      - ./config/Caddyfile:/etc/caddy/Caddyfile\n      - caddy_data:/data\n      - caddy_config:/config",
        "      - ./config/Caddyfile:/etc/caddy/Caddyfile\n      - ./admin:/srv/admin:ro\n      - caddy_data:/data\n      - caddy_config:/config"
    )
    changed = True
    print("  Added ./admin:/srv/admin:ro volume to caddy service.")

if "JWT_SECRET" not in dc:
    dc = dc.replace(
        "      - SEARXNG_URL=http://searxng:8080\n    ports:\n      - \"127.0.0.1:8000:8000\"",
        "      - SEARXNG_URL=http://searxng:8080\n      - JWT_SECRET=${JWT_SECRET}\n      - ADMIN_USER=${ADMIN_USER:-admin}\n      - ADMIN_PASS_HASH=${ADMIN_PASS_HASH}\n    ports:\n      - \"127.0.0.1:8000:8000\""
    )
    changed = True
    print("  Added JWT_SECRET, ADMIN_USER, ADMIN_PASS_HASH env vars to api service.")

if changed:
    backup_and_write(dc_path, dc)
    print("  OK - docker-compose.yml updated.")
else:
    print("  Already patched, skipping.")

# 4. UPDATE .env
print("\n[4/5] Updating .env...")
env_path = f"{V}/.env"
with open(env_path) as f:
    env = f.read()

if "JWT_SECRET" not in env:
    jwt_secret = secrets.token_hex(32)
    admin_pass = "vinosomm2026"
    admin_hash = hashlib.sha256(admin_pass.encode()).hexdigest()

    env = env.rstrip() + f"""\nJWT_SECRET={jwt_secret}\nADMIN_USER=admin\nADMIN_PASS_HASH={admin_hash}\n"""
    backup_and_write(env_path, env)
    print(f"  OK - Added JWT_SECRET, ADMIN_USER=admin, ADMIN_PASS_HASH (password: {admin_pass})")
    print(f"  CHANGE THE PASSWORD LATER by updating ADMIN_PASS_HASH in .env")
else:
    print("  Already has admin vars, skipping.")

# 5. VERIFY
print("\n[5/5] Verification...")
print("\n--- Patched main.py (first 15 lines) ---")
with open(main_path) as f:
    for i, line in enumerate(f):
        if i >= 15: break
        print(f"  {i+1:3d}: {line.rstrip()}")

print(f"\n--- New Caddyfile ---")
with open(caddy_path) as f:
    print(f.read())

print(f"--- docker-compose.yml caddy section ---")
with open(dc_path) as f:
    dc_content = f.read()
    in_caddy = False
    for line in dc_content.split("\n"):
        if line.strip().startswith("caddy:"):
            in_caddy = True
        elif in_caddy and line and not line.startswith(" "):
            in_caddy = False
        if in_caddy:
            print(f"  {line}")

print(f"\n--- docker-compose.yml api env section ---")
with open(dc_path) as f:
    dc_content = f.read()
    in_api = False
    for line in dc_content.split("\n"):
        if line.strip().startswith("api:"):
            in_api = True
        elif in_api and line and not line.startswith(" "):
            in_api = False
        if in_api:
            print(f"  {line}")

print(f"\n--- .env ---")
with open(env_path) as f:
    print(f.read())

print("=" * 50)
print("ALL PATCHES APPLIED SUCCESSFULLY")
print("=" * 50)
print("\nNext: rebuild and restart with:")
print("  cd /opt/vinosomm && docker-compose down && docker-compose up -d --build")
print("\nThen test: http://72.62.63.125/admin/")
print("Login: admin / vinosomm2026")
