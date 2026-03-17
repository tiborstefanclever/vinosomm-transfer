#!/usr/bin/env python3
"""Dump all vinosomm configs for review. Read-only, no changes."""
import os, subprocess
V = "/opt/vinosomm"
def cat(p):
    if os.path.exists(p):
        with open(p) as f: c=f.read()
        print(f"\n===== {p} ({len(c)}b) =====")
        print(c)
        print(f"===== END {p} =====")
    else:
        print(f"\nMISSING: {p}")

# List all files
print("=== FILE TREE ===")
for root,dirs,files in os.walk(V):
    # skip heavy dirs
    if any(x in root for x in ['__pycache__','node_modules','.git','venv']):
        continue
    level = root.replace(V, '').count(os.sep)
    indent = ' ' * 2 * level
    print(f'{indent}{os.path.basename(root)}/')
    subindent = ' ' * 2 * (level + 1)
    for file in files:
        fp = os.path.join(root, file)
        sz = os.path.getsize(fp)
        print(f'{subindent}{file} ({sz}b)')

# Dump key files
cat(f"{V}/app/main.py")
cat(f"{V}/docker-compose.yml")
cat(f"{V}/.env")
cat(f"{V}/config/Caddyfile")
cat(f"{V}/Caddyfile")
cat(f"{V}/caddy/Caddyfile")
cat(f"{V}/config/caddy/Caddyfile")

# Check all .conf and config files
for root,dirs,files in os.walk(f"{V}/config"):
    for f in files:
        cat(os.path.join(root, f))

# Docker status
print("\n=== DOCKER STATUS ===")
r = subprocess.run("docker ps --format 'table {{.Names}}\\t{{.Status}}\\t{{.Ports}}'", shell=True, capture_output=True, text=True)
print(r.stdout)

# Check if admin files are there
print("\n=== ADMIN FILES CHECK ===")
for p in [f"{V}/app/admin_api.py", f"{V}/admin/index.html"]:
    exists = os.path.exists(p)
    sz = os.path.getsize(p) if exists else 0
    print(f"  {'OK' if exists else 'MISSING'}: {p} ({sz}b)")

print("\nDONE")
