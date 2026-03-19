"""Apply fixes to admin_api.py on the VPS.
Run this script on the VPS: python3 apply_fixes.py
It reads admin_api.py, applies 4 fixes, and writes it back.
"""
import re

with open('/opt/vinosomm/app/admin_api.py', 'r') as f:
    code = f.read()

original_len = len(code)
changes = []

# Fix 1: Add Body to FastAPI import
old_import = 'from fastapi import APIRouter, Depends, HTTPException, status, Query, Header'
new_import = 'from fastapi import APIRouter, Depends, HTTPException, status, Query, Header, Body'
if 'Body' not in code.split('\n')[19] if len(code.split('\n')) > 19 else '':
    code = code.replace(old_import, new_import, 1)
    changes.append('Added Body to FastAPI import')

# Fix 2: Make wine_to_dict resilient with getattr
new_func = '''def wine_to_dict(wine: Wine) -> Dict[str, Any]:
    """Convert Wine ORM object to dict. Uses getattr for resilience against missing DB columns."""
    created = getattr(wine, 'created_at', None)
    updated = getattr(wine, 'updated_at', None)
    return {
        "id": wine.id, "name": wine.name, "producer": getattr(wine, 'producer', None),
        "vineyard_id": getattr(wine, 'vineyard_id', None), "vintage": getattr(wine, 'vintage', None),
        "region": getattr(wine, 'region', None), "country": getattr(wine, 'country', None),
        "grape_variety": getattr(wine, 'grape_variety', None), "wine_type": getattr(wine, 'wine_type', None),
        "classification": getattr(wine, 'classification', None),
        "sweetness": getattr(wine, 'sweetness', None), "acidity": getattr(wine, 'acidity', None),
        "tannin": getattr(wine, 'tannin', None), "body": getattr(wine, 'body', None),
        "alcohol_warmth": getattr(wine, 'alcohol_warmth', None), "effervescence": getattr(wine, 'effervescence', None),
        "flavor_intensity": getattr(wine, 'flavor_intensity', None), "finish": getattr(wine, 'finish', None),
        "complexity": getattr(wine, 'complexity', None),
        "fruit_character": getattr(wine, 'fruit_character', None),
        "secondary_aromas": getattr(wine, 'secondary_aromas', None),
        "tertiary_notes": getattr(wine, 'tertiary_notes', None),
        "residual_sugar": getattr(wine, 'residual_sugar', None), "alcohol": getattr(wine, 'alcohol', None),
        "price": getattr(wine, 'price', None), "currency": getattr(wine, 'currency', None), "rating": getattr(wine, 'rating', None),
        "data_method": getattr(wine, 'data_method', None), "source_list": getattr(wine, 'source_list', None),
        "source_count": getattr(wine, 'source_count', 0), "provenance_notes": getattr(wine, 'provenance_notes', None),
        "avg_confidence": getattr(wine, 'avg_confidence', None), "completeness": getattr(wine, 'completeness', None),
        "needs_review": getattr(wine, 'needs_review', False), "review_notes": getattr(wine, 'review_notes', None),
        "created_at": created.isoformat() if created else None,
        "updated_at": updated.isoformat() if updated else None,
    }'''

old_end = 'wine.updated_at.isoformat() if wine.updated_at else None,\n    }'
if old_end in code:
    start_idx = code.index('def wine_to_dict')
    end_idx = code.index(old_end) + len(old_end)
    code = code[:start_idx] + new_func + code[end_idx:]
    changes.append('Replaced wine_to_dict with getattr-safe version')

# Fix 3: Add Body() to POST/PUT endpoints
for old, new in [
    ('async def create_wine(payload: Dict[str, Any], authorization',
     'async def create_wine(payload: Dict[str, Any] = Body(...), authorization'),
    ('async def update_wine(wine_id: int, payload: Dict[str, Any], authorization',
     'async def update_wine(wine_id: int, payload: Dict[str, Any] = Body(...), authorization'),
    ('async def create_vineyard(payload: Dict[str, Any], authorization',
     'async def create_vineyard(payload: Dict[str, Any] = Body(...), authorization'),
    ('async def update_vineyard(vineyard_id: int, payload: Dict[str, Any], authorization',
     'async def update_vineyard(vineyard_id: int, payload: Dict[str, Any] = Body(...), authorization'),
]:
    if old in code:
        code = code.replace(old, new, 1)
        changes.append(f'Added Body() to {old.split("(")[0].split()[-1]}')

# Fix 4: Add startup migration
from sqlalchemy import text as sa_text
migration = '''
# Auto-migrate: add missing columns on startup
def _ensure_columns():
    """Add any columns that exist in the model but not in the DB table."""
    try:
        with engine.connect() as conn:
            for tbl, cols in [
                (\'wines\', [(\'needs_review\', \'BOOLEAN DEFAULT FALSE\'), (\'review_notes\', \'TEXT\'), (\'updated_at\', \'TIMESTAMP\')]),
                (\'vineyards\', [(\'updated_at\', \'TIMESTAMP\')]),
            ]:
                for col_name, col_type in cols:
                    try:
                        conn.execute(text(f"ALTER TABLE {tbl} ADD COLUMN {col_name} {col_type}"))
                        conn.commit()
                    except Exception:
                        conn.rollback()
    except Exception as e:
        print(f"Migration check: {e}")

_ensure_columns()

'''
if '_ensure_columns' not in code:
    code = code.replace('# Create router\nrouter = APIRouter(prefix', migration + '# Create router\nrouter = APIRouter(prefix', 1)
    changes.append('Added _ensure_columns() startup migration')

with open('/opt/vinosomm/app/admin_api.py', 'w') as f:
    f.write(code)

print(f'Original: {original_len} chars')
print(f'Fixed: {len(code)} chars')
print(f'Changes applied: {len(changes)}')
for c in changes:
    print(f'  - {c}')
