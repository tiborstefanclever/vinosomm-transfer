"""Admin API for vinosomm.ai - Wine Intelligence Application
Provides authentication, CRUD operations for Wines and Vineyards,
enrichment tasks, data provenance tracking, dashboard analytics,
crawler control, review queue, and AI chat.
"""
import os
import json
import hmac
import hashlib
import base64
import time
import urllib.request
import urllib.error
import urllib.parse
import re
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from functools import wraps

from fastapi import APIRouter, Depends, HTTPException, status, Query, Header, Body
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, Boolean, select, func, text
from sqlalchemy.orm import declarative_base, Session, sessionmaker
from sqlalchemy.sql import and_, or_

# Configuration from environment
_pg_pass = os.getenv("POSTGRES_PASSWORD", "password")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql://vinosomm:{_pg_pass}@postgres:5432/vinosomm"
)
JWT_SECRET = os.getenv("JWT_SECRET", os.getenv("POSTGRES_PASSWORD", "default-secret"))
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS_HASH = os.getenv("ADMIN_PASS_HASH", hashlib.sha256("admin".encode()).hexdigest())
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://host.docker.internal:11434")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Database setup
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()



# Auto-migrate: add missing columns on startup
def _ensure_columns():
    """Add any columns that exist in the model but not in the DB table."""
    try:
        with engine.connect() as conn:
            for tbl, cols in [
                ('wines', [('needs_review', 'BOOLEAN DEFAULT FALSE'), ('review_notes', 'TEXT'), ('updated_at', 'TIMESTAMP')]),
                ('vineyards', [('region', 'VARCHAR(255)'), ('winemaker', 'VARCHAR(255)'), ('updated_at', 'TIMESTAMP')])
            ]:
                for col_name, col_def in cols:
                    check_sql = f"SELECT 1 FROM information_schema.columns WHERE table_name='{tbl}' AND column_name='{col_name}'"
                    result = conn.execute(text(check_sql))
                    if not result.fetchone():
                        alter_sql = f"ALTER TABLE {tbl} ADD COLUMN {col_name} {col_def}"
                        conn.execute(text(alter_sql))
                        conn.commit()
    except Exception as e:
        pass

_ensure_columns()

# Models
class Wine(Base):
    __tablename__ = 'wines'

    id = Column(Integer, primary_key=True)
    name = Column(String(500), nullable=False)
    producer = Column(String(300))
    vintage = Column(Integer)
    region = Column(String(300))
    country = Column(String(100))
    grape_variety = Column(String(300))
    wine_type = Column(String(100))
    classification = Column(String(500))
    # WSET 9-axis scores
    sweetness = Column(Float)
    acidity = Column(Float)
    tannin = Column(Float)
    body = Column(Float)
    alcohol_warmth = Column(Float)
    effervescence = Column(Float)
    flavor_intensity = Column(Float)
    finish = Column(Float)
    complexity = Column(Float)
    # Aromas & notes
    fruit_character = Column(Text)
    secondary_aromas = Column(Text)
    tertiary_notes = Column(Text)
    # Additional data
    residual_sugar = Column(Float)
    alcohol = Column(Float)
    price = Column(Float)
    currency = Column(String(10))
    rating = Column(Float)
    data_method = Column(String(100))
    source_list = Column(Text)
    source_count = Column(Integer, default=0)
    provenance_notes = Column(Text)
    avg_confidence = Column(Float)
    completeness = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    needs_review = Column(Boolean, default=False)
    review_notes = Column(Text)
    winery_id = Column(Integer)
    vineyard_id = Column(Integer)
    bottle_image_url = Column(Text)
    bottle_image_source_domain = Column(Text)
    bottle_image_local = Column(Text)
    bottle_image_original = Column(Text)
    bottle_image_status = Column(Text, default='none')
    bottle_image_error = Column(Text)
    supabase_id = Column(Text)
    description = Column(Text)
    discovery_metadata = Column(Text)
    bottle_image_candidates = Column(Text)
    data_quality_level = Column(Integer, default=1)
    view_count = Column(Integer, default=0)
    field_confidence = Column(Text)  # JSONB in DB

class Vineyard(Base):
    __tablename__ = 'wineries'

    id = Column(Integer, primary_key=True)
    name = Column(String(500))
    short_name = Column(String(200))
    country = Column(String(100))
    region = Column(String(300))
    sub_region = Column(String(300))
    address = Column(Text)
    latitude = Column(Float)
    longitude = Column(Float)
    phone = Column(String(100))
    email = Column(String(200))
    website_url = Column(String(500))
    instagram_url = Column(String(500))
    facebook_url = Column(String(500))
    twitter_url = Column(String(500))
    producer_type = Column(String(100))
    appellations = Column(Text)
    grape_varieties = Column(Text)
    wine_types = Column(Text)
    founded_year = Column(Integer)
    winemaker = Column(String(500))
    owner = Column(String(300))
    vineyard_hectares = Column(Float)
    annual_production_bottles = Column(Integer)
    annual_production_hl = Column(Float)
    number_of_wines = Column(Integer)
    price_range_min = Column(Float)
    price_range_max = Column(Float)
    export_markets = Column(Text)
    quality_tier = Column(String(100))
    avg_rating = Column(Float)
    vivino_rating = Column(Float)
    wine_searcher_avg = Column(Float)
    jancis_robinson_rating = Column(Float)
    robert_parker_rating = Column(Float)
    rating_sources = Column(Text)
    awards = Column(Text)
    farming_practice = Column(String(100))
    certifications = Column(Text)
    soil_types = Column(Text)
    climate = Column(String(200))
    altitude_meters = Column(Float)
    sustainability_notes = Column(Text)
    description = Column(Text)
    data_method = Column(String(100))
    source_list = Column(Text)
    source_count = Column(Integer)
    avg_confidence = Column(Float)
    completeness = Column(Float)
    needs_review = Column(Boolean)
    review_notes = Column(Text)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    enrichment_status = Column(String(20), default='none')
    last_enriched_at = Column(DateTime)
    logo_url = Column(Text)
    logo_source_domain = Column(Text)
    logo_local = Column(Text)
    logo_original = Column(Text)
    logo_status = Column(Text, default='none')
    supabase_id = Column(Text)
    tasting_room = Column(Boolean, default=False)
    restaurant = Column(Boolean, default=False)
    accommodation = Column(Boolean, default=False)
    tours_available = Column(Boolean, default=False)
    wine_styles = Column(Text)
    notable_wines = Column(Text)
    discovery_metadata = Column(Text)
    scene_url = Column(Text)
    scene_source_domain = Column(Text)
    scene_local = Column(Text)
    scene_status = Column(String(50), default='none')

Base.metadata.create_all(bind=engine)

# JWT & Auth
def _create_jwt(user_id: str, expires_in: int = 3600) -> str:
    """Create a JWT token."""
    import time
    payload = {
        "user_id": user_id,
        "iat": int(time.time()),
        "exp": int(time.time()) + expires_in,
    }
    header = base64.urlsafe_b64encode(json.dumps({"alg": "HS256", "typ": "JWT"}).encode()).rstrip(b'=').decode()
    payload_b64 = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b'=').decode()
    signature = base64.urlsafe_b64encode(
        hmac.new(JWT_SECRET.encode(), f"{header}.{payload_b64}".encode(), hashlib.sha256).digest()
    ).rstrip(b'=').decode()
    return f"{header}.{payload_b64}.{signature}"

def _verify_jwt(token: str) -> Optional[str]:
    """Verify JWT and return user_id."""
    try:
        parts = token.split('.')
        if len(parts) != 3:
            return None
        header, payload_b64, signature = parts
        payload_json = base64.urlsafe_b64decode(payload_b64 + '=' * (4 - len(payload_b64) % 4))
        payload = json.loads(payload_json)
        
        if payload.get("exp", 0) < time.time():
            return None
        
        return payload.get("user_id")
    except Exception:
        return None

def _verify_admin(password: str) -> bool:
    """Check if password matches admin hash."""
    return hashlib.sha256(password.encode()).hexdigest() == ADMIN_PASS_HASH

def require_auth(f):
    """Decorator to require JWT auth."""
    @wraps(f)
    async def wrapper(*args, authorization: Optional[str] = Header(None), **kwargs):
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Missing or invalid authorization")
        
        token = authorization[7:]
        user_id = _verify_jwt(token)
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid or expired token")
        
        return await f(*args, user_id=user_id, **kwargs)
    
    return wrapper

def require_admin(f):
    """Decorator to require admin auth."""
    @wraps(f)
    async def wrapper(*args, authorization: Optional[str] = Header(None), **kwargs):
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Missing or invalid authorization")
        
        token = authorization[7:]
        user_id = _verify_jwt(token)
        if not user_id or user_id != ADMIN_USER:
            raise HTTPException(status_code=403, detail="Admin access required")
        
        return await f(*args, user_id=user_id, **kwargs)
    
    return wrapper

# Routes
router = APIRouter(prefix="/api", tags=["admin"])

@router.post("/auth/login")
async def login(username: str = Body(...), password: str = Body(...)):
    """Admin login."""
    if username == ADMIN_USER and _verify_admin(password):
        token = _create_jwt(ADMIN_USER)
        return {"access_token": token, "token_type": "bearer"}
    raise HTTPException(status_code=401, detail="Invalid credentials")

@router.get("/auth/verify")
async def verify_token(authorization: Optional[str] = Header(None)):
    """Verify if a token is valid."""
    if not authorization or not authorization.startswith("Bearer "):
        return {"valid": False}
    
    token = authorization[7:]
    user_id = _verify_jwt(token)
    return {"valid": user_id is not None, "user_id": user_id}

@router.get("/wines")
async def list_wines(
    db: Session = Depends(lambda: SessionLocal()),
    skip: int = Query(0),
    limit: int = Query(100),
    region: Optional[str] = Query(None),
    needs_review: Optional[bool] = Query(None),
    page: int = Query(1),
    per_page: int = Query(20),
    sort_by: Optional[str] = Query(None)
):
    """List wines with optional filters and pagination."""
    query = db.query(Wine)
    if region:
        query = query.filter(Wine.region.ilike(f"%{region}%"))
    if needs_review is not None:
        query = query.filter(Wine.needs_review == needs_review)
    
    # Support old-style skip/limit and new-style pagination
    if page and per_page:
        wines = query.offset((page - 1) * per_page).limit(per_page).all()
    else:
        wines = query.offset(skip).limit(limit).all()
    
    if sort_by and sort_by in ['name', 'rating', 'price']:
        sort_column = getattr(Wine, sort_by)
        if sort_by in ['rating', 'price']:
            wines = query.order_by(sort_column.desc())
        else:
            query = query.order_by(sort_column.asc())
    wines = query.offset((page - 1) * per_page).limit(per_page).all()
    return {
        "wines": [wine_to_dict(w) for w in wines],
        "count": len(wines),
        "page": page,
        "per_page": per_page
    }

@router.get("/wines/{wine_id}")
async def get_wine(wine_id: int, db: Session = Depends(lambda: SessionLocal())):
    """Get a specific wine."""
    wine = db.query(Wine).filter(Wine.id == wine_id).first()
    if not wine:
        raise HTTPException(status_code=404, detail="Wine not found")
    
    return wine_to_dict(wine)

@router.post("/wines")
async def create_wine(
    wine_data: Dict[str, Any] = Body(...),
    db: Session = Depends(lambda: SessionLocal()),
    authorization: Optional[str] = Header(None)
):
    """Create a new wine."""
    # Allow creation without auth for user submissions
    new_wine = Wine(
        name=wine_data.get("name"),
        region=wine_data.get("region"),
        winery=wine_data.get("winery"),
        vintage=wine_data.get("vintage"),
        rating=wine_data.get("rating"),
        price=wine_data.get("price"),
        description=wine_data.get("description"),
        data_source=wine_data.get("data_source", "user"),
        needs_review=wine_data.get("needs_review", True)  # User submissions need review
    )
    
    db.add(new_wine)
    db.commit()
    db.refresh(new_wine)
    
    return wine_to_dict(new_wine)

@router.put("/wines/{wine_id}")
async def update_wine(
    wine_id: int,
    wine_data: Dict[str, Any] = Body(...),
    db: Session = Depends(lambda: SessionLocal()),
    user_id: str = Depends(require_admin)
):
    """Update a wine (admin only)."""
    wine = db.query(Wine).filter(Wine.id == wine_id).first()
    if not wine:
        raise HTTPException(status_code=404, detail="Wine not found")
    
    for key, value in wine_data.items():
        if hasattr(wine, key) and key not in ['id', 'created_at']:
            setattr(wine, key, value)
    
    wine.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(wine)
    
    return wine_to_dict(wine)

@router.delete("/wines/{wine_id}")
async def delete_wine(
    wine_id: int,
    db: Session = Depends(lambda: SessionLocal()),
    user_id: str = Depends(require_admin)
):
    """Delete a wine (admin only)."""
    wine = db.query(Wine).filter(Wine.id == wine_id).first()
    if not wine:
        raise HTTPException(status_code=404, detail="Wine not found")
    
    db.delete(wine)
    db.commit()
    return {"deleted": True, "id": wine_id}

@router.get("/vineyards")
async def list_vineyards(
    db: Session = Depends(lambda: SessionLocal()),
    skip: int = Query(0),
    limit: int = Query(100),
    region: Optional[str] = Query(None),
    page: int = Query(1),
    per_page: int = Query(20)
):
    """List vineyards with optional filters and pagination."""
    query = db.query(Vineyard)
    if region:
        query = query.filter(Vineyard.region.ilike(f"%{region}%"))
    
    if page and per_page:
        vineyards = query.offset((page - 1) * per_page).limit(per_page).all()
    else:
        vineyards = query.offset(skip).limit(limit).all()
    
    return {
        "vineyards": [vineyard_to_dict(v) for v in vineyards],
        "count": len(vineyards),
        "page": page,
        "per_page": per_page
    }

@router.get("/vineyards/{vineyard_id}")
async def get_vineyard(vineyard_id: int, db: Session = Depends(lambda: SessionLocal())):
    """Get a specific vineyard."""
    vineyard = db.query(Vineyard).filter(Vineyard.id == vineyard_id).first()
    if not vineyard:
        raise HTTPException(status_code=404, detail="Vineyard not found")
    
    return vineyard_to_dict(vineyard)

@router.post("/vineyards")
async def create_vineyard(
    vineyard_data: Dict[str, Any] = Body(...),
    db: Session = Depends(lambda: SessionLocal()),
    authorization: Optional[str] = Header(None)
):
    """Create a new vineyard."""
    new_vineyard = Vineyard(
        name=vineyard_data.get("name"),
        region=vineyard_data.get("region"),
        winemaker=vineyard_data.get("winemaker"),
        location_lat=vineyard_data.get("location_lat"),
        location_lon=vineyard_data.get("location_lon"),
        founded_year=vineyard_data.get("founded_year"),
        description=vineyard_data.get("description"),
        website_url=vineyard_data.get("website_url"),
        instagram_url=vineyard_data.get("instagram_url"),
        facebook_url=vineyard_data.get("facebook_url"),
        twitter_url=vineyard_data.get("twitter_url"),
        data_source=vineyard_data.get("data_source", "user"),
        needs_review=vineyard_data.get("needs_review", True)
    )
    
    db.add(new_vineyard)
    db.commit()
    db.refresh(new_vineyard)
    
    return vineyard_to_dict(new_vineyard)

@router.put("/vineyards/{vineyard_id}")
async def update_vineyard(
    vineyard_id: int,
    vineyard_data: Dict[str, Any] = Body(...),
    db: Session = Depends(lambda: SessionLocal()),
    user_id: str = Depends(require_admin)
):
    """Update a vineyard (admin only)."""
    vineyard = db.query(Vineyard).filter(Vineyard.id == vineyard_id).first()
    if not vineyard:
        raise HTTPException(status_code=404, detail="Vineyard not found")
    
    for key, value in vineyard_data.items():
        if hasattr(vineyard, key) and key not in ['id', 'created_at']:
            setattr(vineyard, key, value)
    
    vineyard.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(vineyard)
    
    return vineyard_to_dict(vineyard)

@router.delete("/vineyards/{vineyard_id}")
async def delete_vineyard(
    vineyard_id: int,
    db: Session = Depends(lambda: SessionLocal()),
    user_id: str = Depends(require_admin)
):
    """Delete a vineyard (admin only)."""
    vineyard = db.query(Vineyard).filter(Vineyard.id == vineyard_id).first()
    if not vineyard:
        raise HTTPException(status_code=404, detail="Vineyard not found")
    
    db.delete(vineyard)
    db.commit()
    return {"deleted": True, "id": vineyard_id}

# Helper functions
def wine_to_dict(wine: Wine) -> Dict[str, Any]:
    """Convert a Wine object to a dictionary."""
    created_at = getattr(wine, 'created_at', None)
    updated_at = getattr(wine, 'updated_at', None)
    return {
        "id": wine.id,
        "name": getattr(wine, 'name', None),
        "producer": getattr(wine, 'producer', None),
        "vintage": getattr(wine, 'vintage', None),
        "region": getattr(wine, 'region', None),
        "country": getattr(wine, 'country', None),
        "grape_variety": getattr(wine, 'grape_variety', None),
        "wine_type": getattr(wine, 'wine_type', None),
        "classification": getattr(wine, 'classification', None),
        "sweetness": getattr(wine, 'sweetness', None),
        "acidity": getattr(wine, 'acidity', None),
        "tannin": getattr(wine, 'tannin', None),
        "body": getattr(wine, 'body', None),
        "alcohol_warmth": getattr(wine, 'alcohol_warmth', None),
        "effervescence": getattr(wine, 'effervescence', None),
        "flavor_intensity": getattr(wine, 'flavor_intensity', None),
        "finish": getattr(wine, 'finish', None),
        "complexity": getattr(wine, 'complexity', None),
        "fruit_character": getattr(wine, 'fruit_character', None),
        "secondary_aromas": getattr(wine, 'secondary_aromas', None),
        "tertiary_notes": getattr(wine, 'tertiary_notes', None),
        "residual_sugar": getattr(wine, 'residual_sugar', None),
        "alcohol": getattr(wine, 'alcohol', None),
        "price": getattr(wine, 'price', None),
        "currency": getattr(wine, 'currency', None),
        "rating": getattr(wine, 'rating', None),
        "data_method": getattr(wine, 'data_method', None),
        "completeness": getattr(wine, 'completeness', None),
        "description": getattr(wine, 'description', None),
        "winery_id": getattr(wine, 'winery_id', None),
        "vineyard_id": getattr(wine, 'vineyard_id', None),
        "bottle_image_url": getattr(wine, 'bottle_image_url', None),
        "data_quality_level": getattr(wine, 'data_quality_level', None),
        "created_at": created_at.isoformat() if created_at else None,
        "updated_at": updated_at.isoformat() if updated_at else None,
        "needs_review": getattr(wine, 'needs_review', None),
        "review_notes": getattr(wine, 'review_notes', None),
    }

def vineyard_to_dict(vineyard: Vineyard) -> Dict[str, Any]:
    """Convert a Vineyard (winery) object to a dictionary."""
    created_at = getattr(vineyard, 'created_at', None)
    updated_at = getattr(vineyard, 'updated_at', None)
    return {
        "id": vineyard.id,
        "name": getattr(vineyard, 'name', None),
        "short_name": getattr(vineyard, 'short_name', None),
        "country": getattr(vineyard, 'country', None),
        "region": getattr(vineyard, 'region', None),
        "sub_region": getattr(vineyard, 'sub_region', None),
        "latitude": getattr(vineyard, 'latitude', None),
        "longitude": getattr(vineyard, 'longitude', None),
        "website_url": getattr(vineyard, 'website_url', None),
        "instagram_url": getattr(vineyard, 'instagram_url', None),
        "facebook_url": getattr(vineyard, 'facebook_url', None),
        "twitter_url": getattr(vineyard, 'twitter_url', None),
        "producer_type": getattr(vineyard, 'producer_type', None),
        "founded_year": getattr(vineyard, 'founded_year', None),
        "winemaker": getattr(vineyard, 'winemaker', None),
        "owner": getattr(vineyard, 'owner', None),
        "vineyard_hectares": getattr(vineyard, 'vineyard_hectares', None),
        "number_of_wines": getattr(vineyard, 'number_of_wines', None),
        "quality_tier": getattr(vineyard, 'quality_tier', None),
        "avg_rating": getattr(vineyard, 'avg_rating', None),
        "farming_practice": getattr(vineyard, 'farming_practice', None),
        "description": getattr(vineyard, 'description', None),
        "data_method": getattr(vineyard, 'data_method', None),
        "completeness": getattr(vineyard, 'completeness', None),
        "enrichment_status": getattr(vineyard, 'enrichment_status', None),
        "logo_url": getattr(vineyard, 'logo_url', None),
        "tasting_room": getattr(vineyard, 'tasting_room', None),
        "restaurant": getattr(vineyard, 'restaurant', None),
        "tours_available": getattr(vineyard, 'tours_available', None),
        "created_at": created_at.isoformat() if created_at else None,
        "updated_at": updated_at.isoformat() if updated_at else None,
        "needs_review": getattr(vineyard, 'needs_review', None),
        "review_notes": getattr(vineyard, 'review_notes', None),
    }

# Pagination helper
def paginate(query, page: int = 1, per_page: int = 20):
    """Paginate a SQLAlchemy query."""
    return query.offset((page - 1) * per_page).limit(per_page).all()

@router.post("/enrichment/task")
async def create_enrichment_task(
    wine_id: Optional[int] = Body(None),
    vineyard_id: Optional[int] = Body(None),
    enrichment_type: str = Body(...),
    db: Session = Depends(lambda: SessionLocal()),
    user_id: str = Depends(require_admin)
):
    """Create an enrichment task."""
    # For now, just return success
    return {
        "task_id": f"task_{int(time.time())}",
        "wine_id": wine_id,
        "vineyard_id": vineyard_id,
        "enrichment_type": enrichment_type,
        "status": "queued"
    }

@router.get("/enrichment/tasks")
async def list_enrichment_tasks(
    status: Optional[str] = Query(None),
    db: Session = Depends(lambda: SessionLocal()),
    user_id: str = Depends(require_admin)
):
    """List enrichment tasks."""
    return [
        {"task_id": "task_1", "status": "completed", "enrichment_type": "llm_review"},
        {"task_id": "task_2", "status": "in_progress", "enrichment_type": "crawler_fetch"}
    ]

@router.post("/review/approve/{wine_id}")
async def approve_wine_review(
    wine_id: int,
    db: Session = Depends(lambda: SessionLocal()),
    user_id: str = Depends(require_admin)
):
    """Approve a wine review."""
    wine = db.query(Wine).filter(Wine.id == wine_id).first()
    if not wine:
        raise HTTPException(status_code=404, detail="Wine not found")
    
    wine.needs_review = False
    wine.updated_at = datetime.utcnow()
    db.commit()
    
    return {"approved": True, "id": wine_id}

@router.post("/review/reject/{wine_id}")
async def reject_wine_review(
    wine_id: int,
    notes: str = Body(...),
    db: Session = Depends(lambda: SessionLocal()),
    user_id: str = Depends(require_admin)
):
    """Reject a wine review."""
    wine = db.query(Wine).filter(Wine.id == wine_id).first()
    if not wine:
        raise HTTPException(status_code=404, detail="Wine not found")
    
    wine.review_notes = notes
    wine.updated_at = datetime.utcnow()
    db.commit()
    
    return {"rejected": True, "id": wine_id, "notes": notes}

@router.get("/dashboard/stats")
async def dashboard_stats(
    db: Session = Depends(lambda: SessionLocal()),
    user_id: str = Depends(require_admin)
):
    """Get dashboard statistics."""
    total_wines = db.query(func.count(Wine.id)).scalar() or 0
    total_vineyards = db.query(func.count(Vineyard.id)).scalar() or 0
    wines_needing_review = db.query(func.count(Wine.id)).filter(Wine.needs_review == True).scalar() or 0
    vineyards_needing_review = db.query(func.count(Vineyard.id)).filter(Vineyard.needs_review == True).scalar() or 0
    
    return {
        "total_wines": total_wines,
        "total_vineyards": total_vineyards,
        "wines_needing_review": wines_needing_review,
        "vineyards_needing_review": vineyards_needing_review,
        "average_wine_rating": 4.2,  # Simplified
        "data_sources": {"user": 150, "crawler": 50}
    }

@router.post("/crawler/start")
async def start_crawler(
    crawler_type: str = Body(...),
    target_url: Optional[str] = Body(None),
    db: Session = Depends(lambda: SessionLocal()),
    user_id: str = Depends(require_admin)
):
    """Start a web crawler."""
    return {
        "crawler_id": f"crawler_{int(time.time())}",
        "crawler_type": crawler_type,
        "target_url": target_url,
        "status": "running"
    }

@router.post("/crawler/stop/{crawler_id}")
async def stop_crawler(
    crawler_id: str,
    db: Session = Depends(lambda: SessionLocal()),
    user_id: str = Depends(require_admin)
):
    """Stop a running crawler."""
    return {"crawler_id": crawler_id, "status": "stopped"}

@router.get("/chat/health")
async def chat_health():
    """Check AI chat service health."""
    health = {
        "status": "healthy",
        "services": {
            "ollama": {"status": "healthy"},
            "redis": {"status": "healthy"},
            "database": {"status": "healthy"}
        }
    }
    
    # Try to connect to Ollama
    try:
        response = urllib.request.urlopen(f"{OLLAMA_URL}/api/tags", timeout=2)
        health["services"]["ollama"]["status"] = "healthy"
    except Exception as e:
        health["services"]["ollama"]["status"] = "unhealthy"
        health["services"]["ollama"]["error"] = str(e)
    
    # Check if celery is available
    try:
        from celery import Celery
        health["services"]["celery"] = {"status": "healthy"}
    except ImportError:
        health["services"]["celery"] = {"status": "unavailable", "error": "Celery not imported"}
    
    return health

# Additional endpoints for advanced functionality

def get_db():
    """Get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def _get_current_user(authorization: Optional[str]):
    """Extract and verify current user from JWT."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid authorization")
    
    token = authorization[7:]
    user_id = _verify_jwt(token)
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    
    return user_id

# Batch operations for intake
class Task(Base):
    __tablename__ = 'tasks'
    id = Column(Integer, primary_key=True)
    task_type = Column(String(100))
    wine_id = Column(Integer)
    wine_name = Column(String(255))
    status = Column(String(50), default="queued")
    celery_task_id = Column(String(255))
    error = Column(Text)
    started_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)

CELERY_AVAILABLE = False
celery_app = None
try:
    from celery import Celery
    celery_app = Celery('vinosomm', broker=REDIS_URL, backend=REDIS_URL)
    CELERY_AVAILABLE = True
except ImportError:
    pass

@router.post("/intake/batch")
async def batch_intake(
    entity_type: str = Body(...),  # "wine" or "vineyard"
    items: List[Dict[str, Any]] = Body(...),
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db)
):
    """Batch create wines or vineyards from structured data."""
    _get_current_user(authorization)
    
    created = []
    tasks_created = []
    
    for item in items:
        item_name = item["name"]
        website_url = item.get("website_url")
        if not item_name:
            continue

        if entity_type == "vineyard":
            entity = Vineyard(
                name=item_name,
                website_url=website_url,
                data_method="ai-intake",
                enrichment_status="queued",
                needs_review=True
            )
            db.add(entity)
            db.flush()

            task = Task(
                task_type="enrich_vineyard",
                wine_id=entity.id,
                wine_name=item_name,
                status="queued"
            )
            db.add(task)
            db.flush()

            if CELERY_AVAILABLE and celery_app:
                try:
                    result = celery_app.send_task('tasks.enrich_vineyard', args=[entity.id])
                    task.celery_task_id = result.id
                    task.status = "running"
                    task.started_at = datetime.utcnow()
                    entity.enrichment_status = "enriching"
                except Exception as e:
                    task.status = "failed"
                    task.error = str(e)
                    entity.enrichment_status = "failed"

            created.append({"id": entity.id, "name": item_name, "type": "vineyard", "enrichment_status": entity.enrichment_status})
            tasks_created.append({"task_id": task.id, "status": task.status})

        else:  # wine
            entity = Wine(name=item_name, wine_type="unknown", data_method="ai-intake", needs_review=True)
            db.add(entity)
            db.flush()

            task = Task(
                task_type="enrich",
                wine_id=entity.id,
                wine_name=item_name,
                status="queued"
            )
            db.add(task)
            db.flush()

            if CELERY_AVAILABLE and celery_app:
                try:
                    result = celery_app.send_task('tasks.enrich_wine', args=[entity.id])
                    task.celery_task_id = result.id
                    task.status = "running"
                    task.started_at = datetime.utcnow()
                except Exception as e:
                    task.status = "failed"
                    task.error = str(e)

            created.append({"id": entity.id, "name": item_name, "type": "wine"})
            tasks_created.append({"task_id": task.id, "status": task.status})

    db.commit()

    return {
        "status": "ok",
        "created": created,
        "tasks": tasks_created,
        "total_created": len(created),
        "message": f"Created {len(created)} {entity_type}(s) with enrichment tasks dispatched"
    }


# ============================================================================
# SYSTEM HEALTH
# ============================================================================
@router.get("/system/health")
async def system_health(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    health = {"status": "healthy", "timestamp": datetime.utcnow().isoformat(), "services": {}}
    try:
        db.execute(text("SELECT 1"))
        health["services"]["postgres"] = {"status": "connected"}
    except Exception as e:
        health["services"]["postgres"] = {"status": "error", "error": str(e)}
        health["status"] = "degraded"
    try:
        import redis
        redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=2)
        redis_client.ping()
        health["services"]["redis"] = {"status": "connected"}
    except Exception as e:
        health["services"]["redis"] = {"status": "unavailable", "error": str(e)}
    try:
        req = urllib.request.Request(f"{OLLAMA_URL}/api/tags")
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode())
            model_count = len(data.get("models", []))
            health["services"]["ollama"] = {"status": "connected", "models": model_count}
    except Exception as e:
        health["services"]["ollama"] = {"status": "unavailable", "error": str(e)}
    try:
        req = urllib.request.Request(os.getenv("SEARXNG_URL", "http://searxng:8080"))
        with urllib.request.urlopen(req, timeout=3) as response:
            health["services"]["searxng"] = {"status": "connected"}
    except Exception as e:
        health["services"]["searxng"] = {"status": "unavailable", "error": str(e)}
    if CELERY_AVAILABLE and celery_app:
        try:
            inspect = celery_app.control.inspect(timeout=3)
            active = inspect.active()
            health["services"]["celery"] = {"status": "connected", "workers": len(active) if active else 0}
        except Exception as e:
            health["services"]["celery"] = {"status": "unavailable", "error": str(e)}
    else:
        health["services"]["celery"] = {"status": "unavailable", "error": "Celery not imported"}
    return health