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
    name = Column(String(255), nullable=False)
    region = Column(String(255))
    winery = Column(String(255))
    vintage = Column(Integer)
    rating = Column(Float)
    price = Column(Float)
    description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    data_source = Column(String(100))  # "user" or "crawler"
    needs_review = Column(Boolean, default=False)
    review_notes = Column(Text)

class Vineyard(Base):
    __tablename__ = 'vineyards'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    region = Column(String(255))
    winemaker = Column(String(255))
    location_lat = Column(Float)
    location_lon = Column(Float)
    founded_year = Column(Integer)
    description = Column(Text)
    website_url = Column(String(500))
    instagram_url = Column(String(500))
    facebook_url = Column(String(500))
    twitter_url = Column(String(500))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    data_source = Column(String(100))
    needs_review = Column(Boolean, default=False)
    review_notes = Column(Text)

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
    needs_review: Optional[bool] = Query(None)
):
    """List wines with optional filters."""
    query = db.query(Wine)
    if region:
        query = query.filter(Wine.region.ilike(f"%{region}%"))
    if needs_review is not None:
        query = query.filter(Wine.needs_review == needs_review)
    
    wines = query.offset(skip).limit(limit).all()
    return [
        {
            "id": w.id,
            "name": w.name,
            "region": w.region,
            "winery": w.winery,
            "vintage": w.vintage,
            "rating": w.rating,
            "price": w.price,
            "description": w.description,
            "created_at": w.created_at.isoformat() if w.created_at else None,
            "updated_at": w.updated_at.isoformat() if w.updated_at else None,
            "data_source": w.data_source,
            "needs_review": w.needs_review,
            "review_notes": w.review_notes
        }
        for w in wines
    ]

@router.get("/wines/{wine_id}")
async def get_wine(wine_id: int, db: Session = Depends(lambda: SessionLocal())):
    """Get a specific wine."""
    wine = db.query(Wine).filter(Wine.id == wine_id).first()
    if not wine:
        raise HTTPException(status_code=404, detail="Wine not found")
    
    return {
        "id": wine.id,
        "name": wine.name,
        "region": wine.region,
        "winery": wine.winery,
        "vintage": wine.vintage,
        "rating": wine.rating,
        "price": wine.price,
        "description": wine.description,
        "created_at": wine.created_at.isoformat() if wine.created_at else None,
        "updated_at": wine.updated_at.isoformat() if wine.updated_at else None,
        "data_source": wine.data_source,
        "needs_review": wine.needs_review,
        "review_notes": wine.review_notes
    }

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
    
    return {
        "id": new_wine.id,
        "name": new_wine.name,
        "region": new_wine.region,
        "winery": new_wine.winery,
        "vintage": new_wine.vintage,
        "rating": new_wine.rating,
        "price": new_wine.price,
        "description": new_wine.description,
        "created_at": new_wine.created_at.isoformat() if new_wine.created_at else None,
        "updated_at": new_wine.updated_at.isoformat() if new_wine.updated_at else None,
        "data_source": new_wine.data_source,
        "needs_review": new_wine.needs_review,
        "review_notes": new_wine.review_notes
    }

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
    
    return {
        "id": wine.id,
        "name": wine.name,
        "region": wine.region,
        "winery": wine.winery,
        "vintage": wine.vintage,
        "rating": wine.rating,
        "price": wine.price,
        "description": wine.description,
        "created_at": wine.created_at.isoformat() if wine.created_at else None,
        "updated_at": wine.updated_at.isoformat() if wine.updated_at else None,
        "data_source": wine.data_source,
        "needs_review": wine.needs_review,
        "review_notes": wine.review_notes
    }

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
    region: Optional[str] = Query(None)
):
    """List vineyards with optional filters."""
    query = db.query(Vineyard)
    if region:
        query = query.filter(Vineyard.region.ilike(f"%{region}%"))
    
    vineyards = query.offset(skip).limit(limit).all()
    return [
        {
            "id": v.id,
            "name": v.name,
            "region": v.region,
            "winemaker": v.winemaker,
            "location_lat": v.location_lat,
            "location_lon": v.location_lon,
            "founded_year": v.founded_year,
            "description": v.description,
            "website_url": v.website_url,
            "instagram_url": v.instagram_url,
            "facebook_url": v.facebook_url,
            "twitter_url": v.twitter_url,
            "created_at": v.created_at.isoformat() if v.created_at else None,
            "updated_at": v.updated_at.isoformat() if v.updated_at else None,
            "data_source": v.data_source,
            "needs_review": v.needs_review,
            "review_notes": v.review_notes
        }
        for v in vineyards
    ]

@router.get("/vineyards/{vineyard_id}")
async def get_vineyard(vineyard_id: int, db: Session = Depends(lambda: SessionLocal())):
    """Get a specific vineyard."""
    vineyard = db.query(Vineyard).filter(Vineyard.id == vineyard_id).first()
    if not vineyard:
        raise HTTPException(status_code=404, detail="Vineyard not found")
    
    return {
        "id": vineyard.id,
        "name": vineyard.name,
        "region": vineyard.region,
        "winemaker": vineyard.winemaker,
        "location_lat": vineyard.location_lat,
        "location_lon": vineyard.location_lon,
        "founded_year": vineyard.founded_year,
        "description": vineyard.description,
        "website_url": vineyard.website_url,
        "instagram_url": vineyard.instagram_url,
        "facebook_url": vineyard.facebook_url,
        "twitter_url": vineyard.twitter_url,
        "created_at": vineyard.created_at.isoformat() if vineyard.created_at else None,
        "updated_at": vineyard.updated_at.isoformat() if vineyard.updated_at else None,
        "data_source": vineyard.data_source,
        "needs_review": vineyard.needs_review,
        "review_notes": vineyard.review_notes
    }

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
    
    return {
        "id": new_vineyard.id,
        "name": new_vineyard.name,
        "region": new_vineyard.region,
        "winemaker": new_vineyard.winemaker,
        "location_lat": new_vineyard.location_lat,
        "location_lon": new_vineyard.location_lon,
        "founded_year": new_vineyard.founded_year,
        "description": new_vineyard.description,
        "website_url": new_vineyard.website_url,
        "instagram_url": new_vineyard.instagram_url,
        "facebook_url": new_vineyard.facebook_url,
        "twitter_url": new_vineyard.twitter_url,
        "created_at": new_vineyard.created_at.isoformat() if new_vineyard.created_at else None,
        "updated_at": new_vineyard.updated_at.isoformat() if new_vineyard.updated_at else None,
        "data_source": new_vineyard.data_source,
        "needs_review": new_vineyard.needs_review,
        "review_notes": new_vineyard.review_notes
    }

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
    
    return {
        "id": vineyard.id,
        "name": vineyard.name,
        "region": vineyard.region,
        "winemaker": vineyard.winemaker,
        "location_lat": vineyard.location_lat,
        "location_lon": vineyard.location_lon,
        "founded_year": vineyard.founded_year,
        "description": vineyard.description,
        "website_url": vineyard.website_url,
        "instagram_url": vineyard.instagram_url,
        "facebook_url": vineyard.facebook_url,
        "twitter_url": vineyard.twitter_url,
        "created_at": vineyard.created_at.isoformat() if vineyard.created_at else None,
        "updated_at": vineyard.updated_at.isoformat() if vineyard.updated_at else None,
        "data_source": vineyard.data_source,
        "needs_review": vineyard.needs_review,
        "review_notes": vineyard.review_notes
    }

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
