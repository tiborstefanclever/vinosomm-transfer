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
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from functools import wraps

from fastapi import APIRouter, Depends, HTTPException, status, Query, Header
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, Boolean, select, func, text
from sqlalchemy.orm import declarative_base, Session, sessionmaker
from sqlalchemy.sql import and_, or_

_pg_pass = os.getenv("POSTGRES_PASSWORD", "password")
DATABASE_URL = os.getenv("DATABASE_URL", f"postgresql://vinosomm:{_pg_pass}@postgres:5432/vinosomm")
JWT_SECRET = os.getenv("JWT_SECRET", os.getenv("POSTGRES_PASSWORD", "default-secret"))
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS_HASH = os.getenv("ADMIN_PASS_HASH", hashlib.sha256("admin".encode()).hexdigest())
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://host.docker.internal:11434")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()
router = APIRouter(prefix="/admin/api", tags=["admin"])

try:
    from celery import Celery
    celery_app = Celery("vinosomm", broker=REDIS_URL, backend=REDIS_URL)
    CELERY_AVAILABLE = True
except ImportError:
    celery_app = None
    CELERY_AVAILABLE = False

# ============================================================================
# ORM Models
# ============================================================================
class Wine(Base):
    __tablename__ = "wines"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(500), index=True)
    producer = Column(String(300))
    vineyard_id = Column(Integer, index=True)
    vintage = Column(Integer)
    region = Column(String(300))
    country = Column(String(100), index=True)
    grape_variety = Column(String(300))
    wine_type = Column(String(50), index=True)
    classification = Column(String(200))
    sweetness = Column(Float)
    acidity = Column(Float)
    tannin = Column(Float)
    body = Column(Float)
    alcohol_warmth = Column(Float)
    effervescence = Column(Float)
    flavor_intensity = Column(Float)
    finish = Column(Float)
    complexity = Column(Float)
    fruit_character = Column(Text)
    secondary_aromas = Column(Text)
    tertiary_notes = Column(Text)
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
    needs_review = Column(Boolean, default=False)
    review_notes = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Vineyard(Base):
    __tablename__ = "vineyards"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(500), index=True)
    short_name = Column(String(200))
    country = Column(String(100), index=True)
    region = Column(String(300))
    sub_region = Column(String(300))
    address = Column(Text)
    latitude = Column(Float)
    longitude = Column(Float)
    phone = Column(String(50))
    email = Column(String(200))
    website_url = Column(String(500))
    instagram_url = Column(String(500))
    facebook_url = Column(String(500))
    twitter_url = Column(String(500))
    producer_type = Column(String(50))
    appellations = Column(Text)
    grape_varieties = Column(Text)
    wine_types = Column(Text)
    founded_year = Column(Integer)
    winemaker = Column(String(300))
    owner = Column(String(300))
    vineyard_hectares = Column(Float)
    annual_production_bottles = Column(Integer)
    annual_production_hl = Column(Float)
    number_of_wines = Column(Integer)
    price_range_min = Column(Float)
    price_range_max = Column(Float)
    export_markets = Column(Text)
    quality_tier = Column(String(50))
    avg_rating = Column(Float)
    vivino_rating = Column(Float)
    wine_searcher_avg = Column(Float)
    jancis_robinson_rating = Column(Float)
    robert_parker_rating = Column(Float)
    rating_sources = Column(Text)
    awards = Column(Text)
    farming_practice = Column(String(50))
    certifications = Column(Text)
    soil_types = Column(Text)
    climate = Column(String(50))
    altitude_meters = Column(Float)
    sustainability_notes = Column(Text)
    description = Column(Text)
    data_method = Column(String(100))
    source_list = Column(Text)
    source_count = Column(Integer, default=0)
    avg_confidence = Column(Float)
    completeness = Column(Float)
    needs_review = Column(Boolean, default=False)
    review_notes = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class DataProvenance(Base):
    __tablename__ = "data_provenance"
    id = Column(Integer, primary_key=True, index=True)
    entity_type = Column(String(50), index=True)
    entity_id = Column(Integer, index=True)
    field_name = Column(String(100), index=True)
    value = Column(Text)
    source_url = Column(Text)
    source_name = Column(String(200))
    source_type = Column(String(50))
    source_excerpt = Column(String(500))
    content_hash = Column(String(64))
    confidence = Column(Float)
    scraped_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)


class Task(Base):
    __tablename__ = "tasks"
    id = Column(Integer, primary_key=True, index=True)
    task_type = Column(String(50))
    wine_id = Column(Integer)
    wine_name = Column(String(500))
    status = Column(String(20), default="queued")
    celery_task_id = Column(String(200))
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    result = Column(Text)
    error = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


class Setting(Base):
    __tablename__ = "admin_settings"
    id = Column(Integer, primary_key=True, index=True)
    key = Column(String(100), unique=True, index=True)
    value = Column(Text)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

try:
    Base.metadata.create_all(bind=engine)
except Exception:
    pass

# ============================================================================
# JWT Auth
# ============================================================================
def _encode_jwt(payload):
    header = base64.urlsafe_b64encode(json.dumps({"alg": "HS256", "typ": "JWT"}).encode()).rstrip(b"=").decode()
    body = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
    message = f"{header}.{body}".encode()
    signature = base64.urlsafe_b64encode(hmac.new(JWT_SECRET.encode(), message, hashlib.sha256).digest()).rstrip(b"=").decode()
    return f"{header}.{body}.{signature}"

def _decode_jwt(token):
    try:
        parts = token.split(".")
        if len(parts) != 3: return None
        header, body, signature = parts
        message = f"{header}.{body}".encode()
        expected_sig = base64.urlsafe_b64encode(hmac.new(JWT_SECRET.encode(), message, hashlib.sha256).digest()).rstrip(b"=").decode()
        if not hmac.compare_digest(signature, expected_sig): return None
        padding = "=" * (4 - len(body) % 4)
        payload = json.loads(base64.urlsafe_b64decode(body + padding))
        if payload.get("exp", 0) < time.time(): return None
        return payload
    except Exception: return None

def _get_current_user(authorization=Header(None)):
    if not authorization: raise HTTPException(status_code=401, detail="Missing token")
    try:
        scheme, token = authorization.split(" ")
        if scheme.lower() != "bearer": raise ValueError()
    except: raise HTTPException(status_code=401, detail="Invalid token format")
    payload = _decode_jwt(token)
    if not payload or payload.get("sub") != ADMIN_USER: raise HTTPException(status_code=401, detail="Invalid token")
    return payload.get("sub")

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

# ============================================================================
# Serializers
# ============================================================================
def wine_to_dict(w):
    return {"id": w.id, "name": w.name, "producer": w.producer, "vineyard_id": w.vineyard_id, "vintage": w.vintage, "region": w.region, "country": w.country, "grape_variety": w.grape_variety, "wine_type": w.wine_type, "classification": w.classification, "sweetness": w.sweetness, "acidity": w.acidity, "tannin": w.tannin, "body": w.body, "alcohol_warmth": w.alcohol_warmth, "effervescence": w.effervescence, "flavor_intensity": w.flavor_intensity, "finish": w.finish, "complexity": w.complexity, "fruit_character": w.fruit_character, "secondary_aromas": w.secondary_aromas, "tertiary_notes": w.tertiary_notes, "residual_sugar": w.residual_sugar, "alcohol": w.alcohol, "price": w.price, "currency": w.currency, "rating": w.rating, "data_method": w.data_method, "source_list": w.source_list, "source_count": w.source_count, "provenance_notes": w.provenance_notes, "avg_confidence": w.avg_confidence, "completeness": w.completeness, "needs_review": w.needs_review, "review_notes": w.review_notes, "created_at": w.created_at.isoformat() if w.created_at else None, "updated_at": w.updated_at.isoformat() if w.updated_at else None}

def vineyard_to_dict(v):
    return {"id": v.id, "name": v.name, "short_name": v.short_name, "country": v.country, "region": v.region, "sub_region": v.sub_region, "address": v.address, "latitude": v.latitude, "longitude": v.longitude, "phone": v.phone, "email": v.email, "website_url": v.website_url, "instagram_url": v.instagram_url, "facebook_url": v.facebook_url, "twitter_url": v.twitter_url, "producer_type": v.producer_type, "appellations": v.appellations, "grape_varieties": v.grape_varieties, "wine_types": v.wine_types, "founded_year": v.founded_year, "winemaker": v.winemaker, "owner": v.owner, "vineyard_hectares": v.vineyard_hectares, "annual_production_bottles": v.annual_production_bottles, "annual_production_hl": v.annual_production_hl, "number_of_wines": v.number_of_wines, "price_range_min": v.price_range_min, "price_range_max": v.price_range_max, "export_markets": v.export_markets, "quality_tier": v.quality_tier, "avg_rating": v.avg_rating, "vivino_rating": v.vivino_rating, "wine_searcher_avg": v.wine_searcher_avg, "jancis_robinson_rating": v.jancis_robinson_rating, "robert_parker_rating": v.robert_parker_rating, "rating_sources": v.rating_sources, "awards": v.awards, "farming_practice": v.farming_practice, "certifications": v.certifications, "soil_types": v.soil_types, "climate": v.climate, "altitude_meters": v.altitude_meters, "sustainability_notes": v.sustainability_notes, "description": v.description, "data_method": v.data_method, "source_list": v.source_list, "source_count": v.source_count, "avg_confidence": v.avg_confidence, "completeness": v.completeness, "needs_review": v.needs_review, "review_notes": v.review_notes, "created_at": v.created_at.isoformat() if v.created_at else None, "updated_at": v.updated_at.isoformat() if v.updated_at else None}

def provenance_to_dict(p):
    return {"id": p.id, "entity_type": p.entity_type, "entity_id": p.entity_id, "field_name": p.field_name, "value": p.value, "source_url": p.source_url, "source_name": p.source_name, "source_type": p.source_type, "source_excerpt": p.source_excerpt, "content_hash": p.content_hash, "confidence": p.confidence, "scraped_at": p.scraped_at.isoformat() if p.scraped_at else None, "created_at": p.created_at.isoformat() if p.created_at else None}

def task_to_dict(t):
    return {"id": t.id, "task_type": t.task_type, "wine_id": t.wine_id, "wine_name": t.wine_name, "status": t.status, "celery_task_id": t.celery_task_id, "started_at": t.started_at.isoformat() if t.started_at else None, "completed_at": t.completed_at.isoformat() if t.completed_at else None, "result": t.result, "error": t.error, "created_at": t.created_at.isoformat() if t.created_at else None}

# ============================================================================
# AUTH
# ============================================================================
@router.post("/auth/login")
async def login(payload: Dict[str, str]):
    username, password = payload.get("username"), payload.get("password")
    if not username or not password: raise HTTPException(status_code=400, detail="Missing credentials")
    if username != ADMIN_USER or hashlib.sha256(password.encode()).hexdigest() != ADMIN_PASS_HASH: raise HTTPException(status_code=401, detail="Invalid credentials")
    return {"access_token": _encode_jwt({"sub": ADMIN_USER, "iat": int(time.time()), "exp": int(time.time()) + 86400}), "token_type": "bearer", "expires_in": 86400}

@router.get("/auth/me")
async def get_current_user_info(authorization: Optional[str] = Header(None)):
    return {"username": _get_current_user(authorization), "role": "admin"}

# ============================================================================
# DASHBOARD
# ============================================================================
@router.get("/dashboard")
async def get_dashboard(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    total_wines = db.query(func.count(Wine.id)).scalar() or 0
    wines_by_type = {r[0] or "unknown": r[1] for r in db.query(Wine.wine_type, func.count(Wine.id)).group_by(Wine.wine_type).all()}
    wines_by_country = {r[0] or "unknown": r[1] for r in db.query(Wine.country, func.count(Wine.id)).group_by(Wine.country).order_by(func.count(Wine.id).desc()).limit(10).all()}
    recent_wines = [wine_to_dict(w) for w in db.query(Wine).order_by(Wine.created_at.desc()).limit(5).all()]
    avg_completeness = float(db.query(func.avg(Wine.completeness)).scalar() or 0)
    now = datetime.utcnow()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = today_start - timedelta(days=now.weekday())
    month_start = today_start.replace(day=1)
    total_vineyards = db.query(func.count(Vineyard.id)).scalar() or 0
    vineyards_by_country = {r[0] or "unknown": r[1] for r in db.query(Vineyard.country, func.count(Vineyard.id)).group_by(Vineyard.country).order_by(func.count(Vineyard.id).desc()).limit(10).all()}
    vineyards_by_type = {r[0] or "unknown": r[1] for r in db.query(Vineyard.producer_type, func.count(Vineyard.id)).group_by(Vineyard.producer_type).all()}
    return {
        "total_wines": total_wines, "wines_by_type": wines_by_type, "wines_by_country": wines_by_country,
        "recent_wines": recent_wines, "avg_completeness": avg_completeness,
        "enrichment_stats": {"total_with_rating": db.query(func.count(Wine.id)).filter(Wine.rating.isnot(None)).scalar() or 0, "total_with_source": db.query(func.count(Wine.id)).filter(Wine.source_list.isnot(None)).scalar() or 0},
        "wines_added_today": db.query(func.count(Wine.id)).filter(Wine.created_at >= today_start).scalar() or 0,
        "wines_added_this_week": db.query(func.count(Wine.id)).filter(Wine.created_at >= week_start).scalar() or 0,
        "wines_added_this_month": db.query(func.count(Wine.id)).filter(Wine.created_at >= month_start).scalar() or 0,
        "task_stats": {"queued": db.query(func.count(Task.id)).filter(Task.status == "queued").scalar() or 0, "running": db.query(func.count(Task.id)).filter(Task.status == "running").scalar() or 0, "completed": db.query(func.count(Task.id)).filter(Task.status == "completed").scalar() or 0, "failed": db.query(func.count(Task.id)).filter(Task.status == "failed").scalar() or 0},
        "needs_review": db.query(func.count(Wine.id)).filter(Wine.needs_review == True).scalar() or 0,
        "total_vineyards": total_vineyards, "vineyards_by_country": vineyards_by_country, "vineyards_by_type": vineyards_by_type,
        "total_provenance_records": db.query(func.count(DataProvenance.id)).scalar() or 0,
    }

# ============================================================================
# WINES CRUD
# ============================================================================
@router.get("/wines")
async def list_wines(authorization: Optional[str] = Header(None), db: Session = Depends(get_db), search: Optional[str] = Query(None), wine_type: Optional[str] = Query(None), country: Optional[str] = Query(None), vineyard_id: Optional[int] = Query(None), min_completeness: Optional[float] = Query(None), max_completeness: Optional[float] = Query(None), needs_review: Optional[bool] = Query(None), sort_by: str = Query("created_at"), sort_dir: str = Query("desc"), page: int = Query(1, ge=1), per_page: int = Query(20, ge=1, le=100)):
    _get_current_user(authorization)
    q = db.query(Wine)
    if search: s = f"%{search}%"; q = q.filter(or_(Wine.name.ilike(s), Wine.producer.ilike(s), Wine.region.ilike(s)))
    if wine_type: q = q.filter(Wine.wine_type == wine_type)
    if country: q = q.filter(Wine.country == country)
    if vineyard_id is not None: q = q.filter(Wine.vineyard_id == vineyard_id)
    if min_completeness is not None: q = q.filter(Wine.completeness >= min_completeness)
    if max_completeness is not None: q = q.filter(Wine.completeness <= max_completeness)
    if needs_review is not None: q = q.filter(Wine.needs_review == needs_review)
    total = q.count()
    col = getattr(Wine, sort_by, Wine.created_at)
    q = q.order_by(col.asc() if sort_dir.lower() == "asc" else col.desc())
    return {"wines": [wine_to_dict(w) for w in q.offset((page-1)*per_page).limit(per_page).all()], "total": total, "page": page, "per_page": per_page, "total_pages": (total+per_page-1)//per_page}

@router.get("/wines/{wine_id}")
async def get_wine(wine_id: int, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    w = db.query(Wine).filter(Wine.id == wine_id).first()
    if not w: raise HTTPException(status_code=404, detail="Wine not found")
    return wine_to_dict(w)

@router.post("/wines")
async def create_wine(payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    if not payload.get("name") or not payload.get("wine_type"): raise HTTPException(status_code=400, detail="name and wine_type required")
    w = Wine(**{k: v for k, v in payload.items() if hasattr(Wine, k) and k != 'id'})
    db.add(w); db.commit(); db.refresh(w)
    return wine_to_dict(w)

@router.put("/wines/{wine_id}")
async def update_wine(wine_id: int, payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    w = db.query(Wine).filter(Wine.id == wine_id).first()
    if not w: raise HTTPException(status_code=404, detail="Wine not found")
    for k, v in payload.items():
        if hasattr(w, k) and k != 'id': setattr(w, k, v)
    w.updated_at = datetime.utcnow(); db.commit(); db.refresh(w)
    return wine_to_dict(w)

@router.delete("/wines/{wine_id}")
async def delete_wine(wine_id: int, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    w = db.query(Wine).filter(Wine.id == wine_id).first()
    if not w: raise HTTPException(status_code=404, detail="Wine not found")
    db.delete(w); db.commit()
    return {"message": "Wine deleted successfully"}

# ============================================================================
# VINEYARDS CRUD
# ============================================================================
@router.get("/vineyards")
async def list_vineyards(authorization: Optional[str] = Header(None), db: Session = Depends(get_db), search: Optional[str] = Query(None), country: Optional[str] = Query(None), region: Optional[str] = Query(None), producer_type: Optional[str] = Query(None), farming_practice: Optional[str] = Query(None), quality_tier: Optional[str] = Query(None), needs_review: Optional[bool] = Query(None), sort_by: str = Query("created_at"), sort_dir: str = Query("desc"), page: int = Query(1, ge=1), per_page: int = Query(20, ge=1, le=100)):
    _get_current_user(authorization)
    q = db.query(Vineyard)
    if search: s = f"%{search}%"; q = q.filter(or_(Vineyard.name.ilike(s), Vineyard.short_name.ilike(s), Vineyard.region.ilike(s), Vineyard.winemaker.ilike(s), Vineyard.owner.ilike(s)))
    if country: q = q.filter(Vineyard.country == country)
    if region: q = q.filter(Vineyard.region.ilike(f"%{region}%"))
    if producer_type: q = q.filter(Vineyard.producer_type == producer_type)
    if farming_practice: q = q.filter(Vineyard.farming_practice == farming_practice)
    if quality_tier: q = q.filter(Vineyard.quality_tier == quality_tier)
    if needs_review is not None: q = q.filter(Vineyard.needs_review == needs_review)
    total = q.count()
    col = getattr(Vineyard, sort_by, Vineyard.created_at)
    q = q.order_by(col.asc() if sort_dir.lower() == "asc" else col.desc())
    return {"vineyards": [vineyard_to_dict(v) for v in q.offset((page-1)*per_page).limit(per_page).all()], "total": total, "page": page, "per_page": per_page, "total_pages": (total+per_page-1)//per_page}

@router.get("/vineyards/stats")
async def vineyard_stats(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    total = db.query(func.count(Vineyard.id)).scalar() or 0
    return {"total": total, "by_country": {r[0] or "unknown": r[1] for r in db.query(Vineyard.country, func.count(Vineyard.id)).group_by(Vineyard.country).order_by(func.count(Vineyard.id).desc()).limit(20).all()}, "by_producer_type": {r[0] or "unknown": r[1] for r in db.query(Vineyard.producer_type, func.count(Vineyard.id)).group_by(Vineyard.producer_type).all()}, "by_farming_practice": {r[0] or "unknown": r[1] for r in db.query(Vineyard.farming_practice, func.count(Vineyard.id)).group_by(Vineyard.farming_practice).all()}, "by_quality_tier": {r[0] or "unknown": r[1] for r in db.query(Vineyard.quality_tier, func.count(Vineyard.id)).group_by(Vineyard.quality_tier).all()}, "avg_hectares": float(db.query(func.avg(Vineyard.vineyard_hectares)).filter(Vineyard.vineyard_hectares.isnot(None)).scalar() or 0), "avg_production_bottles": float(db.query(func.avg(Vineyard.annual_production_bottles)).filter(Vineyard.annual_production_bottles.isnot(None)).scalar() or 0)}

@router.get("/vineyards/{vineyard_id}")
async def get_vineyard(vineyard_id: int, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    v = db.query(Vineyard).filter(Vineyard.id == vineyard_id).first()
    if not v: raise HTTPException(status_code=404, detail="Vineyard not found")
    return vineyard_to_dict(v)

@router.get("/vineyards/{vineyard_id}/wines")
async def get_vineyard_wines(vineyard_id: int, authorization: Optional[str] = Header(None), db: Session = Depends(get_db), page: int = Query(1, ge=1), per_page: int = Query(20, ge=1, le=100)):
    _get_current_user(authorization)
    v = db.query(Vineyard).filter(Vineyard.id == vineyard_id).first()
    if not v: raise HTTPException(status_code=404, detail="Vineyard not found")
    q = db.query(Wine).filter(Wine.vineyard_id == vineyard_id)
    total = q.count()
    return {"wines": [wine_to_dict(w) for w in q.order_by(Wine.name).offset((page-1)*per_page).limit(per_page).all()], "total": total, "page": page, "per_page": per_page, "total_pages": (total+per_page-1)//per_page}

@router.post("/vineyards")
async def create_vineyard(payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    if not payload.get("name"): raise HTTPException(status_code=400, detail="name is required")
    v = Vineyard(**{k: val for k, val in payload.items() if hasattr(Vineyard, k) and k != 'id'})
    db.add(v); db.commit(); db.refresh(v)
    return vineyard_to_dict(v)

@router.put("/vineyards/{vineyard_id}")
async def update_vineyard(vineyard_id: int, payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    v = db.query(Vineyard).filter(Vineyard.id == vineyard_id).first()
    if not v: raise HTTPException(status_code=404, detail="Vineyard not found")
    for k, val in payload.items():
        if hasattr(v, k) and k != 'id': setattr(v, k, val)
    v.updated_at = datetime.utcnow(); db.commit(); db.refresh(v)
    return vineyard_to_dict(v)

@router.delete("/vineyards/{vineyard_id}")
async def delete_vineyard(vineyard_id: int, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    v = db.query(Vineyard).filter(Vineyard.id == vineyard_id).first()
    if not v: raise HTTPException(status_code=404, detail="Vineyard not found")
    db.delete(v); db.commit()
    return {"message": "Vineyard deleted successfully"}

# ============================================================================
# DATA PROVENANCE
# ============================================================================
@router.get("/provenance/{entity_type}/{entity_id}")
async def get_provenance(entity_type: str, entity_id: int, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    if entity_type not in ("wine", "vineyard"): raise HTTPException(status_code=400, detail="entity_type must be 'wine' or 'vineyard'")
    records = db.query(DataProvenance).filter(DataProvenance.entity_type == entity_type, DataProvenance.entity_id == entity_id).order_by(DataProvenance.field_name, DataProvenance.confidence.desc()).all()
    grouped = {}
    for r in records:
        if r.field_name not in grouped: grouped[r.field_name] = []
        grouped[r.field_name].append(provenance_to_dict(r))
    return {"entity_type": entity_type, "entity_id": entity_id, "fields": grouped, "total_records": len(records)}

@router.get("/provenance/{entity_type}/{entity_id}/{field_name}")
async def get_field_provenance(entity_type: str, entity_id: int, field_name: str, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    records = db.query(DataProvenance).filter(DataProvenance.entity_type == entity_type, DataProvenance.entity_id == entity_id, DataProvenance.field_name == field_name).order_by(DataProvenance.confidence.desc()).all()
    return {"field_name": field_name, "sources": [provenance_to_dict(r) for r in records], "count": len(records)}

@router.post("/provenance")
async def create_provenance(payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    if not payload.get("entity_type") or not payload.get("entity_id") or not payload.get("field_name"): raise HTTPException(status_code=400, detail="entity_type, entity_id, and field_name required")
    if payload["entity_type"] not in ("wine", "vineyard"): raise HTTPException(status_code=400, detail="entity_type must be 'wine' or 'vineyard'")
    p = DataProvenance(entity_type=payload["entity_type"], entity_id=payload["entity_id"], field_name=payload["field_name"], value=payload.get("value"), source_url=payload.get("source_url"), source_name=payload.get("source_name"), source_type=payload.get("source_type"), source_excerpt=payload.get("source_excerpt"), content_hash=payload.get("content_hash"), confidence=payload.get("confidence"), scraped_at=datetime.fromisoformat(payload["scraped_at"]) if payload.get("scraped_at") else datetime.utcnow())
    db.add(p); db.commit(); db.refresh(p)
    return provenance_to_dict(p)

@router.delete("/provenance/{provenance_id}")
async def delete_provenance(provenance_id: int, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    p = db.query(DataProvenance).filter(DataProvenance.id == provenance_id).first()
    if not p: raise HTTPException(status_code=404, detail="Provenance record not found")
    db.delete(p); db.commit()
    return {"message": "Provenance record deleted"}

# ============================================================================
# ENRICHMENT
# ============================================================================
@router.post("/enrich/{wine_id}")
async def enrich_wine(wine_id: int, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    w = db.query(Wine).filter(Wine.id == wine_id).first()
    if not w: raise HTTPException(status_code=404, detail="Wine not found")
    task = Task(task_type="enrich", wine_id=w.id, wine_name=w.name, status="queued")
    db.add(task); db.commit(); db.refresh(task)
    cid = None
    if CELERY_AVAILABLE and celery_app:
        try:
            result = celery_app.send_task('tasks.enrich_wine', args=[wine_id]); cid = result.id
            task.celery_task_id = cid; task.status = "running"; task.started_at = datetime.utcnow(); db.commit()
        except Exception as e: task.status = "failed"; task.error = str(e); db.commit()
    else: task.status = "failed"; task.error = "Celery not available"; db.commit()
    return {"task_id": task.id, "celery_task_id": cid, "status": task.status, "message": "Enrichment task dispatched"}

@router.post("/enrich/batch")
async def enrich_batch(payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    wine_ids = payload.get("wine_ids"); filters = payload.get("filter")
    if not wine_ids and not filters: raise HTTPException(status_code=400, detail="Provide wine_ids or filter")
    q = db.query(Wine)
    if wine_ids: q = q.filter(Wine.id.in_(wine_ids))
    elif filters:
        if filters.get("wine_type"): q = q.filter(Wine.wine_type == filters["wine_type"])
        if filters.get("country"): q = q.filter(Wine.country == filters["country"])
        if filters.get("min_completeness"): q = q.filter(Wine.completeness <= filters["min_completeness"])
    wines = q.all(); tasks = []
    for wine in wines: t = Task(task_type="enrich", wine_id=wine.id, wine_name=wine.name, status="queued"); db.add(t); tasks.append(t)
    db.commit(); dispatched = 0
    if CELERY_AVAILABLE and celery_app:
        for t in tasks:
            try: r = celery_app.send_task('tasks.enrich_wine', args=[t.wine_id]); t.celery_task_id = r.id; t.status = "running"; t.started_at = datetime.utcnow(); dispatched += 1
            except Exception as e: t.status = "failed"; t.error = str(e)
        db.commit()
    return {"task_count": len(tasks), "dispatched": dispatched, "task_ids": [t.id for t in tasks], "status": "running" if dispatched > 0 else "failed", "message": f"Batch: {dispatched}/{len(tasks)} dispatched"}

# ============================================================================
# TASKS
# ============================================================================
@router.get("/tasks")
async def list_tasks(authorization: Optional[str] = Header(None), db: Session = Depends(get_db), status_filter: Optional[str] = Query(None), task_type: Optional[str] = Query(None), page: int = Query(1, ge=1), per_page: int = Query(20, ge=1, le=100)):
    _get_current_user(authorization)
    q = db.query(Task)
    if status_filter: q = q.filter(Task.status == status_filter)
    if task_type: q = q.filter(Task.task_type == task_type)
    total = q.count()
    tasks = q.order_by(Task.created_at.desc()).offset((page-1)*per_page).limit(per_page).all()
    if CELERY_AVAILABLE and celery_app:
        for t in tasks:
            if t.status == "running" and t.celery_task_id:
                try:
                    r = celery_app.AsyncResult(t.celery_task_id)
                    if r.ready():
                        if r.successful(): t.status = "completed"; t.completed_at = datetime.utcnow(); t.result = json.dumps(r.result) if r.result else None
                        else: t.status = "failed"; t.completed_at = datetime.utcnow(); t.error = str(r.result)
                except: pass
        db.commit()
    return {"tasks": [task_to_dict(t) for t in tasks], "total": total, "page": page, "per_page": per_page, "total_pages": (total+per_page-1)//per_page}

@router.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: int, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    t = db.query(Task).filter(Task.id == task_id).first()
    if not t: raise HTTPException(status_code=404, detail="Task not found")
    if t.status in ["completed", "failed"]: raise HTTPException(status_code=400, detail="Cannot cancel completed/failed task")
    if CELERY_AVAILABLE and celery_app and t.celery_task_id:
        try: celery_app.control.revoke(t.celery_task_id, terminate=True)
        except: pass
    t.status = "cancelled"; t.completed_at = datetime.utcnow(); db.commit()
    return {"message": "Task cancelled", "task_id": task_id}

@router.post("/tasks/clear")
async def clear_tasks(payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    ct = payload.get("type", "completed")
    if ct == "completed": db.query(Task).filter(Task.status == "completed").delete()
    elif ct == "failed": db.query(Task).filter(Task.status == "failed").delete()
    elif ct == "all": db.query(Task).filter(Task.status.in_(["completed", "failed", "cancelled"])).delete(synchronize_session=False)
    db.commit()
    return {"message": f"Cleared {ct} tasks"}

# ============================================================================
# REVIEW QUEUE
# ============================================================================
@router.get("/review")
async def list_review_queue(authorization: Optional[str] = Header(None), db: Session = Depends(get_db), page: int = Query(1, ge=1), per_page: int = Query(20, ge=1, le=100)):
    _get_current_user(authorization)
    q = db.query(Wine).filter(Wine.needs_review == True); total = q.count()
    return {"wines": [wine_to_dict(w) for w in q.order_by(Wine.updated_at.desc()).offset((page-1)*per_page).limit(per_page).all()], "total": total, "page": page, "per_page": per_page}

@router.post("/review/{wine_id}/approve")
async def approve_wine(wine_id: int, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    w = db.query(Wine).filter(Wine.id == wine_id).first()
    if not w: raise HTTPException(status_code=404, detail="Wine not found")
    w.needs_review = False; w.review_notes = None; w.updated_at = datetime.utcnow(); db.commit()
    return {"message": "Wine approved", "wine_id": wine_id}

@router.post("/review/{wine_id}/reject")
async def reject_wine(wine_id: int, payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    w = db.query(Wine).filter(Wine.id == wine_id).first()
    if not w: raise HTTPException(status_code=404, detail="Wine not found")
    for f in ['sweetness','acidity','tannin','body','complexity','flavor_intensity','finish','data_method','source_list']: setattr(w, f, None)
    w.source_count = 0; w.avg_confidence = None; w.completeness = None; w.needs_review = False; w.review_notes = payload.get("reason", "Rejected"); w.updated_at = datetime.utcnow(); db.commit()
    return {"message": "Wine rejected", "wine_id": wine_id}

@router.post("/review/flag/{wine_id}")
async def flag_for_review(wine_id: int, payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    w = db.query(Wine).filter(Wine.id == wine_id).first()
    if not w: raise HTTPException(status_code=404, detail="Wine not found")
    w.needs_review = True; w.review_notes = payload.get("reason", "Flagged"); w.updated_at = datetime.utcnow(); db.commit()
    return {"message": "Wine flagged", "wine_id": wine_id}

# ============================================================================
# CRAWLER CONTROL
# ============================================================================
@router.get("/crawler/status")
async def crawler_status(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    last_crawl = db.query(Task).filter(Task.task_type == "crawl").order_by(Task.created_at.desc()).first()
    searxng_status = "unknown"
    try:
        with urllib.request.urlopen(urllib.request.Request(os.getenv("SEARXNG_URL", "http://searxng:8080")), timeout=3): searxng_status = "connected"
    except: searxng_status = "unreachable"
    return {"active_crawls": db.query(func.count(Task.id)).filter(Task.task_type == "crawl", Task.status == "running").scalar() or 0, "total_crawls": db.query(func.count(Task.id)).filter(Task.task_type == "crawl").scalar() or 0, "completed_crawls": db.query(func.count(Task.id)).filter(Task.task_type == "crawl", Task.status == "completed").scalar() or 0, "last_crawl": task_to_dict(last_crawl) if last_crawl else None, "searxng_status": searxng_status}

@router.post("/crawler/start")
async def start_crawler(payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    query_str = payload.get("query", ""); target = payload.get("target", "all_unenriched")
    if target == "custom" and not query_str: raise HTTPException(status_code=400, detail="Query required")
    task = Task(task_type="crawl", wine_name=query_str or target, status="queued"); db.add(task); db.commit(); db.refresh(task)
    if CELERY_AVAILABLE and celery_app:
        try:
            if target == "all_unenriched": result = celery_app.send_task('tasks.enrich_pending_wines')
            else:
                for wine in db.query(Wine).filter(Wine.name.ilike(f"%{query_str}%")).all(): celery_app.send_task('tasks.enrich_wine', args=[wine.id])
                result = type('obj', (object,), {'id': f'batch-{task.id}'})()
            task.celery_task_id = result.id; task.status = "running"; task.started_at = datetime.utcnow(); db.commit()
        except Exception as e: task.status = "failed"; task.error = str(e); db.commit()
    return {"task_id": task.id, "status": task.status, "message": "Crawler started"}

@router.post("/crawler/stop")
async def stop_crawler(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    running = db.query(Task).filter(Task.task_type == "crawl", Task.status == "running").all(); stopped = 0
    for t in running:
        if CELERY_AVAILABLE and celery_app and t.celery_task_id:
            try: celery_app.control.revoke(t.celery_task_id, terminate=True)
            except: pass
        t.status = "cancelled"; t.completed_at = datetime.utcnow(); stopped += 1
    db.commit()
    return {"message": f"Stopped {stopped} crawl tasks", "stopped": stopped}

# ============================================================================
# AI CHAT
# ============================================================================
def _query_ollama(prompt, system_prompt):
    try:
        req = urllib.request.Request(f"{OLLAMA_URL}/api/generate", data=json.dumps({"model": "llama3.1:8b-instruct-q4_K_M", "prompt": f"{system_prompt}\n\n{prompt}", "stream": False}).encode(), headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=60) as resp: return json.loads(resp.read().decode()).get("response", "").strip()
    except Exception as e: return f"Error connecting to Ollama: {e}"

@router.post("/chat")
async def chat(payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    message = payload.get("message", "").strip(); history = payload.get("history", [])
    if not message: raise HTTPException(status_code=400, detail="Message required")
    ctx = "".join(f"{m.get('role','user')}: {m.get('content','')}\n" for m in history[-5:])
    tw = db.query(func.count(Wine.id)).scalar() or 0; tv = db.query(func.count(Vineyard.id)).scalar() or 0
    en = db.query(func.count(Wine.id)).filter(Wine.data_method.isnot(None)).scalar() or 0
    sp = f"You are a wine database assistant for vinosomm.ai.\nDatabase: {tw} wines, {en} enriched, {tv} vineyards.\nBe concise, knowledgeable, helpful."
    resp = _query_ollama(ctx + f"user: {message}\nassistant:", sp)
    parsed = None
    try: parsed = json.loads(resp)
    except: pass
    return {"response": resp, "parsed": parsed}

# ============================================================================
# STATISTICS
# ============================================================================
@router.get("/stats")
async def get_stats(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    total = db.query(func.count(Wine.id)).scalar() or 0
    enriched = db.query(func.count(Wine.id)).filter(Wine.data_method.isnot(None)).scalar() or 0
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    timeline = [{"date": str(r[0]), "count": r[1]} for r in db.query(func.date(Wine.created_at), func.count(Wine.id)).filter(Wine.created_at >= thirty_days_ago).group_by(func.date(Wine.created_at)).order_by(func.date(Wine.created_at)).all()]
    return {
        "total": total, "enriched": enriched, "unenriched": total - enriched,
        "by_type": {r[0] or "unknown": r[1] for r in db.query(Wine.wine_type, func.count(Wine.id)).group_by(Wine.wine_type).all()},
        "by_country": {r[0] or "unknown": r[1] for r in db.query(Wine.country, func.count(Wine.id)).group_by(Wine.country).order_by(func.count(Wine.id).desc()).limit(15).all()},
        "completeness_distribution": {"0-20%": db.query(func.count(Wine.id)).filter(Wine.completeness < 20).scalar() or 0, "20-40%": db.query(func.count(Wine.id)).filter(Wine.completeness >= 20, Wine.completeness < 40).scalar() or 0, "40-60%": db.query(func.count(Wine.id)).filter(Wine.completeness >= 40, Wine.completeness < 60).scalar() or 0, "60-80%": db.query(func.count(Wine.id)).filter(Wine.completeness >= 60, Wine.completeness < 80).scalar() or 0, "80-100%": db.query(func.count(Wine.id)).filter(Wine.completeness >= 80).scalar() or 0},
        "avg_scores": {k: float(db.query(func.avg(getattr(Wine, k))).filter(getattr(Wine, k).isnot(None)).scalar() or 0) for k in ['sweetness','acidity','tannin','body','complexity']},
        "timeline": timeline,
        "by_method": {r[0] or "none": r[1] for r in db.query(Wine.data_method, func.count(Wine.id)).group_by(Wine.data_method).all()},
    }

# ============================================================================
# SETTINGS
# ============================================================================
DEFAULT_SETTINGS = {"ollama_model": "llama3.1:8b-instruct-q4_K_M", "ollama_url": "http://host.docker.internal:11434", "searxng_url": "http://searxng:8080", "enrichment_batch_size": "20", "enrichment_schedule": "daily_2am", "auto_review_threshold": "0.5", "max_concurrent_tasks": "4"}

@router.get("/settings")
async def get_settings(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    db_map = {s.key: s.value for s in db.query(Setting).all()}
    return {"settings": {k: db_map.get(k, v) for k, v in DEFAULT_SETTINGS.items()}}

@router.put("/settings")
async def update_settings(payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    settings = payload.get("settings", payload); updated = []
    for k, v in settings.items():
        if k in DEFAULT_SETTINGS:
            existing = db.query(Setting).filter(Setting.key == k).first()
            if existing: existing.value = str(v); existing.updated_at = datetime.utcnow()
            else: db.add(Setting(key=k, value=str(v)))
            updated.append(k)
    db.commit()
    return {"message": f"Updated {len(updated)} settings", "updated": updated}

# ============================================================================
# SYSTEM HEALTH
# ============================================================================
@router.get("/system/health")
async def system_health(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    health = {"status": "healthy", "timestamp": datetime.utcnow().isoformat(), "services": {}}
    try: db.execute(text("SELECT 1")); health["services"]["postgres"] = {"status": "connected"}
    except Exception as e: health["services"]["postgres"] = {"status": "error", "error": str(e)}; health["status"] = "degraded"
    try:
        import redis; redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=2).ping()
        health["services"]["redis"] = {"status": "connected"}
    except Exception as e: health["services"]["redis"] = {"status": "unavailable", "error": str(e)}
    try:
        with urllib.request.urlopen(urllib.request.Request(f"{OLLAMA_URL}/api/tags"), timeout=5) as resp:
            health["services"]["ollama"] = {"status": "connected", "models": len(json.loads(resp.read().decode()).get("models", []))}
    except Exception as e: health["services"]["ollama"] = {"status": "unavailable", "error": str(e)}
    try:
        with urllib.request.urlopen(urllib.request.Request(os.getenv("SEARXNG_URL", "http://searxng:8080")), timeout=3): health["services"]["searxng"] = {"status": "connected"}
    except Exception as e: health["services"]["searxng"] = {"status": "unavailable", "error": str(e)}
    if CELERY_AVAILABLE and celery_app:
        try: active = celery_app.control.inspect(timeout=3).active(); health["services"]["celery"] = {"status": "connected", "workers": len(active) if active else 0}
        except Exception as e: health["services"]["celery"] = {"status": "unavailable", "error": str(e)}
    else: health["services"]["celery"] = {"status": "unavailable", "error": "Celery not imported"}
    return health