"""Admin API for vinosomm.ai - Wine Intelligence Application
Provides authentication, CRUD operations, enrichment tasks,
dashboard analytics, crawler control, review queue, and AI chat.
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

# Create router
router = APIRouter(prefix="/admin/api", tags=["admin"])

# Celery integration
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


# Create tables (safe - won't fail if they exist)
try:
    Base.metadata.create_all(bind=engine)
except Exception:
    pass


# ============================================================================
# JWT Auth (stdlib only)
# ============================================================================
def _encode_jwt(payload: Dict[str, Any]) -> str:
    header = base64.urlsafe_b64encode(
        json.dumps({"alg": "HS256", "typ": "JWT"}).encode()
    ).rstrip(b"=").decode()
    payload_json = json.dumps(payload)
    body = base64.urlsafe_b64encode(payload_json.encode()).rstrip(b"=").decode()
    message = f"{header}.{body}".encode()
    signature = base64.urlsafe_b64encode(
        hmac.new(JWT_SECRET.encode(), message, hashlib.sha256).digest()
    ).rstrip(b"=").decode()
    return f"{header}.{body}.{signature}"


def _decode_jwt(token: str) -> Optional[Dict[str, Any]]:
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        header, body, signature = parts
        message = f"{header}.{body}".encode()
        expected_sig = base64.urlsafe_b64encode(
            hmac.new(JWT_SECRET.encode(), message, hashlib.sha256).digest()
        ).rstrip(b"=").decode()
        if not hmac.compare_digest(signature, expected_sig):
            return None
        padding = "=" * (4 - len(body) % 4)
        payload_json = base64.urlsafe_b64decode(body + padding)
        payload = json.loads(payload_json)
        if payload.get("exp", 0) < time.time():
            return None
        return payload
    except Exception:
        return None


def _get_current_user(authorization: Optional[str] = Header(None)) -> str:
    if not authorization:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")
    try:
        scheme, token = authorization.split(" ")
        if scheme.lower() != "bearer":
            raise ValueError("Invalid scheme")
    except ValueError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token format")
    payload = _decode_jwt(token)
    if not payload or payload.get("sub") != ADMIN_USER:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    return payload.get("sub")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ============================================================================
# Serializers
# ============================================================================
def wine_to_dict(wine: Wine) -> Dict[str, Any]:
    return {
        "id": wine.id,
        "name": wine.name,
        "producer": wine.producer,
        "vintage": wine.vintage,
        "region": wine.region,
        "country": wine.country,
        "grape_variety": wine.grape_variety,
        "wine_type": wine.wine_type,
        "classification": wine.classification,
        "sweetness": wine.sweetness,
        "acidity": wine.acidity,
        "tannin": wine.tannin,
        "body": wine.body,
        "alcohol_warmth": wine.alcohol_warmth,
        "effervescence": wine.effervescence,
        "flavor_intensity": wine.flavor_intensity,
        "finish": wine.finish,
        "complexity": wine.complexity,
        "fruit_character": wine.fruit_character,
        "secondary_aromas": wine.secondary_aromas,
        "tertiary_notes": wine.tertiary_notes,
        "residual_sugar": wine.residual_sugar,
        "alcohol": wine.alcohol,
        "price": wine.price,
        "currency": wine.currency,
        "rating": wine.rating,
        "data_method": wine.data_method,
        "source_list": wine.source_list,
        "source_count": wine.source_count,
        "provenance_notes": wine.provenance_notes,
        "avg_confidence": wine.avg_confidence,
        "completeness": wine.completeness,
        "needs_review": wine.needs_review,
        "review_notes": wine.review_notes,
        "created_at": wine.created_at.isoformat() if wine.created_at else None,
        "updated_at": wine.updated_at.isoformat() if wine.updated_at else None,
    }


def task_to_dict(task: Task) -> Dict[str, Any]:
    return {
        "id": task.id,
        "task_type": task.task_type,
        "wine_id": task.wine_id,
        "wine_name": task.wine_name,
        "status": task.status,
        "celery_task_id": task.celery_task_id,
        "started_at": task.started_at.isoformat() if task.started_at else None,
        "completed_at": task.completed_at.isoformat() if task.completed_at else None,
        "result": task.result,
        "error": task.error,
        "created_at": task.created_at.isoformat() if task.created_at else None,
    }


# ============================================================================
# AUTH Endpoints
# ============================================================================
@router.post("/auth/login")
async def login(payload: Dict[str, str]):
    username = payload.get("username")
    password = payload.get("password")
    if not username or not password:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing credentials")
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    if username != ADMIN_USER or password_hash != ADMIN_PASS_HASH:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    exp = int(time.time()) + 86400
    token = _encode_jwt({"sub": ADMIN_USER, "iat": int(time.time()), "exp": exp})
    return {"access_token": token, "token_type": "bearer", "expires_in": 86400}


@router.get("/auth/me")
async def get_current_user_info(authorization: Optional[str] = Header(None)):
    user = _get_current_user(authorization)
    return {"username": user, "role": "admin"}


# ============================================================================
# DASHBOARD
# ============================================================================
@router.get("/dashboard")
async def get_dashboard(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    total_wines = db.query(func.count(Wine.id)).scalar() or 0
    wines_by_type = db.query(Wine.wine_type, func.count(Wine.id)).group_by(Wine.wine_type).all()
    wines_by_type_dict = {row[0] or "unknown": row[1] for row in wines_by_type}
    wines_by_country = db.query(Wine.country, func.count(Wine.id)).group_by(Wine.country).order_by(func.count(Wine.id).desc()).limit(10).all()
    wines_by_country_dict = {row[0] or "unknown": row[1] for row in wines_by_country}
    recent_wines = db.query(Wine).order_by(Wine.created_at.desc()).limit(5).all()
    recent_wines_list = [wine_to_dict(w) for w in recent_wines]
    avg_completeness = db.query(func.avg(Wine.completeness)).scalar() or 0
    total_with_rating = db.query(func.count(Wine.id)).filter(Wine.rating.isnot(None)).scalar() or 0
    total_with_source = db.query(func.count(Wine.id)).filter(Wine.source_list.isnot(None)).scalar() or 0
    now = datetime.utcnow()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = today_start - timedelta(days=now.weekday())
    month_start = today_start.replace(day=1)
    wines_added_today = db.query(func.count(Wine.id)).filter(Wine.created_at >= today_start).scalar() or 0
    wines_added_this_week = db.query(func.count(Wine.id)).filter(Wine.created_at >= week_start).scalar() or 0
    wines_added_this_month = db.query(func.count(Wine.id)).filter(Wine.created_at >= month_start).scalar() or 0
    # Task stats
    tasks_queued = db.query(func.count(Task.id)).filter(Task.status == "queued").scalar() or 0
    tasks_running = db.query(func.count(Task.id)).filter(Task.status == "running").scalar() or 0
    tasks_completed = db.query(func.count(Task.id)).filter(Task.status == "completed").scalar() or 0
    tasks_failed = db.query(func.count(Task.id)).filter(Task.status == "failed").scalar() or 0
    # Review queue count
    needs_review = db.query(func.count(Wine.id)).filter(Wine.needs_review == True).scalar() or 0
    return {
        "total_wines": total_wines,
        "wines_by_type": wines_by_type_dict,
        "wines_by_country": wines_by_country_dict,
        "recent_wines": recent_wines_list,
        "avg_completeness": float(avg_completeness),
        "enrichment_stats": {"total_with_rating": total_with_rating, "total_with_source": total_with_source},
        "wines_added_today": wines_added_today,
        "wines_added_this_week": wines_added_this_week,
        "wines_added_this_month": wines_added_this_month,
        "task_stats": {"queued": tasks_queued, "running": tasks_running, "completed": tasks_completed, "failed": tasks_failed},
        "needs_review": needs_review,
    }


# ============================================================================
# WINES CRUD
# ============================================================================
@router.get("/wines")
async def list_wines(
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db),
    search: Optional[str] = Query(None),
    wine_type: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    min_completeness: Optional[float] = Query(None),
    max_completeness: Optional[float] = Query(None),
    needs_review: Optional[bool] = Query(None),
    sort_by: str = Query("created_at"),
    sort_dir: str = Query("desc"),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    _get_current_user(authorization)
    query = db.query(Wine)
    if search:
        search_term = f"%{search}%"
        query = query.filter(or_(Wine.name.ilike(search_term), Wine.producer.ilike(search_term), Wine.region.ilike(search_term)))
    if wine_type:
        query = query.filter(Wine.wine_type == wine_type)
    if country:
        query = query.filter(Wine.country == country)
    if min_completeness is not None:
        query = query.filter(Wine.completeness >= min_completeness)
    if max_completeness is not None:
        query = query.filter(Wine.completeness <= max_completeness)
    if needs_review is not None:
        query = query.filter(Wine.needs_review == needs_review)
    total = query.count()
    sort_column = getattr(Wine, sort_by, Wine.created_at)
    if sort_dir.lower() == "asc":
        query = query.order_by(sort_column.asc())
    else:
        query = query.order_by(sort_column.desc())
    wines = query.offset((page - 1) * per_page).limit(per_page).all()
    return {
        "wines": [wine_to_dict(w) for w in wines],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
    }


@router.get("/wines/{wine_id}")
async def get_wine(wine_id: int, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    wine = db.query(Wine).filter(Wine.id == wine_id).first()
    if not wine:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Wine not found")
    return wine_to_dict(wine)


@router.post("/wines")
async def create_wine(payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    name = payload.get("name")
    wine_type = payload.get("wine_type")
    if not name or not wine_type:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="name and wine_type are required")
    wine = Wine(
        name=name, wine_type=wine_type,
        producer=payload.get("producer"), vintage=payload.get("vintage"),
        region=payload.get("region"), country=payload.get("country"),
        grape_variety=payload.get("grape_variety"), classification=payload.get("classification"),
        sweetness=payload.get("sweetness"), acidity=payload.get("acidity"),
        tannin=payload.get("tannin"), body=payload.get("body"),
        alcohol_warmth=payload.get("alcohol_warmth"), effervescence=payload.get("effervescence"),
        flavor_intensity=payload.get("flavor_intensity"), finish=payload.get("finish"),
        complexity=payload.get("complexity"), fruit_character=payload.get("fruit_character"),
        secondary_aromas=payload.get("secondary_aromas"), tertiary_notes=payload.get("tertiary_notes"),
        residual_sugar=payload.get("residual_sugar"), alcohol=payload.get("alcohol"),
        price=payload.get("price"), currency=payload.get("currency"),
        rating=payload.get("rating"), data_method=payload.get("data_method"),
        source_list=payload.get("source_list"), source_count=payload.get("source_count", 0),
        provenance_notes=payload.get("provenance_notes"), avg_confidence=payload.get("avg_confidence"),
        completeness=payload.get("completeness"),
    )
    db.add(wine)
    db.commit()
    db.refresh(wine)
    return wine_to_dict(wine)


@router.put("/wines/{wine_id}")
async def update_wine(wine_id: int, payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    wine = db.query(Wine).filter(Wine.id == wine_id).first()
    if not wine:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Wine not found")
    for key, value in payload.items():
        if hasattr(wine, key) and value is not None:
            setattr(wine, key, value)
    wine.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(wine)
    return wine_to_dict(wine)


@router.delete("/wines/{wine_id}")
async def delete_wine(wine_id: int, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    wine = db.query(Wine).filter(Wine.id == wine_id).first()
    if not wine:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Wine not found")
    db.delete(wine)
    db.commit()
    return {"message": "Wine deleted successfully"}


# ============================================================================
# ENRICHMENT Endpoints (with real Celery integration)
# ============================================================================
@router.post("/enrich/{wine_id}")
async def enrich_wine(wine_id: int, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    wine = db.query(Wine).filter(Wine.id == wine_id).first()
    if not wine:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Wine not found")
    task = Task(task_type="enrich", wine_id=wine.id, wine_name=wine.name, status="queued")
    db.add(task)
    db.commit()
    db.refresh(task)
    # Trigger real Celery task
    celery_task_id = None
    if CELERY_AVAILABLE and celery_app:
        try:
            result = celery_app.send_task('tasks.enrich_wine', args=[wine_id])
            celery_task_id = result.id
            task.celery_task_id = celery_task_id
            task.status = "running"
            task.started_at = datetime.utcnow()
            db.commit()
        except Exception as e:
            task.status = "failed"
            task.error = f"Failed to dispatch Celery task: {str(e)}"
            db.commit()
    else:
        task.status = "failed"
        task.error = "Celery not available"
        db.commit()
    return {"task_id": task.id, "celery_task_id": celery_task_id, "status": task.status, "message": "Enrichment task dispatched"}


@router.post("/enrich/batch")
async def enrich_batch(payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    wine_ids = payload.get("wine_ids")
    filters = payload.get("filter")
    if not wine_ids and not filters:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Provide wine_ids or filter")
    query = db.query(Wine)
    if wine_ids:
        query = query.filter(Wine.id.in_(wine_ids))
    elif filters:
        if filters.get("wine_type"):
            query = query.filter(Wine.wine_type == filters["wine_type"])
        if filters.get("country"):
            query = query.filter(Wine.country == filters["country"])
        if filters.get("min_completeness"):
            query = query.filter(Wine.completeness <= filters["min_completeness"])
    wines = query.all()
    tasks = []
    for wine in wines:
        task = Task(task_type="enrich", wine_id=wine.id, wine_name=wine.name, status="queued")
        db.add(task)
        tasks.append(task)
    db.commit()
    # Dispatch Celery tasks
    dispatched = 0
    if CELERY_AVAILABLE and celery_app:
        for t in tasks:
            try:
                result = celery_app.send_task('tasks.enrich_wine', args=[t.wine_id])
                t.celery_task_id = result.id
                t.status = "running"
                t.started_at = datetime.utcnow()
                dispatched += 1
            except Exception as e:
                t.status = "failed"
                t.error = str(e)
        db.commit()
    return {
        "task_count": len(tasks),
        "dispatched": dispatched,
        "task_ids": [t.id for t in tasks],
        "status": "running" if dispatched > 0 else "failed",
        "message": f"Batch enrichment: {dispatched}/{len(tasks)} dispatched",
    }


# ============================================================================
# TASKS
# ============================================================================
@router.get("/tasks")
async def list_tasks(
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db),
    status_filter: Optional[str] = Query(None),
    task_type: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    _get_current_user(authorization)
    query = db.query(Task)
    if status_filter:
        query = query.filter(Task.status == status_filter)
    if task_type:
        query = query.filter(Task.task_type == task_type)
    total = query.count()
    tasks = query.order_by(Task.created_at.desc()).offset((page - 1) * per_page).limit(per_page).all()
    # Check and update Celery task statuses
    if CELERY_AVAILABLE and celery_app:
        for t in tasks:
            if t.status == "running" and t.celery_task_id:
                try:
                    result = celery_app.AsyncResult(t.celery_task_id)
                    if result.ready():
                        if result.successful():
                            t.status = "completed"
                            t.completed_at = datetime.utcnow()
                            t.result = json.dumps(result.result) if result.result else None
                        else:
                            t.status = "failed"
                            t.completed_at = datetime.utcnow()
                            t.error = str(result.result)
                except Exception:
                    pass
        db.commit()
    return {
        "tasks": [task_to_dict(t) for t in tasks],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
    }


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: int, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
    if task.status in ["completed", "failed"]:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot cancel a completed or failed task")
    # Revoke Celery task if possible
    if CELERY_AVAILABLE and celery_app and task.celery_task_id:
        try:
            celery_app.control.revoke(task.celery_task_id, terminate=True)
        except Exception:
            pass
    task.status = "cancelled"
    task.completed_at = datetime.utcnow()
    db.commit()
    return {"message": "Task cancelled successfully", "task_id": task_id}


@router.post("/tasks/clear")
async def clear_tasks(payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    clear_type = payload.get("type", "completed")
    if clear_type == "completed":
        db.query(Task).filter(Task.status == "completed").delete()
    elif clear_type == "failed":
        db.query(Task).filter(Task.status == "failed").delete()
    elif clear_type == "all":
        db.query(Task).filter(Task.status.in_(["completed", "failed", "cancelled"])).delete(synchronize_session=False)
    db.commit()
    return {"message": f"Cleared {clear_type} tasks"}


# ============================================================================
# REVIEW QUEUE
# ============================================================================
@router.get("/review")
async def list_review_queue(
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    _get_current_user(authorization)
    query = db.query(Wine).filter(Wine.needs_review == True)
    total = query.count()
    wines = query.order_by(Wine.updated_at.desc()).offset((page - 1) * per_page).limit(per_page).all()
    return {
        "wines": [wine_to_dict(w) for w in wines],
        "total": total,
        "page": page,
        "per_page": per_page,
    }


@router.post("/review/{wine_id}/approve")
async def approve_wine(wine_id: int, authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    wine = db.query(Wine).filter(Wine.id == wine_id).first()
    if not wine:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Wine not found")
    wine.needs_review = False
    wine.review_notes = None
    wine.updated_at = datetime.utcnow()
    db.commit()
    return {"message": "Wine approved", "wine_id": wine_id}


@router.post("/review/{wine_id}/reject")
async def reject_wine(wine_id: int, payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    wine = db.query(Wine).filter(Wine.id == wine_id).first()
    if not wine:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Wine not found")
    reason = payload.get("reason", "Rejected by admin")
    # Reset enrichment data
    wine.sweetness = None
    wine.acidity = None
    wine.tannin = None
    wine.body = None
    wine.complexity = None
    wine.flavor_intensity = None
    wine.finish = None
    wine.data_method = None
    wine.source_list = None
    wine.source_count = 0
    wine.avg_confidence = None
    wine.completeness = None
    wine.needs_review = False
    wine.review_notes = reason
    wine.updated_at = datetime.utcnow()
    db.commit()
    return {"message": "Wine rejected and data cleared", "wine_id": wine_id}


@router.post("/review/flag/{wine_id}")
async def flag_for_review(wine_id: int, payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    wine = db.query(Wine).filter(Wine.id == wine_id).first()
    if not wine:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Wine not found")
    wine.needs_review = True
    wine.review_notes = payload.get("reason", "Flagged for review")
    wine.updated_at = datetime.utcnow()
    db.commit()
    return {"message": "Wine flagged for review", "wine_id": wine_id}


# ============================================================================
# CRAWLER CONTROL
# ============================================================================
@router.get("/crawler/status")
async def crawler_status(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    # Check active crawl tasks
    active_crawls = db.query(func.count(Task.id)).filter(Task.task_type == "crawl", Task.status == "running").scalar() or 0
    total_crawls = db.query(func.count(Task.id)).filter(Task.task_type == "crawl").scalar() or 0
    completed_crawls = db.query(func.count(Task.id)).filter(Task.task_type == "crawl", Task.status == "completed").scalar() or 0
    last_crawl = db.query(Task).filter(Task.task_type == "crawl").order_by(Task.created_at.desc()).first()
    # Check SearXNG
    searxng_status = "unknown"
    try:
        req = urllib.request.Request(os.getenv("SEARXNG_URL", "http://searxng:8080"))
        with urllib.request.urlopen(req, timeout=3) as resp:
            searxng_status = "connected"
    except Exception:
        searxng_status = "unreachable"
    return {
        "active_crawls": active_crawls,
        "total_crawls": total_crawls,
        "completed_crawls": completed_crawls,
        "last_crawl": task_to_dict(last_crawl) if last_crawl else None,
        "searxng_status": searxng_status,
    }


@router.post("/crawler/start")
async def start_crawler(payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    query_str = payload.get("query", "")
    target = payload.get("target", "all_unenriched")
    if target == "custom" and not query_str:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Query required for custom crawl")
    task = Task(task_type="crawl", wine_name=query_str or target, status="queued")
    db.add(task)
    db.commit()
    db.refresh(task)
    # Dispatch
    if CELERY_AVAILABLE and celery_app:
        try:
            if target == "all_unenriched":
                result = celery_app.send_task('tasks.enrich_pending_wines')
            else:
                # For custom queries, enrich wines matching the search
                wines = db.query(Wine).filter(Wine.name.ilike(f"%{query_str}%")).all()
                for wine in wines:
                    celery_app.send_task('tasks.enrich_wine', args=[wine.id])
                result = type('obj', (object,), {'id': f'batch-{task.id}'})()
            task.celery_task_id = result.id
            task.status = "running"
            task.started_at = datetime.utcnow()
            db.commit()
        except Exception as e:
            task.status = "failed"
            task.error = str(e)
            db.commit()
    return {"task_id": task.id, "status": task.status, "message": "Crawler started"}


@router.post("/crawler/stop")
async def stop_crawler(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    running = db.query(Task).filter(Task.task_type == "crawl", Task.status == "running").all()
    stopped = 0
    for t in running:
        if CELERY_AVAILABLE and celery_app and t.celery_task_id:
            try:
                celery_app.control.revoke(t.celery_task_id, terminate=True)
            except Exception:
                pass
        t.status = "cancelled"
        t.completed_at = datetime.utcnow()
        stopped += 1
    db.commit()
    return {"message": f"Stopped {stopped} crawl tasks", "stopped": stopped}


# ============================================================================
# AI CHAT
# ============================================================================
def _query_ollama(prompt: str, system_prompt: str) -> str:
    try:
        request_data = {
            "model": "llama3.1:8b-instruct-q4_K_M",
            "prompt": f"{system_prompt}\n\n{prompt}",
            "stream": False,
        }
        req = urllib.request.Request(
            f"{OLLAMA_URL}/api/generate",
            data=json.dumps(request_data).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=60) as response:
            data = json.loads(response.read().decode())
            return data.get("response", "").strip()
    except (urllib.error.URLError, urllib.error.HTTPError, Exception) as e:
        return f"Error connecting to Ollama: {str(e)}"


@router.post("/chat")
async def chat(payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    message = payload.get("message", "").strip()
    history = payload.get("history", [])
    if not message:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Message is required")
    # Build conversation context
    context = ""
    for msg in history[-5:]:
        context += f"{msg.get('role', 'user')}: {msg.get('content', '')}\n"
    # Get some wine stats for context
    total_wines = db.query(func.count(Wine.id)).scalar() or 0
    enriched = db.query(func.count(Wine.id)).filter(Wine.data_method.isnot(None)).scalar() or 0
    system_prompt = f"""You are a wine database assistant for vinosomm.ai.
The database currently has {total_wines} wines, {enriched} enriched.

Your capabilities:
1. Answer questions about wines in the database
2. Provide wine knowledge and recommendations
3. Help with data quality and enrichment decisions

Be concise, knowledgeable, and helpful. If asked about specific wines, note that you can see aggregate stats but the admin panel shows individual wines."""

    full_prompt = context + f"user: {message}\nassistant:"
    response = _query_ollama(full_prompt, system_prompt)
    parsed_response = None
    try:
        parsed_response = json.loads(response)
    except json.JSONDecodeError:
        pass
    return {"response": response, "parsed": parsed_response}


# ============================================================================
# STATISTICS & ANALYTICS
# ============================================================================
@router.get("/stats")
async def get_stats(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    total = db.query(func.count(Wine.id)).scalar() or 0
    enriched = db.query(func.count(Wine.id)).filter(Wine.data_method.isnot(None)).scalar() or 0
    # By type
    by_type = db.query(Wine.wine_type, func.count(Wine.id)).group_by(Wine.wine_type).all()
    # By country (top 15)
    by_country = db.query(Wine.country, func.count(Wine.id)).group_by(Wine.country).order_by(func.count(Wine.id).desc()).limit(15).all()
    # Completeness distribution
    comp_ranges = {
        "0-20%": db.query(func.count(Wine.id)).filter(Wine.completeness < 20).scalar() or 0,
        "20-40%": db.query(func.count(Wine.id)).filter(Wine.completeness >= 20, Wine.completeness < 40).scalar() or 0,
        "40-60%": db.query(func.count(Wine.id)).filter(Wine.completeness >= 40, Wine.completeness < 60).scalar() or 0,
        "60-80%": db.query(func.count(Wine.id)).filter(Wine.completeness >= 60, Wine.completeness < 80).scalar() or 0,
        "80-100%": db.query(func.count(Wine.id)).filter(Wine.completeness >= 80).scalar() or 0,
    }
    # Average scores
    avg_scores = {
        "sweetness": float(db.query(func.avg(Wine.sweetness)).filter(Wine.sweetness.isnot(None)).scalar() or 0),
        "acidity": float(db.query(func.avg(Wine.acidity)).filter(Wine.acidity.isnot(None)).scalar() or 0),
        "tannin": float(db.query(func.avg(Wine.tannin)).filter(Wine.tannin.isnot(None)).scalar() or 0),
        "body": float(db.query(func.avg(Wine.body)).filter(Wine.body.isnot(None)).scalar() or 0),
        "complexity": float(db.query(func.avg(Wine.complexity)).filter(Wine.complexity.isnot(None)).scalar() or 0),
    }
    # Timeline (wines added per day, last 30 days)
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    timeline_raw = db.query(
        func.date(Wine.created_at),
        func.count(Wine.id)
    ).filter(Wine.created_at >= thirty_days_ago).group_by(func.date(Wine.created_at)).order_by(func.date(Wine.created_at)).all()
    timeline = [{"date": str(row[0]), "count": row[1]} for row in timeline_raw]
    # Data methods
    by_method = db.query(Wine.data_method, func.count(Wine.id)).group_by(Wine.data_method).all()
    return {
        "total": total,
        "enriched": enriched,
        "unenriched": total - enriched,
        "by_type": {row[0] or "unknown": row[1] for row in by_type},
        "by_country": {row[0] or "unknown": row[1] for row in by_country},
        "completeness_distribution": comp_ranges,
        "avg_scores": avg_scores,
        "timeline": timeline,
        "by_method": {row[0] or "none": row[1] for row in by_method},
    }


# ============================================================================
# SETTINGS
# ============================================================================
DEFAULT_SETTINGS = {
    "ollama_model": "llama3.1:8b-instruct-q4_K_M",
    "ollama_url": "http://host.docker.internal:11434",
    "searxng_url": "http://searxng:8080",
    "enrichment_batch_size": "20",
    "enrichment_schedule": "daily_2am",
    "auto_review_threshold": "0.5",
    "max_concurrent_tasks": "4",
}


@router.get("/settings")
async def get_settings(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    settings = {}
    db_settings = db.query(Setting).all()
    db_map = {s.key: s.value for s in db_settings}
    for key, default in DEFAULT_SETTINGS.items():
        settings[key] = db_map.get(key, default)
    return {"settings": settings}


@router.put("/settings")
async def update_settings(payload: Dict[str, Any], authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    settings = payload.get("settings", payload)
    updated = []
    for key, value in settings.items():
        if key in DEFAULT_SETTINGS:
            existing = db.query(Setting).filter(Setting.key == key).first()
            if existing:
                existing.value = str(value)
                existing.updated_at = datetime.utcnow()
            else:
                db.add(Setting(key=key, value=str(value)))
            updated.append(key)
    db.commit()
    return {"message": f"Updated {len(updated)} settings", "updated": updated}


# ============================================================================
# SYSTEM HEALTH
# ============================================================================
@router.get("/system/health")
async def system_health(authorization: Optional[str] = Header(None), db: Session = Depends(get_db)):
    _get_current_user(authorization)
    health = {"status": "healthy", "timestamp": datetime.utcnow().isoformat(), "services": {}}
    # PostgreSQL
    try:
        db.execute(text("SELECT 1"))
        health["services"]["postgres"] = {"status": "connected"}
    except Exception as e:
        health["services"]["postgres"] = {"status": "error", "error": str(e)}
        health["status"] = "degraded"
    # Redis
    try:
        import redis
        redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=2)
        redis_client.ping()
        health["services"]["redis"] = {"status": "connected"}
    except Exception as e:
        health["services"]["redis"] = {"status": "unavailable", "error": str(e)}
    # Ollama
    try:
        req = urllib.request.Request(f"{OLLAMA_URL}/api/tags")
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode())
            model_count = len(data.get("models", []))
            health["services"]["ollama"] = {"status": "connected", "models": model_count}
    except Exception as e:
        health["services"]["ollama"] = {"status": "unavailable", "error": str(e)}
    # SearXNG
    try:
        req = urllib.request.Request(os.getenv("SEARXNG_URL", "http://searxng:8080"))
        with urllib.request.urlopen(req, timeout=3) as response:
            health["services"]["searxng"] = {"status": "connected"}
    except Exception as e:
        health["services"]["searxng"] = {"status": "unavailable", "error": str(e)}
    # Celery
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
