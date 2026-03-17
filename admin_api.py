"""Admin API for vinosomm.ai - Wine Intelligence Application
Provides authentication, CRUD operations, enrichment tasks, dashboard analytics, and AI chat.
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

from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, select, func
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

# Database setup
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()

# Create router
router = APIRouter(prefix="/admin/api", tags=["admin"])


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
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Task(Base):
    __tablename__ = "tasks"

    id = Column(Integer, primary_key=True, index=True)
    task_type = Column(String(50))
    wine_id = Column(Integer)
    wine_name = Column(String(500))
    status = Column(String(20), default="queued")
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    result = Column(Text)
    error = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


# Create tables
Base.metadata.create_all(bind=engine)


# ============================================================================
# JWT Auth (stdlib only)
# ============================================================================

def _encode_jwt(payload: Dict[str, Any]) -> str:
    """Encode JWT token using HMAC-SHA256."""
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
    """Decode and validate JWT token."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None

        header, body, signature = parts

        # Verify signature
        message = f"{header}.{body}".encode()
        expected_sig = base64.urlsafe_b64encode(
            hmac.new(JWT_SECRET.encode(), message, hashlib.sha256).digest()
        ).rstrip(b"=").decode()

        if not hmac.compare_digest(signature, expected_sig):
            return None

        # Decode payload
        padding = "=" * (4 - len(body) % 4)
        payload_json = base64.urlsafe_b64decode(body + padding)
        payload = json.loads(payload_json)

        # Check expiry
        if payload.get("exp", 0) < time.time():
            return None

        return payload
    except Exception:
        return None


def _get_current_user(authorization: Optional[str] = None) -> str:
    """Extract and validate user from Bearer token."""
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


def require_auth(f):
    """Dependency for protected endpoints."""
    @wraps(f)
    async def wrapper(*args, authorization: Optional[str] = None, **kwargs):
        user = _get_current_user(authorization)
        return await f(*args, user=user, **kwargs)
    return wrapper


def get_db():
    """Dependency to get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ============================================================================
# Pydantic-like schemas (dicts for simplicity)
# ============================================================================

def wine_to_dict(wine: Wine) -> Dict[str, Any]:
    """Convert Wine ORM object to dictionary."""
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
        "created_at": wine.created_at.isoformat() if wine.created_at else None,
        "updated_at": wine.updated_at.isoformat() if wine.updated_at else None,
    }


def task_to_dict(task: Task) -> Dict[str, Any]:
    """Convert Task ORM object to dictionary."""
    return {
        "id": task.id,
        "task_type": task.task_type,
        "wine_id": task.wine_id,
        "wine_name": task.wine_name,
        "status": task.status,
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
    """Login endpoint - accepts username and password, returns JWT token."""
    username = payload.get("username")
    password = payload.get("password")

    if not username or not password:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing credentials")

    # Verify credentials
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    if username != ADMIN_USER or password_hash != ADMIN_PASS_HASH:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    # Generate JWT
    exp = int(time.time()) + 86400  # 24 hours
    token = _encode_jwt({
        "sub": ADMIN_USER,
        "iat": int(time.time()),
        "exp": exp,
    })

    return {"access_token": token, "token_type": "bearer", "expires_in": 86400}


@router.get("/auth/me")
async def get_current_user_info(authorization: Optional[str] = None):
    """Get current authenticated user info."""
    user = _get_current_user(authorization)
    return {"username": user, "role": "admin"}


# ============================================================================
# DASHBOARD Endpoint
# ============================================================================

@router.get("/dashboard")
async def get_dashboard(authorization: Optional[str] = None, db: Session = Depends(get_db)):
    """Get dashboard statistics."""
    _get_current_user(authorization)

    # Total wines
    total_wines = db.query(func.count(Wine.id)).scalar() or 0

    # Wines by type
    wines_by_type = db.query(Wine.wine_type, func.count(Wine.id)).group_by(Wine.wine_type).all()
    wines_by_type_dict = {row[0]: row[1] for row in wines_by_type}

    # Wines by country (top 10)
    wines_by_country = db.query(Wine.country, func.count(Wine.id)).group_by(Wine.country).order_by(func.count(Wine.id).desc()).limit(10).all()
    wines_by_country_dict = {row[0]: row[1] for row in wines_by_country}

    # Recent wines (last 5)
    recent_wines = db.query(Wine).order_by(Wine.created_at.desc()).limit(5).all()
    recent_wines_list = [wine_to_dict(w) for w in recent_wines]

    # Average completeness
    avg_completeness = db.query(func.avg(Wine.completeness)).scalar() or 0

    # Enrichment stats
    total_with_rating = db.query(func.count(Wine.id)).filter(Wine.rating.isnot(None)).scalar() or 0
    total_with_source = db.query(func.count(Wine.id)).filter(Wine.source_list.isnot(None)).scalar() or 0

    # Wines added today/this week/this month
    now = datetime.utcnow()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = today_start - timedelta(days=now.weekday())
    month_start = today_start.replace(day=1)

    wines_added_today = db.query(func.count(Wine.id)).filter(Wine.created_at >= today_start).scalar() or 0
    wines_added_this_week = db.query(func.count(Wine.id)).filter(Wine.created_at >= week_start).scalar() or 0
    wines_added_this_month = db.query(func.count(Wine.id)).filter(Wine.created_at >= month_start).scalar() or 0

    return {
        "total_wines": total_wines,
        "wines_by_type": wines_by_type_dict,
        "wines_by_country": wines_by_country_dict,
        "recent_wines": recent_wines_list,
        "avg_completeness": float(avg_completeness),
        "enrichment_stats": {
            "total_with_rating": total_with_rating,
            "total_with_source": total_with_source,
        },
        "wines_added_today": wines_added_today,
        "wines_added_this_week": wines_added_this_week,
        "wines_added_this_month": wines_added_this_month,
    }


# ============================================================================
# WINES CRUD Endpoints
# ============================================================================

@router.get("/wines")
async def list_wines(
    authorization: Optional[str] = None,
    db: Session = Depends(get_db),
    search: Optional[str] = Query(None),
    wine_type: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    min_completeness: Optional[float] = Query(None),
    max_completeness: Optional[float] = Query(None),
    sort_by: str = Query("created_at"),
    sort_dir: str = Query("desc"),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    """Get paginated list of wines with filters."""
    _get_current_user(authorization)

    query = db.query(Wine)

    # Apply filters
    if search:
        search_term = f"%{search}%"
        query = query.filter(or_(
            Wine.name.ilike(search_term),
            Wine.producer.ilike(search_term),
            Wine.region.ilike(search_term),
        ))

    if wine_type:
        query = query.filter(Wine.wine_type == wine_type)

    if country:
        query = query.filter(Wine.country == country)

    if min_completeness is not None:
        query = query.filter(Wine.completeness >= min_completeness)

    if max_completeness is not None:
        query = query.filter(Wine.completeness <= max_completeness)

    # Get total count
    total = query.count()

    # Sort
    sort_column = getattr(Wine, sort_by, Wine.created_at)
    if sort_dir.lower() == "asc":
        query = query.order_by(sort_column.asc())
    else:
        query = query.order_by(sort_column.desc())

    # Paginate
    wines = query.offset((page - 1) * per_page).limit(per_page).all()

    return {
        "wines": [wine_to_dict(w) for w in wines],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
    }


@router.get("/wines/{wine_id}")
async def get_wine(wine_id: int, authorization: Optional[str] = None, db: Session = Depends(get_db)):
    """Get full details of a specific wine."""
    _get_current_user(authorization)

    wine = db.query(Wine).filter(Wine.id == wine_id).first()
    if not wine:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Wine not found")

    return wine_to_dict(wine)


@router.post("/wines")
async def create_wine(payload: Dict[str, Any], authorization: Optional[str] = None, db: Session = Depends(get_db)):
    """Create a new wine."""
    _get_current_user(authorization)

    name = payload.get("name")
    wine_type = payload.get("wine_type")

    if not name or not wine_type:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="name and wine_type are required")

    wine = Wine(
        name=name,
        wine_type=wine_type,
        producer=payload.get("producer"),
        vintage=payload.get("vintage"),
        region=payload.get("region"),
        country=payload.get("country"),
        grape_variety=payload.get("grape_variety"),
        classification=payload.get("classification"),
        sweetness=payload.get("sweetness"),
        acidity=payload.get("acidity"),
        tannin=payload.get("tannin"),
        body=payload.get("body"),
        alcohol_warmth=payload.get("alcohol_warmth"),
        effervescence=payload.get("effervescence"),
        flavor_intensity=payload.get("flavor_intensity"),
        finish=payload.get("finish"),
        complexity=payload.get("complexity"),
        fruit_character=payload.get("fruit_character"),
        secondary_aromas=payload.get("secondary_aromas"),
        tertiary_notes=payload.get("tertiary_notes"),
        residual_sugar=payload.get("residual_sugar"),
        alcohol=payload.get("alcohol"),
        price=payload.get("price"),
        currency=payload.get("currency"),
        rating=payload.get("rating"),
        data_method=payload.get("data_method"),
        source_list=payload.get("source_list"),
        source_count=payload.get("source_count", 0),
        provenance_notes=payload.get("provenance_notes"),
        avg_confidence=payload.get("avg_confidence"),
        completeness=payload.get("completeness"),
    )

    db.add(wine)
    db.commit()
    db.refresh(wine)

    return wine_to_dict(wine)


@router.put("/wines/{wine_id}")
async def update_wine(wine_id: int, payload: Dict[str, Any], authorization: Optional[str] = None, db: Session = Depends(get_db)):
    """Update an existing wine."""
    _get_current_user(authorization)

    wine = db.query(Wine).filter(Wine.id == wine_id).first()
    if not wine:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Wine not found")

    # Update fields
    for key, value in payload.items():
        if hasattr(wine, key) and value is not None:
            setattr(wine, key, value)

    wine.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(wine)

    return wine_to_dict(wine)


@router.delete("/wines/{wine_id}")
async def delete_wine(wine_id: int, authorization: Optional[str] = None, db: Session = Depends(get_db)):
    """Delete a wine."""
    _get_current_user(authorization)

    wine = db.query(Wine).filter(Wine.id == wine_id).first()
    if not wine:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Wine not found")

    db.delete(wine)
    db.commit()

    return {"message": "Wine deleted successfully"}


# ============================================================================
# ENRICHMENT Endpoints
# ============================================================================

@router.post("/enrich/{wine_id}")
async def enrich_wine(wine_id: int, authorization: Optional[str] = None, db: Session = Depends(get_db)):
    """Trigger enrichment for a single wine."""
    _get_current_user(authorization)

    wine = db.query(Wine).filter(Wine.id == wine_id).first()
    if not wine:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Wine not found")

    # Create task record
    task = Task(
        task_type="enrich",
        wine_id=wine.id,
        wine_name=wine.name,
        status="queued",
    )

    db.add(task)
    db.commit()
    db.refresh(task)

    # TODO: Trigger Celery task here
    # celery_app.send_task('tasks.enrich_wine', args=[wine_id, task.id])

    return {
        "task_id": task.id,
        "status": task.status,
        "message": "Enrichment task created",
    }


@router.post("/enrich/batch")
async def enrich_batch(payload: Dict[str, Any], authorization: Optional[str] = None, db: Session = Depends(get_db)):
    """Trigger enrichment for multiple wines."""
    _get_current_user(authorization)

    wine_ids = payload.get("wine_ids")
    filters = payload.get("filter")

    if not wine_ids and not filters:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Provide wine_ids or filter")

    # Get wines to enrich
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

    # Create task records
    tasks = []
    for wine in wines:
        task = Task(
            task_type="enrich",
            wine_id=wine.id,
            wine_name=wine.name,
            status="queued",
        )
        db.add(task)
        tasks.append(task)

    db.commit()

    # TODO: Trigger Celery batch task
    # celery_app.send_task('tasks.enrich_batch', args=[[t.id for t in tasks]])

    return {
        "task_count": len(tasks),
        "task_ids": [t.id for t in tasks],
        "status": "queued",
        "message": f"Batch enrichment created for {len(tasks)} wines",
    }


@router.get("/tasks")
async def list_tasks(
    authorization: Optional[str] = None,
    db: Session = Depends(get_db),
    status_filter: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    """Get paginated list of enrichment tasks."""
    _get_current_user(authorization)

    query = db.query(Task)

    if status_filter:
        query = query.filter(Task.status == status_filter)

    total = query.count()

    tasks = query.order_by(Task.created_at.desc()).offset((page - 1) * per_page).limit(per_page).all()

    return {
        "tasks": [task_to_dict(t) for t in tasks],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
    }


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: int, authorization: Optional[str] = None, db: Session = Depends(get_db)):
    """Cancel an enrichment task."""
    _get_current_user(authorization)

    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")

    if task.status in ["completed", "failed"]:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot cancel a completed or failed task")

    task.status = "cancelled"
    db.commit()

    return {"message": "Task cancelled successfully", "task_id": task_id}


# ============================================================================
# AI CHAT Endpoint
# ============================================================================

def _query_ollama(prompt: str, system_prompt: str) -> str:
    """Query Ollama API and return response."""
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
async def chat(payload: Dict[str, Any], authorization: Optional[str] = None, db: Session = Depends(get_db)):
    """AI chat endpoint for wine database assistant."""
    _get_current_user(authorization)

    message = payload.get("message", "").strip()
    history = payload.get("history", [])

    if not message:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Message is required")

    # Build conversation context
    context = ""
    for msg in history[-5:]:  # Last 5 messages
        context += f"{msg.get('role', 'user')}: {msg.get('content', '')}\n"

    # System prompt for wine assistant
    system_prompt = """You are a wine database assistant for vinosomm.ai. Your capabilities:

1. ADD WINE: If user says "add wine" or similar, extract wine details and respond with JSON:
{"action": "add_wine", "data": {"name": "...", "producer": "...", "wine_type": "...", ...}}

2. ENRICH: If user says "enrich wine" or "update details", respond with:
{"action": "enrich", "wine_ids": [1, 2, 3]}

3. QUERY: If user asks about wines in the database, respond with:
{"action": "query", "sql_hint": "SELECT * FROM wines WHERE ..."}

4. ANSWER: For general wine knowledge questions, provide helpful information.

Always respond in valid JSON when performing actions, or plain text for general questions."""

    # Query Ollama
    full_prompt = context + f"user: {message}\nassistant:"
    response = _query_ollama(full_prompt, system_prompt)

    # Try to parse as JSON action
    parsed_response = None
    try:
        parsed_response = json.loads(response)
    except json.JSONDecodeError:
        # Not JSON, return as plain text
        pass

    return {
        "response": response,
        "parsed": parsed_response,
    }


# ============================================================================
# SYSTEM HEALTH Endpoint
# ============================================================================

@router.get("/system/health")
async def system_health(authorization: Optional[str] = None, db: Session = Depends(get_db)):
    """Check system health and service statuses."""
    _get_current_user(authorization)

    health = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "services": {},
    }

    # Check PostgreSQL
    try:
        db.execute("SELECT 1")
        health["services"]["postgres"] = {"status": "connected"}
    except Exception as e:
        health["services"]["postgres"] = {"status": "error", "error": str(e)}
        health["status"] = "degraded"

    # Check Redis (optional - just try to import)
    try:
        import redis
        redis_client = redis.Redis.from_url(
            f"redis://:{os.getenv('REDIS_PASSWORD', 'password')}@redis:6379/0",
            decode_responses=True,
            socket_connect_timeout=2,
        )
        redis_client.ping()
        health["services"]["redis"] = {"status": "connected"}
    except Exception as e:
        health["services"]["redis"] = {"status": "unavailable", "error": str(e)}

    # Check Ollama
    try:
        req = urllib.request.Request(f"{OLLAMA_URL}/api/tags")
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode())
            model_count = len(data.get("models", []))
            health["services"]["ollama"] = {"status": "connected", "models": model_count}
    except Exception as e:
        health["services"]["ollama"] = {"status": "unavailable", "error": str(e)}

    return health