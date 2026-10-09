"""Seguridad por diseño del API Gateway - WanderSync

- Hashing de contraseñas con Argon2id (alto factor de costo)
- Sesiones del lado del servidor en Redis, con REGENERACIÓN del identificador tras el
  login (mitiga Session Fixation) y rechazo de identificadores que el servidor no emitió
- Rate limiting por ventana fija en Redis
"""
import hashlib
import json
import os
import re
import secrets
import time

import redis.asyncio as redis
from argon2 import PasswordHasher, Type
from argon2.exceptions import InvalidHashError, VerifyMismatchError

REDIS_URL = os.environ["REDIS_URL"]
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "false").lower() == "true"
SESSION_COOKIE = "ws_session"
SESSION_TTL = int(os.getenv("SESSION_TTL_SECONDS", "1800"))   # 30 min de inactividad

r = redis.from_url(REDIS_URL, decode_responses=True)

# ----------------------------------------------------------------------
# Contraseñas: Argon2id
#   time_cost=3 iteraciones, memory_cost=64 MiB, parallelism=4  (recomendación OWASP)
# ----------------------------------------------------------------------
ph = PasswordHasher(time_cost=3, memory_cost=64 * 1024, parallelism=4, hash_len=32,
                    salt_len=16, type=Type.ID)

# Hash "señuelo": si el usuario no existe se verifica contra él igual,
# para que el tiempo de respuesta no revele qué correos están registrados
DUMMY_HASH = ph.hash(secrets.token_urlsafe(16))

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")


def hash_password(password: str) -> str:
    return ph.hash(password)


def verify_password(stored_hash: str | None, password: str) -> bool:
    try:
        return ph.verify(stored_hash or DUMMY_HASH, password) and stored_hash is not None
    except (VerifyMismatchError, InvalidHashError):
        return False


def needs_rehash(stored_hash: str) -> bool:
    return ph.check_needs_rehash(stored_hash)


def password_problems(password: str) -> str | None:
    if len(password) < 10:
        return "La contraseña debe tener al menos 10 caracteres"
    if not re.search(r"[A-Za-z]", password) or not re.search(r"\d", password):
        return "La contraseña debe combinar letras y números"
    if len(password) > 128:
        return "La contraseña es demasiado larga"
    return None


# ----------------------------------------------------------------------
# Sesiones en Redis
# ----------------------------------------------------------------------
def _key(session_id: str) -> str:
    return f"session:{session_id}"


def fingerprint(session_id: str) -> str:
    """Huella corta y NO reversible del identificador (para mostrar en la demo)."""
    return hashlib.sha256(session_id.encode()).hexdigest()[:10]


async def load_session(session_id: str | None) -> dict | None:
    """Solo se aceptan identificadores emitidos por el servidor (existentes en Redis)."""
    if not session_id or len(session_id) > 100:
        return None
    raw = await r.get(_key(session_id))
    if raw is None:
        return None
    await r.expire(_key(session_id), SESSION_TTL)   # expiración por inactividad
    return json.loads(raw)


async def create_session(data: dict) -> str:
    session_id = secrets.token_urlsafe(32)          # 256 bits de entropía
    await r.set(_key(session_id), json.dumps({**data, "created_at": int(time.time())}), ex=SESSION_TTL)
    return session_id


async def destroy_session(session_id: str | None) -> None:
    if session_id:
        await r.delete(_key(session_id))


async def regenerate_session(old_session_id: str | None, data: dict) -> str:
    """Mitigación de Session Fixation: se invalida el ID anterior y se emite uno NUEVO."""
    await destroy_session(old_session_id)
    return await create_session(data)


def set_session_cookie(response, session_id: str) -> None:
    response.set_cookie(SESSION_COOKIE, session_id, max_age=SESSION_TTL, httponly=True,
                        secure=COOKIE_SECURE, samesite="strict", path="/")


def clear_session_cookie(response) -> None:
    response.delete_cookie(SESSION_COOKIE, path="/", httponly=True, secure=COOKIE_SECURE, samesite="strict")


# ----------------------------------------------------------------------
# Rate limiting (ventana fija)
# ----------------------------------------------------------------------
async def rate_limit(bucket: str, limit: int, window: int) -> int | None:
    """Devuelve None si se permite, o los segundos que faltan si se superó el límite."""
    key = f"ratelimit:{bucket}"
    count = await r.incr(key)
    if count == 1:
        await r.expire(key, window)
    if count > limit:
        ttl = await r.ttl(key)
        return max(ttl, 1)
    return None


def client_ip(request) -> str:
    """IP real del cliente. nginx (frontend) SOBRESCRIBE X-Real-IP, así que no se puede falsificar."""
    return request.headers.get("x-real-ip") or (request.client.host if request.client else "unknown")
