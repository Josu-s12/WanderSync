"""API Gateway GraphQL - WanderSync

- Lecturas (catálogo): se delegan a pg_graphql (GraphQL nativo de Supabase/Postgres),
  pidiendo SOLO los campos que el cliente seleccionó -> cero over-fetching.
- Escrituras (reservas): se delegan al servicio de Órdenes, que ejecuta la SAGA.
- Seguridad: sesiones en Redis con regeneración de ID, Argon2id y rate limiting.
"""
import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum

import httpx
import psycopg
import strawberry
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from graphql import GraphQLError
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from strawberry.extensions import MaskErrors, QueryDepthLimiter
from strawberry.fastapi import GraphQLRouter
from strawberry.types import Info
from strawberry.types.nodes import SelectedField

import security as sec

DATABASE_URL = os.environ["DATABASE_URL"]
ORDERS_URL = os.environ["ORDERS_URL"]
ALLOW_FAILURE_SIMULATION = os.getenv("ALLOW_FAILURE_SIMULATION", "false").lower() == "true"
ENABLE_GRAPHIQL = os.getenv("ENABLE_GRAPHIQL", "false").lower() == "true"
GLOBAL_RATE_LIMIT = int(os.getenv("GLOBAL_RATE_LIMIT_PER_MINUTE", "120"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [GATEWAY] %(message)s")
log = logging.getLogger("gateway")


# ======================================================================
# Acceso a pg_graphql
# ======================================================================
FLIGHT_FIELDS = {"id", "sourceId", "origin", "destination", "airline", "departAt", "price", "seats", "scrapedAt"}
HOTEL_FIELDS = {"id", "sourceId", "city", "name", "stars", "pricePerNight", "rooms", "scrapedAt"}
CAR_FIELDS = {"id", "sourceId", "city", "model", "category", "pricePerDay", "units", "scrapedAt"}


def to_snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def requested_fields(info: Info, *path: str) -> set[str] | None:
    """Campos que el cliente pidió en la consulta (None = no se pudo determinar)."""
    node = info.selected_fields[0]
    for name in path:
        node = next((s for s in node.selections
                     if isinstance(s, SelectedField) and s.name == name), None)
        if node is None:
            return set()
    names = set()
    for s in node.selections:
        if not isinstance(s, SelectedField):   # fragmentos: pedir todo
            return None
        names.add(s.name)
    return names


def build_collection(collection, allowed, wanted, filters, order_field, first, prefix):
    """Arma un fragmento de consulta pg_graphql con sus variables."""
    fields = allowed if wanted is None else (wanted & allowed)
    fields = fields | {"id"}
    var_defs, variables, conds = [], {}, []
    for column, (op, value, gql_type) in filters.items():
        if value is None:
            continue
        var = f"{prefix}_{column}"
        var_defs.append(f"${var}: {gql_type}")
        variables[var] = value
        conds.append(f"{column}: {{{op}: ${var}}}")
    filter_arg = f"filter: {{{', '.join(conds)}}}, " if conds else ""
    snippet = (f"{collection}({filter_arg}orderBy: [{{{order_field}: AscNullsLast}}], first: {int(first)}) "
               f"{{ edges {{ node {{ {' '.join(sorted(fields))} }} }} }}")
    return snippet, var_defs, variables


async def pg_graphql(parts) -> dict:
    """Ejecuta una o varias colecciones en UNA sola llamada a la base de datos."""
    snippets, var_defs, variables = [], [], {}
    for snippet, defs, vars_ in parts:
        snippets.append(snippet)
        var_defs += defs
        variables |= vars_
    header = f"query({', '.join(var_defs)})" if var_defs else "query"
    query = f"{header} {{ {' '.join(snippets)} }}"
    log.info("pg_graphql <- %s | vars=%s", query, variables)

    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        async with conn.transaction():
            # Mínimo privilegio: rol de SOLO LECTURA que únicamente ve las tablas de ofertas
            await conn.execute("SET LOCAL ROLE gateway_reader")
            await conn.execute("SET LOCAL search_path TO flights, hotels, cars")
            cur = await conn.execute("SELECT graphql.resolve(%s, %s)", (query, Jsonb(variables)))
            (result,) = await cur.fetchone()

    if result.get("errors"):
        raise GraphQLError(f"Error en la capa de datos: {result['errors'][0]['message']}")
    return result["data"]


def nodes(data: dict, collection: str) -> list[dict]:
    return [{to_snake(k): v for k, v in e["node"].items()} for e in data[collection]["edges"]]


# ======================================================================
# Tipos GraphQL
# ======================================================================
@strawberry.type
class FlightOffer:
    id: int
    source_id: str | None = None
    origin: str | None = None
    destination: str | None = None
    airline: str | None = None
    depart_at: datetime | None = None
    price: float | None = None
    seats: int | None = None
    scraped_at: datetime | None = None


@strawberry.type
class HotelOffer:
    id: int
    source_id: str | None = None
    city: str | None = None
    name: str | None = None
    stars: int | None = None
    price_per_night: float | None = None
    rooms: int | None = None
    scraped_at: datetime | None = None


@strawberry.type
class CarOffer:
    id: int
    source_id: str | None = None
    city: str | None = None
    model: str | None = None
    category: str | None = None
    price_per_day: float | None = None
    units: int | None = None
    scraped_at: datetime | None = None


@strawberry.type
class TravelPackage:
    flight: FlightOffer
    hotel: HotelOffer
    car: CarOffer
    nights: int
    days: int
    total_price: float


@strawberry.type
class SagaStep:
    step: str
    action: str
    result: str
    detail: str | None
    at: datetime


@strawberry.type
class Order:
    id: strawberry.ID
    status: str
    total: float | None
    created_at: datetime
    _steps: strawberry.Private[list | None] = None

    @strawberry.field(description="Bitácora de la SAGA (solo se consulta si el cliente la pide)")
    async def steps(self) -> list[SagaStep]:
        if self._steps is None:
            self._steps = (await orders_get(f"/orders/{self.id}"))["steps"]
        return [SagaStep(step=s["step"], action=s["action"], result=s["result"],
                         detail=s["detail"], at=datetime.fromisoformat(s["at"]))
                for s in self._steps]


def parse(cls, row: dict):
    """Convierte la fila de pg_graphql (BigFloat -> float, fechas ISO -> datetime)."""
    for k, v in list(row.items()):
        if k in ("price", "price_per_night", "price_per_day") and v is not None:
            row[k] = float(v)
        elif k in ("depart_at", "scraped_at") and v is not None:
            row[k] = datetime.fromisoformat(v)
    return cls(**row)


def to_order(o: dict) -> Order:
    return Order(id=o["id"], status=o["status"], total=o["total"],
                 created_at=datetime.fromisoformat(o["created_at"]), _steps=o.get("steps"))


@strawberry.type
class User:
    id: int
    email: str


@strawberry.type
class SessionInfo:
    authenticated: bool
    email: str | None
    fingerprint: str = strawberry.field(
        description="Huella NO reversible del ID de sesión: permite ver en la demo que cambia tras el login")


# ======================================================================
# Helpers de seguridad
# ======================================================================
def current_user_id(info: Info) -> int:
    user_id = info.context["session"].get("user_id")
    if not user_id:
        raise GraphQLError("Debes iniciar sesión", extensions={"code": "UNAUTHENTICATED"})
    return user_id


async def enforce_rate_limit(bucket: str, limit: int, window: int) -> None:
    retry_after = await sec.rate_limit(bucket, limit, window)
    if retry_after:
        log.warning("Rate limit superado: %s", bucket)
        raise GraphQLError(f"Demasiados intentos. Intenta de nuevo en {retry_after} s",
                           extensions={"code": "RATE_LIMITED", "retryAfter": retry_after})


@asynccontextmanager
async def users_db():
    async with await psycopg.AsyncConnection.connect(
            DATABASE_URL, autocommit=True, row_factory=dict_row) as conn:
        # Mínimo privilegio: rol que solo puede tocar la tabla de usuarios
        await conn.execute("SET ROLE auth_service")
        yield conn


# ======================================================================
# Cliente del servicio de Órdenes
# ======================================================================
async def orders_get(path: str):
    async with httpx.AsyncClient(base_url=ORDERS_URL, timeout=10) as client:
        r = await client.get(path)
    if r.status_code != 200:
        raise GraphQLError(r.json().get("detail", "Error en el servicio de órdenes"))
    return r.json()


# ======================================================================
# Query
# ======================================================================
@strawberry.enum
class FailStep(Enum):
    FLIGHT = "FLIGHT"
    HOTEL = "HOTEL"
    CAR = "CAR"
    PAYMENT = "PAYMENT"


@strawberry.input
class BookPackageInput:
    flight_offer_id: int
    hotel_offer_id: int
    car_offer_id: int
    nights: int = 1
    days: int = 1
    simulate_failure_at: FailStep | None = strawberry.field(
        default=None, description="Solo para la demo: fuerza un fallo en ese paso de la SAGA")


@strawberry.type
class Query:
    @strawberry.field
    def health(self) -> str:
        return "ok"

    @strawberry.field(description="Vuelos disponibles")
    async def flights(self, info: Info, origin: str | None = None,
                      destination: str | None = None, limit: int = 20) -> list[FlightOffer]:
        part = build_collection(
            "flightOfferCollection", FLIGHT_FIELDS, requested_fields(info),
            {"origin": ("eq", origin, "String"), "destination": ("eq", destination, "String"),
             "seats": ("gt", 0, "Int")},
            "price", limit, "f")
        data = await pg_graphql([part])
        return [parse(FlightOffer, n) for n in nodes(data, "flightOfferCollection")]

    @strawberry.field(description="Hoteles disponibles")
    async def hotels(self, info: Info, city: str | None = None,
                     min_stars: int | None = None, limit: int = 20) -> list[HotelOffer]:
        part = build_collection(
            "hotelOfferCollection", HOTEL_FIELDS, requested_fields(info),
            {"city": ("eq", city, "String"), "stars": ("gte", min_stars, "Int"),
             "rooms": ("gt", 0, "Int")},
            "pricePerNight", limit, "h")
        data = await pg_graphql([part])
        return [parse(HotelOffer, n) for n in nodes(data, "hotelOfferCollection")]

    @strawberry.field(description="Autos disponibles")
    async def cars(self, info: Info, city: str | None = None,
                   category: str | None = None, limit: int = 20) -> list[CarOffer]:
        part = build_collection(
            "carOfferCollection", CAR_FIELDS, requested_fields(info),
            {"city": ("eq", city, "String"), "category": ("eq", category, "String"),
             "units": ("gt", 0, "Int")},
            "pricePerDay", limit, "c")
        data = await pg_graphql([part])
        return [parse(CarOffer, n) for n in nodes(data, "carOfferCollection")]

    @strawberry.field(description="Consolida vuelo + hotel + auto en paquetes ordenados por precio")
    async def search_packages(self, info: Info, origin: str, destination: str,
                              nights: int = 1, days: int = 1,
                              max_budget: float | None = None, limit: int = 5) -> list[TravelPackage]:
        f_wanted = requested_fields(info, "flight")
        h_wanted = requested_fields(info, "hotel")
        c_wanted = requested_fields(info, "car")
        # Los precios siempre se piden porque se necesitan para calcular el total
        data = await pg_graphql([
            build_collection("flightOfferCollection", FLIGHT_FIELDS,
                             None if f_wanted is None else f_wanted | {"price"},
                             {"origin": ("eq", origin, "String"),
                              "destination": ("eq", destination, "String"),
                              "seats": ("gt", 0, "Int")}, "price", 10, "f"),
            build_collection("hotelOfferCollection", HOTEL_FIELDS,
                             None if h_wanted is None else h_wanted | {"pricePerNight"},
                             {"city": ("eq", destination, "String"), "rooms": ("gt", 0, "Int")},
                             "pricePerNight", 10, "h"),
            build_collection("carOfferCollection", CAR_FIELDS,
                             None if c_wanted is None else c_wanted | {"pricePerDay"},
                             {"city": ("eq", destination, "String"), "units": ("gt", 0, "Int")},
                             "pricePerDay", 10, "c"),
        ])
        flights = [parse(FlightOffer, n) for n in nodes(data, "flightOfferCollection")]
        hotels = [parse(HotelOffer, n) for n in nodes(data, "hotelOfferCollection")]
        cars = [parse(CarOffer, n) for n in nodes(data, "carOfferCollection")]

        packages = []
        for f in flights:
            for h in hotels:
                for c in cars:
                    total = f.price + h.price_per_night * nights + c.price_per_day * days
                    if max_budget is None or total <= max_budget:
                        packages.append(TravelPackage(flight=f, hotel=h, car=c,
                                                      nights=nights, days=days, total_price=total))
        packages.sort(key=lambda p: p.total_price)
        return packages[:limit]

    @strawberry.field(description="Detalle de una orden del usuario autenticado")
    async def order(self, info: Info, id: strawberry.ID) -> Order:
        user_id = current_user_id(info)
        o = await orders_get(f"/orders/{id}")
        if o.get("user_id") != user_id:   # control de acceso: solo sus propias órdenes
            raise GraphQLError("Orden no encontrada")
        return to_order(o)

    @strawberry.field(description="Órdenes del usuario autenticado")
    async def my_orders(self, info: Info, limit: int = 20) -> list[Order]:
        user_id = current_user_id(info)
        return [to_order(o) for o in await orders_get(f"/orders?user_id={user_id}&limit={int(limit)}")]

    @strawberry.field(description="Usuario autenticado (null si no hay sesión)")
    def me(self, info: Info) -> User | None:
        session = info.context["session"]
        if not session.get("user_id"):
            return None
        return User(id=session["user_id"], email=session["email"])

    @strawberry.field(description="Estado de la sesión actual")
    def session(self, info: Info) -> SessionInfo:
        session = info.context["session"]
        return SessionInfo(authenticated=bool(session.get("user_id")), email=session.get("email"),
                           fingerprint=sec.fingerprint(info.context["session_id"]))


# ======================================================================
# Mutation
# ======================================================================
@strawberry.type
class Mutation:
    @strawberry.mutation(description="Crea una cuenta (contraseña guardada con Argon2id)")
    async def register(self, info: Info, email: str, password: str) -> User:
        await enforce_rate_limit(f"register:ip:{info.context['ip']}", 5, 600)
        email = email.strip().lower()
        if not sec.EMAIL_RE.match(email) or len(email) > 254:
            raise GraphQLError("Correo inválido")
        if problem := sec.password_problems(password):
            raise GraphQLError(problem)

        password_hash = await asyncio.to_thread(sec.hash_password, password)   # no bloquea el event loop
        try:
            async with users_db() as conn:
                row = await (await conn.execute(
                    "INSERT INTO orders.users (email, password_hash) VALUES (%s, %s) RETURNING id, email",
                    (email, password_hash))).fetchone()
        except UniqueViolation:
            raise GraphQLError("No se pudo registrar con ese correo")
        log.info("Usuario registrado id=%s", row["id"])
        return User(**row)

    @strawberry.mutation(description="Inicia sesión y REGENERA el ID de sesión (anti Session Fixation)")
    async def login(self, info: Info, email: str, password: str) -> User:
        ctx = info.context
        email = email.strip().lower()
        # Anti fuerza bruta: límite por IP y por cuenta
        await enforce_rate_limit(f"login:ip:{ctx['ip']}", 5, 60)
        await enforce_rate_limit(f"login:email:{email}", 10, 900)

        async with users_db() as conn:
            row = await (await conn.execute(
                "SELECT id, email, password_hash FROM orders.users WHERE email = %s", (email,))).fetchone()
        ok = await asyncio.to_thread(sec.verify_password, row["password_hash"] if row else None, password)
        if not ok:
            log.warning("Login fallido para %s desde %s", email, ctx["ip"])
            raise GraphQLError("Credenciales inválidas", extensions={"code": "UNAUTHENTICATED"})

        if sec.needs_rehash(row["password_hash"]):   # si se sube el costo, se actualiza el hash
            new_hash = await asyncio.to_thread(sec.hash_password, password)
            async with users_db() as conn:
                await conn.execute("UPDATE orders.users SET password_hash = %s WHERE id = %s",
                                   (new_hash, row["id"]))

        old_fp = sec.fingerprint(ctx["session_id"])
        new_session_id = await sec.regenerate_session(ctx["session_id"],
                                                      {"user_id": row["id"], "email": row["email"]})
        sec.set_session_cookie(ctx["response"], new_session_id)
        ctx["session_id"], ctx["session"] = new_session_id, {"user_id": row["id"], "email": row["email"]}
        log.info("Login OK user=%s | sesión regenerada %s -> %s",
                 row["id"], old_fp, sec.fingerprint(new_session_id))
        return User(id=row["id"], email=row["email"])

    @strawberry.mutation(description="Cierra la sesión y destruye el ID en el servidor")
    async def logout(self, info: Info) -> bool:
        await sec.destroy_session(info.context["session_id"])
        sec.clear_session_cookie(info.context["response"])
        return True

    @strawberry.mutation(description="Reserva un paquete completo ejecutando la SAGA (requiere sesión)")
    async def book_package(self, info: Info, input: BookPackageInput) -> Order:
        user_id = current_user_id(info)
        # Checkout/pago: endpoint sensible -> límite por usuario y por IP
        await enforce_rate_limit(f"checkout:user:{user_id}", 5, 60)
        await enforce_rate_limit(f"checkout:ip:{info.context['ip']}", 10, 60)
        if input.simulate_failure_at and not ALLOW_FAILURE_SIMULATION:
            raise GraphQLError("La simulación de fallos está deshabilitada")
        payload = {
            "user_id": user_id,
            "flight_offer_id": input.flight_offer_id,
            "hotel_offer_id": input.hotel_offer_id,
            "car_offer_id": input.car_offer_id,
            "nights": input.nights,
            "days": input.days,
            "fail_at": input.simulate_failure_at.value if input.simulate_failure_at else None,
        }
        async with httpx.AsyncClient(base_url=ORDERS_URL, timeout=60) as client:
            r = await client.post("/orders", json=payload)
        if r.status_code != 201:
            raise GraphQLError(f"No se pudo crear la orden: {r.text}")
        return to_order(r.json())


def is_unexpected(error) -> bool:
    """Error interno (bug, BD caída...) y no un error de negocio lanzado a propósito."""
    return error.original_error is not None and not isinstance(error.original_error, GraphQLError)


class WanderSyncSchema(strawberry.Schema):
    def process_errors(self, errors, execution_context=None):
        # Solo se registran con traza los errores inesperados
        super().process_errors([e for e in errors if is_unexpected(e)], execution_context)


schema = WanderSyncSchema(
    query=Query,
    mutation=Mutation,
    extensions=[
        QueryDepthLimiter(max_depth=6),   # protege contra consultas abusivas
        # Los errores internos se ocultan al cliente ("Unexpected error.") para no filtrar detalles
        MaskErrors(should_mask_error=is_unexpected),
    ],
)

async def get_context(request: Request, response: Response) -> dict:
    """Carga la sesión del cookie. Un ID desconocido (p. ej. fijado por un atacante) NO se
    acepta: se emite uno nuevo generado por el servidor."""
    session_id = request.cookies.get(sec.SESSION_COOKIE)
    session = await sec.load_session(session_id)
    if session is None:
        session = {"user_id": None}
        session_id = await sec.create_session(session)   # sesión anónima
        sec.set_session_cookie(response, session_id)
    return {"session_id": session_id, "session": session, "ip": sec.client_ip(request)}


app = FastAPI(title="WanderSync - API Gateway (GraphQL)", docs_url=None, redoc_url=None, openapi_url=None)
app.include_router(
    GraphQLRouter(schema, context_getter=get_context, graphql_ide="graphiql" if ENABLE_GRAPHIQL else None),
    prefix="/graphql",
)


@app.middleware("http")
async def security_middleware(request: Request, call_next):
    # Rate limit global por IP (protección contra DoS sobre el endpoint GraphQL)
    if request.url.path.startswith("/graphql") and request.method == "POST":
        retry_after = await sec.rate_limit(f"global:{sec.client_ip(request)}", GLOBAL_RATE_LIMIT, 60)
        if retry_after:
            return JSONResponse({"errors": [{"message": "Demasiadas solicitudes"}]}, status_code=429,
                                headers={"Retry-After": str(retry_after)})
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    if request.method == "POST":
        response.headers["Cache-Control"] = "no-store"
    return response


@app.get("/health")
def health():
    return {"status": "ok", "service": "gateway"}