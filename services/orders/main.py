"""Microservicio de Órdenes y Facturación - Orquestador SAGA - WanderSync

La SAGA se ejecuta como un flow de Prefect: cada reserva, el cobro y cada compensación
son tareas visibles en la UI de Prefect (con sus reintentos). Si Prefect no está
disponible, la misma SAGA se ejecuta directamente: las reservas NUNCA dependen de Prefect.
"""
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Literal

import httpx
import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.runtime import flow_run
from prefect.states import Completed, Failed
from pydantic import BaseModel, Field

DATABASE_URL = os.environ["DATABASE_URL"]
SERVICES = {
    "FLIGHT": os.environ["FLIGHTS_URL"],
    "HOTEL": os.environ["HOTELS_URL"],
    "CAR": os.environ["CARS_URL"],
}
SAGA_PREFECT = os.getenv("SAGA_PREFECT", "true").lower() == "true"
PREFECT_API_URL = os.getenv("PREFECT_API_URL")
PREFECT_UI_URL = os.getenv("PREFECT_UI_URL", "http://localhost:4200")
STEP_NAMES = {"FLIGHT": "vuelo", "HOTEL": "hotel", "CAR": "auto", "PAYMENT": "pago"}

# Prefect configura el logging al importarse: los mensajes del logger "saga" salen por consola
# y, dentro de un flow, también en la UI de Prefect (PREFECT_LOGGING_EXTRA_LOGGERS=saga)
log = logging.getLogger("saga")
log.setLevel(logging.INFO)


def get_conn():
    # autocommit: cada registro del saga_log queda guardado al instante
    return psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=True)


class StepFailed(Exception):
    def __init__(self, step: str, reason: str):
        super().__init__(f"{step}: {reason}")
        self.step = step
        self.reason = reason

    def __reduce__(self):   # para que Prefect pueda serializar la excepción
        return StepFailed, (self.step, self.reason)


class OrderIn(BaseModel):
    user_id: int | None = None
    flight_offer_id: int
    hotel_offer_id: int
    car_offer_id: int
    nights: int = Field(default=1, ge=1, le=30)
    days: int = Field(default=1, ge=1, le=30)
    # Para la demo: en qué paso forzar un fallo
    fail_at: Literal["FLIGHT", "HOTEL", "CAR", "PAYMENT"] | None = None


# ----------------------------------------------------------------------
# Registro de cada paso (trazabilidad de la SAGA)
# ----------------------------------------------------------------------
def log_step(conn, order_id, step, action, result, detail=None, booking_id=None):
    conn.execute(
        """INSERT INTO orders.saga_log (order_id, step, action, result, detail, booking_id)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (order_id, step, action, result, detail, booking_id),
    )
    log.info("order=%s | %-7s | %-10s | %-6s | %s", order_id, step, action, result, detail or "")


# ----------------------------------------------------------------------
# Transacciones locales (llamadas a los microservicios)
# ----------------------------------------------------------------------
def book(step: str, order_id: uuid.UUID, payload: dict) -> dict:
    try:
        r = httpx.post(f"{SERVICES[step]}/bookings",
                       json={"order_id": str(order_id), **payload}, timeout=5)
    except httpx.HTTPError as e:
        raise StepFailed(step, f"error de red ({e.__class__.__name__})")
    if r.status_code not in (200, 201):
        try:
            reason = r.json().get("detail", r.text)
        except ValueError:
            reason = r.text
        raise StepFailed(step, f"HTTP {r.status_code} - {reason}")
    return r.json()


def cancel_once(step: str, booking_id) -> None:
    """Transacción de compensación: cancela la reserva en el microservicio."""
    try:
        r = httpx.post(f"{SERVICES[step]}/bookings/{booking_id}/cancel", timeout=5)
    except httpx.HTTPError as e:
        raise StepFailed(step, f"error de red al compensar ({e.__class__.__name__})")
    if r.status_code != 200:
        raise StepFailed(step, f"HTTP {r.status_code} al compensar")


def cancel(step: str, booking_id, retries: int = 3) -> None:
    """Compensación con hasta 3 reintentos y backoff (1 s, 2 s, 3 s): DEBE terminar bien."""
    for attempt in range(retries + 1):
        try:
            return cancel_once(step, booking_id)
        except StepFailed:
            if attempt == retries:
                raise StepFailed(step, "no se pudo compensar tras varios reintentos")
            log.warning("Reintento %s/%s compensando %s %s", attempt + 1, retries, step, booking_id)
            time.sleep(attempt + 1)


def charge(conn, order_id, amount: Decimal, fail: bool) -> uuid.UUID:
    """Facturación: registrar el cobro del paquete."""
    if fail:
        raise StepFailed("PAYMENT", "pago rechazado por la pasarela (fallo simulado)")
    payment_id = uuid.uuid4()
    conn.execute(
        "INSERT INTO orders.payments (id, order_id, amount, status) VALUES (%s, %s, %s, 'CAPTURED')",
        (payment_id, order_id, amount),
    )
    return payment_id


# ----------------------------------------------------------------------
# Formas de ejecutar cada paso: directa o como tareas de Prefect
# ----------------------------------------------------------------------
class DirectOps:
    """Ejecución directa (respaldo si Prefect no está disponible)."""
    book = staticmethod(book)
    charge = staticmethod(charge)
    cancel = staticmethod(cancel)


@task(name="reservar", task_run_name="reservar-{step}", cache_policy=NO_CACHE)
def book_task(step: str, order_id: uuid.UUID, payload: dict) -> dict:
    return book(step, order_id, payload)


@task(name="cobrar-pago", task_run_name="cobrar-pago", cache_policy=NO_CACHE)
def charge_task(order_id: uuid.UUID, amount: Decimal, fail: bool) -> uuid.UUID:
    with get_conn() as conn:
        return charge(conn, order_id, amount, fail)


@task(name="compensar", task_run_name="compensar-{step}", cache_policy=NO_CACHE,
      retries=3, retry_delay_seconds=[1, 2, 3])
def cancel_task(step: str, booking_id) -> None:
    cancel_once(step, booking_id)


class PrefectOps:
    """Cada paso de la SAGA queda registrado como una tarea en la UI de Prefect."""
    book = staticmethod(book_task)
    cancel = staticmethod(cancel_task)

    @staticmethod
    def charge(conn, order_id, amount, fail):
        return charge_task(order_id, amount, fail)


# ----------------------------------------------------------------------
# Orquestador SAGA
# ----------------------------------------------------------------------
def compensate(conn, order_id, completed: list[tuple[str, str]], ops=DirectOps) -> bool:
    """Deshace los pasos ya confirmados en ORDEN INVERSO."""
    all_ok = True
    for step, booking_id in reversed(completed):
        try:
            ops.cancel(step, booking_id)
            log_step(conn, order_id, step, "COMPENSATE", "OK", "reserva cancelada", booking_id)
        except StepFailed as e:
            all_ok = False
            log_step(conn, order_id, step, "COMPENSATE", "FAILED", e.reason, booking_id)
    return all_ok


def run_saga(order_id: uuid.UUID, data: OrderIn, ops=DirectOps) -> tuple[str, str | None]:
    """Ejecuta la SAGA. Devuelve (estado final de la orden, paso que falló)."""
    plan = [
        ("FLIGHT", {"offer_id": data.flight_offer_id}),
        ("HOTEL", {"offer_id": data.hotel_offer_id, "nights": data.nights}),
        ("CAR", {"offer_id": data.car_offer_id, "days": data.days}),
    ]
    completed: list[tuple[str, str]] = []
    total = Decimal("0")

    with get_conn() as conn:
        try:
            for step, payload in plan:
                booking = ops.book(step, order_id, {**payload, "simulate_failure": data.fail_at == step})
                completed.append((step, booking["id"]))
                total += Decimal(str(booking["amount"]))
                log_step(conn, order_id, step, "EXECUTE", "OK", f"monto={booking['amount']}", booking["id"])

            payment_id = ops.charge(conn, order_id, total, data.fail_at == "PAYMENT")
            log_step(conn, order_id, "PAYMENT", "EXECUTE", "OK", f"total={total}", payment_id)

        except StepFailed as e:
            log_step(conn, order_id, e.step, "EXECUTE", "FAILED", e.reason)
            ok = compensate(conn, order_id, completed, ops)
            status = "COMPENSATED" if ok else "COMPENSATION_PENDING"
            conn.execute("UPDATE orders.orders SET status = %s WHERE id = %s", (status, order_id))
            return status, e.step

        conn.execute(
            "UPDATE orders.orders SET status = 'CONFIRMED', total = %s WHERE id = %s", (total, order_id)
        )
    return "CONFIRMED", None


# ----------------------------------------------------------------------
# La SAGA como flow de Prefect (observabilidad)
# ----------------------------------------------------------------------
def saga_run_name() -> str:
    params = flow_run.parameters
    fail_at = params["data"].get("fail_at")
    scenario = f"fallo-{STEP_NAMES[fail_at]}" if fail_at else "happy-path"
    return f"saga-{params['order_id'][:8]}-{scenario}"


@flow(name="saga-reserva-paquete", flow_run_name=saga_run_name, validate_parameters=False)
def saga_flow(order_id: str, data: dict):
    status, failed_step = run_saga(uuid.UUID(order_id), OrderIn(**data), ops=PrefectOps)
    if status == "CONFIRMED":
        return Completed(message="Reserva confirmada: vuelo, hotel, auto y pago OK")
    if status == "COMPENSATED":
        return Completed(name="Compensated",
                         message=f"Falló el paso {failed_step}: compensaciones ejecutadas en orden inverso")
    return Failed(name="CompensationPending",
                  message=f"Falló el paso {failed_step} y una compensación quedó pendiente")


_prefect_health = {"ok": False, "checked": 0.0}


def prefect_available() -> bool:
    """Se verifica Prefect (con caché de 15 s) antes de usarlo, para no depender de él."""
    if not (SAGA_PREFECT and PREFECT_API_URL):
        return False
    if time.time() - _prefect_health["checked"] > 15:
        try:
            _prefect_health["ok"] = httpx.get(f"{PREFECT_API_URL}/health", timeout=1).status_code == 200
        except httpx.HTTPError:
            _prefect_health["ok"] = False
        _prefect_health["checked"] = time.time()
    return _prefect_health["ok"]


def execute_saga(order_id: uuid.UUID, data: OrderIn) -> str | None:
    """Ejecuta la SAGA con Prefect si está disponible; si no, directamente.
    Devuelve el enlace a la ejecución en la UI de Prefect (o None)."""
    if not prefect_available():
        log.info("order=%s | Prefect no disponible: SAGA en modo directo", order_id)
        run_saga(order_id, data)
        return None
    try:
        state = saga_flow(str(order_id), data.model_dump(), return_state=True)
        return f"{PREFECT_UI_URL}/runs/flow-run/{state.state_details.flow_run_id}"
    except Exception:
        log.exception("order=%s | Falló la integración con Prefect", order_id)
        _prefect_health["checked"] = 0.0
        with get_conn() as conn:
            started = conn.execute("SELECT 1 FROM orders.saga_log WHERE order_id = %s LIMIT 1",
                                   (order_id,)).fetchone()
        if started:
            recover_order(order_id)        # compensa lo que alcanzó a ejecutarse
        else:
            run_saga(order_id, data)       # no alcanzó a empezar: se ejecuta en modo directo
        return None


def recover_order(order_id) -> bool:
    """Compensa una orden que quedó a medias usando el saga_log. Devuelve True si se recuperó."""
    with get_conn() as conn:
        order = conn.execute("SELECT status FROM orders.orders WHERE id = %s", (order_id,)).fetchone()
        if not order or order["status"] not in ("PENDING", "COMPENSATION_PENDING"):
            return False
        pending = conn.execute(
            """SELECT step, booking_id FROM orders.saga_log l
               WHERE l.order_id = %s AND l.action = 'EXECUTE' AND l.result = 'OK'
                 AND l.step <> 'PAYMENT'
                 AND NOT EXISTS (SELECT 1 FROM orders.saga_log c
                                 WHERE c.order_id = l.order_id AND c.booking_id = l.booking_id
                                   AND c.action = 'COMPENSATE' AND c.result = 'OK')
               ORDER BY l.id""",
            (order_id,),
        ).fetchall()
        ok = compensate(conn, order_id, [(p["step"], p["booking_id"]) for p in pending])
        status = "COMPENSATED" if ok else "COMPENSATION_PENDING"
        conn.execute("UPDATE orders.orders SET status = %s WHERE id = %s", (status, order_id))
    return True


def recover_orphans() -> int:
    """Evita "reservas huérfanas": si el servicio se cayó a mitad de una SAGA,
    al arrancar compensa las órdenes que quedaron a medias."""
    with get_conn() as conn:
        orders = conn.execute(
            "SELECT id FROM orders.orders WHERE status IN ('PENDING', 'COMPENSATION_PENDING')"
        ).fetchall()
    return sum(recover_order(o["id"]) for o in orders)


@asynccontextmanager
async def lifespan(app: FastAPI):
    n = recover_orphans()
    log.info("Recuperación al arrancar: %s orden(es) huérfana(s) compensada(s)", n)
    yield


app = FastAPI(title="WanderSync - Orders & Billing Service (SAGA Orchestrator)", lifespan=lifespan)


# ----------------------------------------------------------------------
# API
# ----------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok", "service": "orders"}


@app.post("/orders", status_code=201)
def create_order(data: OrderIn):
    order_id = uuid.uuid4()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO orders.orders (id, user_id, status) VALUES (%s, %s, 'PENDING')",
            (order_id, data.user_id),
        )
    log.info("order=%s | ===== INICIO SAGA ===== fail_at=%s", order_id, data.fail_at)
    prefect_url = execute_saga(order_id, data)
    return {**get_order(order_id), "prefect_flow_run_url": prefect_url}


@app.get("/orders/{order_id}")
def get_order(order_id: uuid.UUID):
    with get_conn() as conn:
        order = conn.execute("SELECT * FROM orders.orders WHERE id = %s", (order_id,)).fetchone()
        if not order:
            raise HTTPException(404, "Orden no encontrada")
        steps = conn.execute(
            """SELECT step, action, result, detail, at FROM orders.saga_log
               WHERE order_id = %s ORDER BY id""",
            (order_id,),
        ).fetchall()
    return {**order, "steps": steps}


@app.get("/orders")
def list_orders(user_id: int | None = None, limit: int = 20):
    sql = "SELECT * FROM orders.orders"
    params = []
    if user_id is not None:
        sql += " WHERE user_id = %s"
        params.append(user_id)
    sql += " ORDER BY created_at DESC LIMIT %s"
    params.append(limit)
    with get_conn() as conn:
        return conn.execute(sql, params).fetchall()


@app.post("/orders/recover")
def recover():
    """Dispara manualmente la recuperación de órdenes huérfanas."""
    return {"recovered": recover_orphans()}