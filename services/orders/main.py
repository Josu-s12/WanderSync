"""Microservicio de Órdenes y Facturación - Orquestador SAGA - WanderSync"""
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
from pydantic import BaseModel, Field

DATABASE_URL = os.environ["DATABASE_URL"]
SERVICES = {
    "FLIGHT": os.environ["FLIGHTS_URL"],
    "HOTEL": os.environ["HOTELS_URL"],
    "CAR": os.environ["CARS_URL"],
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SAGA] %(message)s")
log = logging.getLogger("saga")


def get_conn():
    # autocommit: cada registro del saga_log queda guardado al instante
    return psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=True)


class StepFailed(Exception):
    def __init__(self, step: str, reason: str):
        super().__init__(f"{step}: {reason}")
        self.step = step
        self.reason = reason


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


def cancel(step: str, booking_id, retries: int = 3) -> None:
    """Compensación con reintentos y backoff: una compensación DEBE terminar bien."""
    for attempt in range(1, retries + 1):
        try:
            r = httpx.post(f"{SERVICES[step]}/bookings/{booking_id}/cancel", timeout=5)
            if r.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        log.warning("Reintento %s/%s compensando %s %s", attempt, retries, step, booking_id)
        time.sleep(attempt)
    raise StepFailed(step, "no se pudo compensar tras varios reintentos")


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
# Orquestador SAGA
# ----------------------------------------------------------------------
def compensate(conn, order_id, completed: list[tuple[str, str]]) -> bool:
    """Deshace los pasos ya confirmados en ORDEN INVERSO."""
    all_ok = True
    for step, booking_id in reversed(completed):
        try:
            cancel(step, booking_id)
            log_step(conn, order_id, step, "COMPENSATE", "OK", "reserva cancelada", booking_id)
        except StepFailed as e:
            all_ok = False
            log_step(conn, order_id, step, "COMPENSATE", "FAILED", e.reason, booking_id)
    return all_ok


def run_saga(order_id: uuid.UUID, data: OrderIn) -> None:
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
                booking = book(step, order_id, {**payload, "simulate_failure": data.fail_at == step})
                completed.append((step, booking["id"]))
                total += Decimal(str(booking["amount"]))
                log_step(conn, order_id, step, "EXECUTE", "OK", f"monto={booking['amount']}", booking["id"])

            payment_id = charge(conn, order_id, total, data.fail_at == "PAYMENT")
            log_step(conn, order_id, "PAYMENT", "EXECUTE", "OK", f"total={total}", payment_id)

        except StepFailed as e:
            log_step(conn, order_id, e.step, "EXECUTE", "FAILED", e.reason)
            ok = compensate(conn, order_id, completed)
            status = "COMPENSATED" if ok else "COMPENSATION_PENDING"
            conn.execute("UPDATE orders.orders SET status = %s WHERE id = %s", (status, order_id))
            return

        conn.execute(
            "UPDATE orders.orders SET status = 'CONFIRMED', total = %s WHERE id = %s", (total, order_id)
        )


def recover_orphans() -> int:
    """Evita "reservas huérfanas": si el servicio se cayó a mitad de una SAGA,
    al arrancar compensa las órdenes que quedaron a medias."""
    recovered = 0
    with get_conn() as conn:
        orders = conn.execute(
            "SELECT id FROM orders.orders WHERE status IN ('PENDING', 'COMPENSATION_PENDING')"
        ).fetchall()
        for order in orders:
            pending = conn.execute(
                """SELECT step, booking_id FROM orders.saga_log l
                   WHERE l.order_id = %s AND l.action = 'EXECUTE' AND l.result = 'OK'
                     AND l.step <> 'PAYMENT'
                     AND NOT EXISTS (SELECT 1 FROM orders.saga_log c
                                     WHERE c.order_id = l.order_id AND c.booking_id = l.booking_id
                                       AND c.action = 'COMPENSATE' AND c.result = 'OK')
                   ORDER BY l.id""",
                (order["id"],),
            ).fetchall()
            ok = compensate(conn, order["id"], [(p["step"], p["booking_id"]) for p in pending])
            status = "COMPENSATED" if ok else "COMPENSATION_PENDING"
            conn.execute("UPDATE orders.orders SET status = %s WHERE id = %s", (status, order["id"]))
            recovered += 1
    return recovered


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
    run_saga(order_id, data)
    return get_order(order_id)


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