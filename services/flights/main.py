"""Microservicio de Vuelos - WanderSync"""
import os
import uuid

import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

DATABASE_URL = os.environ["DATABASE_URL"]

app = FastAPI(title="WanderSync - Flights Service")


def get_conn():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


class BookingIn(BaseModel):
    order_id: uuid.UUID
    offer_id: int
    simulate_failure: bool = False   # para demostrar las compensaciones SAGA


@app.get("/health")
def health():
    return {"status": "ok", "service": "flights"}


@app.get("/offers")
def list_offers(origin: str | None = None, destination: str | None = None, limit: int = 50):
    sql = "SELECT * FROM flights.offers WHERE seats > 0"
    params = []
    if origin:
        sql += " AND origin = %s"
        params.append(origin)
    if destination:
        sql += " AND destination = %s"
        params.append(destination)
    sql += " ORDER BY price LIMIT %s"
    params.append(limit)
    with get_conn() as conn:
        return conn.execute(sql, params).fetchall()


@app.get("/offers/{offer_id}")
def get_offer(offer_id: int):
    with get_conn() as conn:
        offer = conn.execute("SELECT * FROM flights.offers WHERE id = %s", (offer_id,)).fetchone()
    if not offer:
        raise HTTPException(404, "Vuelo no encontrado")
    return offer


@app.post("/bookings", status_code=201)
def create_booking(data: BookingIn):
    """Paso de la SAGA: reservar un asiento."""
    if data.simulate_failure:
        raise HTTPException(503, "Fallo simulado en el servicio de vuelos")

    with get_conn() as conn:
        # Idempotencia: si ya existe una reserva para esta orden, se devuelve la misma
        existing = conn.execute(
            "SELECT * FROM flights.bookings WHERE order_id = %s AND status = 'CONFIRMED'",
            (data.order_id,),
        ).fetchone()
        if existing:
            return existing

        offer = conn.execute(
            "UPDATE flights.offers SET seats = seats - 1 WHERE id = %s AND seats > 0 RETURNING price",
            (data.offer_id,),
        ).fetchone()
        if not offer:
            raise HTTPException(409, "No hay asientos disponibles para este vuelo")

        booking = conn.execute(
            """INSERT INTO flights.bookings (id, order_id, offer_id, status)
               VALUES (%s, %s, %s, 'CONFIRMED') RETURNING *""",
            (uuid.uuid4(), data.order_id, data.offer_id),
        ).fetchone()

    return {**booking, "amount": offer["price"]}


@app.post("/bookings/{booking_id}/cancel")
def cancel_booking(booking_id: uuid.UUID):
    """Transacción de compensación de la SAGA: liberar el asiento."""
    with get_conn() as conn:
        booking = conn.execute(
            "SELECT * FROM flights.bookings WHERE id = %s FOR UPDATE", (booking_id,)
        ).fetchone()
        if not booking:
            raise HTTPException(404, "Reserva no encontrada")
        if booking["status"] == "CANCELLED":   # idempotente
            return booking

        conn.execute("UPDATE flights.offers SET seats = seats + 1 WHERE id = %s", (booking["offer_id"],))
        booking = conn.execute(
            "UPDATE flights.bookings SET status = 'CANCELLED' WHERE id = %s RETURNING *", (booking_id,)
        ).fetchone()

    return booking