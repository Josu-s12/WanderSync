"""Microservicio de Hoteles - WanderSync"""
import os
import uuid

import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

DATABASE_URL = os.environ["DATABASE_URL"]

app = FastAPI(title="WanderSync - Hotels Service")


def get_conn():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


class BookingIn(BaseModel):
    order_id: uuid.UUID
    offer_id: int
    nights: int = Field(default=1, ge=1, le=30)
    simulate_failure: bool = False   # para demostrar las compensaciones SAGA


@app.get("/health")
def health():
    return {"status": "ok", "service": "hotels"}


@app.get("/offers")
def list_offers(city: str | None = None, min_stars: int = 0, limit: int = 50):
    sql = "SELECT * FROM hotels.offers WHERE rooms > 0 AND stars >= %s"
    params = [min_stars]
    if city:
        sql += " AND city = %s"
        params.append(city)
    sql += " ORDER BY price_per_night LIMIT %s"
    params.append(limit)
    with get_conn() as conn:
        return conn.execute(sql, params).fetchall()


@app.get("/offers/{offer_id}")
def get_offer(offer_id: int):
    with get_conn() as conn:
        offer = conn.execute("SELECT * FROM hotels.offers WHERE id = %s", (offer_id,)).fetchone()
    if not offer:
        raise HTTPException(404, "Hotel no encontrado")
    return offer


@app.post("/bookings", status_code=201)
def create_booking(data: BookingIn):
    """Paso de la SAGA: reservar una habitación."""
    if data.simulate_failure:
        raise HTTPException(503, "Fallo simulado en el servicio de hoteles")

    with get_conn() as conn:
        # Idempotencia: si ya existe una reserva para esta orden, se devuelve la misma
        existing = conn.execute(
            "SELECT * FROM hotels.bookings WHERE order_id = %s AND status = 'CONFIRMED'",
            (data.order_id,),
        ).fetchone()
        if existing:
            return existing

        offer = conn.execute(
            "UPDATE hotels.offers SET rooms = rooms - 1 WHERE id = %s AND rooms > 0 RETURNING price_per_night",
            (data.offer_id,),
        ).fetchone()
        if not offer:
            raise HTTPException(409, "No hay habitaciones disponibles en este hotel")

        booking = conn.execute(
            """INSERT INTO hotels.bookings (id, order_id, offer_id, status)
               VALUES (%s, %s, %s, 'CONFIRMED') RETURNING *""",
            (uuid.uuid4(), data.order_id, data.offer_id),
        ).fetchone()

    return {**booking, "amount": offer["price_per_night"] * data.nights}


@app.post("/bookings/{booking_id}/cancel")
def cancel_booking(booking_id: uuid.UUID):
    """Transacción de compensación de la SAGA: liberar la habitación."""
    with get_conn() as conn:
        booking = conn.execute(
            "SELECT * FROM hotels.bookings WHERE id = %s FOR UPDATE", (booking_id,)
        ).fetchone()
        if not booking:
            raise HTTPException(404, "Reserva no encontrada")
        if booking["status"] == "CANCELLED":   # idempotente
            return booking

        conn.execute("UPDATE hotels.offers SET rooms = rooms + 1 WHERE id = %s", (booking["offer_id"],))
        booking = conn.execute(
            "UPDATE hotels.bookings SET status = 'CANCELLED' WHERE id = %s RETURNING *", (booking_id,)
        ).fetchone()

    return booking