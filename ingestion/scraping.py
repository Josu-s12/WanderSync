"""Extracción y limpieza de datos turísticos (se ejecuta dentro de los workers de Dask)."""
import re
from datetime import datetime, timedelta, timezone

from bs4 import BeautifulSoup

COLOMBIA_TZ = timezone(timedelta(hours=-5))


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def parse_price(text: str | None) -> int | None:
    """'$ 420.000 COP' | 'COP 420,000' | '  $420.000 ' | '420000 pesos'  ->  420000"""
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


def parse_count(text: str | None) -> int | None:
    """'Quedan 5 asientos' -> 5 | 'Último asiento!' -> 1 | 'Agotado' -> 0"""
    if not text:
        return None
    text = text.lower()
    if "agotado" in text:
        return 0
    if "último" in text or "ultimo" in text:
        return 1
    match = re.search(r"\d+", text)
    return int(match.group()) if match else None


def _text(card, cls):
    el = card.find(class_=cls)
    return clean_text(el.get_text()) if el else None


def parse_flights(html: str) -> list[dict]:
    rows = []
    for card in BeautifulSoup(html, "html.parser").select("div.result-card.flight"):
        route = _text(card, "route") or ""
        origin, _, destination = (p.strip() for p in route.partition("→"))
        departure = card.find("time", class_="departure")
        rows.append({
            "source_id": card.get("data-id"),
            "origin": origin or None,
            "destination": destination or None,
            "airline": _text(card, "airline"),
            "depart_at": datetime.fromisoformat(departure["datetime"]) if departure else None,
            "price": parse_price(_text(card, "price")),
            "seats": parse_count(_text(card, "seats")),
        })
    return rows


def parse_hotels(html: str) -> list[dict]:
    rows = []
    for card in BeautifulSoup(html, "html.parser").select("div.result-card.hotel"):
        stars = card.find(class_="stars")
        rows.append({
            "source_id": card.get("data-id"),
            "city": _text(card, "city"),
            "name": _text(card, "name"),
            "stars": stars.get_text().count("★") if stars else None,
            "price_per_night": parse_price(_text(card, "price")),
            "rooms": parse_count(_text(card, "rooms")),
        })
    return rows


def parse_cars(html: str) -> list[dict]:
    rows = []
    for card in BeautifulSoup(html, "html.parser").select("div.result-card.car"):
        rows.append({
            "source_id": card.get("data-id"),
            "city": _text(card, "city"),
            "model": _text(card, "model"),
            "category": _text(card, "category"),
            "price_per_day": parse_price(_text(card, "price")),
            "units": parse_count(_text(card, "units")),
        })
    return rows


# ----------------------------------------------------------------------
# Google Flights (fuente REAL de vuelos)
# Cada resultado trae un aria-label con una frase completa, por ejemplo:
#   "A partir de 184350 pesos colombianos. Vuelo directo de LATAM. ... Sale de Aeropuerto
#    Internacional El Dorado el domingo, noviembre 15 a las 18:15. Llega a ... Duración total: 1 h 30 min."
# ----------------------------------------------------------------------
GF_PRICE = re.compile(r"A partir de ([\d.,]+) pesos colombianos")
GF_AIRLINE = re.compile(r"Vuelo (directo|con (\d+) escalas?) de (.+?)\.(?: |$)")
GF_DEPARTURE = re.compile(r"Sale de .+? a las (\d{1,2}):(\d{2})")
GF_DEFAULT_SEATS = 9   # Google Flights no publica cupos: se asume un inventario para simular reservas


def parse_google_flights(html_text: str, origin: str, destination: str, date) -> list[dict]:
    """Extrae los vuelos de una página de resultados de Google Flights (deduplicados)."""
    soup = BeautifulSoup(html_text, "html.parser")
    rows = {}
    for el in soup.select("[aria-label]"):
        label = clean_text(el["aria-label"]) or ""
        if "pesos colombianos" not in label or "Sale de" not in label:
            continue
        price, airline, departure = GF_PRICE.search(label), GF_AIRLINE.search(label), GF_DEPARTURE.search(label)
        if not (price and airline and departure):
            continue
        stops = 0 if airline.group(1) == "directo" else int(airline.group(2))
        name = airline.group(3).strip() + ("" if stops == 0 else f" · {stops} escala{'s' if stops > 1 else ''}")
        hour, minute = int(departure.group(1)), int(departure.group(2))
        depart_at = datetime(date.year, date.month, date.day, hour, minute, tzinfo=COLOMBIA_TZ)
        key = f"gf-{slug(origin)}-{slug(destination)}-{date:%Y%m%d}-{slug(name)}-{hour:02d}{minute:02d}"
        rows[key] = {
            "source_id": key,
            "origin": origin,
            "destination": destination,
            "airline": name,
            "depart_at": depart_at,
            "price": parse_price(price.group(1)),
            "seats": GF_DEFAULT_SEATS,
        }
    return list(rows.values())


def slug(text: str) -> str:
    return "".join(ch for ch in text.upper() if ch.isalnum())


PARSERS = {"flights": parse_flights, "hotels": parse_hotels, "cars": parse_cars}


def clean(kind: str, html: str) -> tuple[list[dict], int]:
    """Devuelve (registros válidos, cantidad descartados por datos incompletos)."""
    rows = PARSERS[kind](html)
    valid = [r for r in rows if all(v is not None for v in r.values())
             and next(v for k, v in r.items() if k.startswith("price")) > 0]
    return valid, len(rows) - len(valid)
