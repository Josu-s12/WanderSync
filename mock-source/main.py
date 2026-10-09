"""Mock Travel Source - WanderSync

Simula un portal público de viajes (estilo metabuscador) que se debe scrapear.
Reproduce a propósito los problemas de una fuente real:
  - Respuestas en HTML (no hay API), con paginación
  - Latencia variable
  - Fallos intermitentes (503) y limitación de peticiones (429)
  - Datos "sucios": precios en distintos formatos, espacios, textos en vez de números
    y algunos registros incompletos que el pipeline debe limpiar o descartar
  - Precios que cambian con el tiempo (cada PRICE_WINDOW_SECONDS)
"""
import asyncio
import hashlib
import os
import random
import time
from datetime import datetime, timedelta, timezone
from html import escape
from urllib.parse import urlencode

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0.2"))
MAX_LATENCY = float(os.getenv("MAX_LATENCY", "1.0"))
PRICE_WINDOW_SECONDS = int(os.getenv("PRICE_WINDOW_SECONDS", "600"))
PAGES = 3
PER_PAGE = 8

AIRLINES = ["Avianca", "LATAM", "Wingo", "JetSMART", "Clic Air"]
HOTEL_PREFIX = ["Hotel", "Casa", "Hostal", "Resort", "Suites"]
HOTEL_NAMES = ["del Mar", "Colonial", "Sol Caribe", "Las Palmas", "Bahia", "El Faro", "La Muralla",
               "Brisas", "San Pedro", "Coral"]
CAR_MODELS = [("Kia Picanto", "Economico"), ("Renault Logan", "Economico"), ("Chevrolet Onix", "Compacto"),
              ("Mazda 3", "Compacto"), ("Mazda CX-30", "SUV"), ("Toyota Fortuner", "SUV"),
              ("Nissan Frontier", "Pickup")]
TZ = timezone(timedelta(hours=-5))

app = FastAPI(title="Mock Travel Source (sitio a scrapear)")


def rng_for(*parts) -> random.Random:
    """Generador determinista: los mismos datos dentro de una ventana de tiempo."""
    window = int(time.time() // PRICE_WINDOW_SECONDS)
    seed = hashlib.sha256("|".join(map(str, (*parts, window))).encode()).hexdigest()
    return random.Random(seed)


def slug(text: str) -> str:
    return "".join(ch for ch in text.upper() if ch.isalnum())


def stable_rng(*parts) -> random.Random:
    """Generador determinista que NO cambia con el tiempo (identidad de la oferta)."""
    return random.Random(hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest())


def messy_price(value: int, r: random.Random) -> str:
    """Cada portal formatea distinto: así obligamos a limpiar los datos."""
    style = r.randint(0, 3)
    if style == 0:
        return f"$ {value:,.0f} COP".replace(",", ".")
    if style == 1:
        return f"COP {value:,.0f}"
    if style == 2:
        return f"  ${value:,.0f}  ".replace(",", ".")
    return f"{value} pesos"


async def simulate_network():
    await asyncio.sleep(random.uniform(0.05, MAX_LATENCY))
    roll = random.random()
    if roll < FAILURE_RATE / 2:
        raise HTTPException(503, "Service temporarily unavailable")
    if roll < FAILURE_RATE:
        raise HTTPException(429, "Too many requests", headers={"Retry-After": "2"})


def page_html(title: str, cards: list[str], page: int, base: str) -> str:
    nav = f'<a class="next" href="{escape(base)}&amp;page={page + 1}">Siguiente</a>' if page < PAGES else ""
    return f"""<!doctype html><html lang="es"><head><meta charset="utf-8"><title>{escape(title)}</title></head>
<body><h1>{escape(title)}</h1><p class="meta">Página {page} de {PAGES}</p>
<section id="results">{''.join(cards)}</section><nav>{nav}</nav></body></html>"""


@app.get("/health")
def health():
    return {"status": "ok", "failure_rate": FAILURE_RATE}


@app.get("/", response_class=HTMLResponse)
def index():
    return """<!doctype html><html lang="es"><head><meta charset="utf-8"><title>Mock Travel Source</title></head><body>
<h1>Mock Travel Source</h1><ul>
<li><a href="/flights?origin=Bogota&destination=Cartagena">Vuelos Bogota - Cartagena</a></li>
<li><a href="/hotels?city=Cartagena">Hoteles en Cartagena</a></li>
<li><a href="/cars?city=Cartagena">Autos en Cartagena</a></li></ul></body></html>"""


@app.get("/flights", response_class=HTMLResponse)
async def flights(origin: str, destination: str, page: int = Query(1, ge=1, le=PAGES)):
    await simulate_network()
    cards = []
    for i in range(PER_PAGE):
        n = (page - 1) * PER_PAGE + i
        ident = stable_rng("F", origin, destination, n)
        r = rng_for("F", origin, destination, n)
        airline = ident.choice(AIRLINES)
        depart = (datetime.now(TZ).replace(minute=0, second=0, microsecond=0)
                  + timedelta(days=ident.randint(2, 30), hours=ident.randint(0, 23)))
        price = r.randint(18, 90) * 10_000
        seats = r.randint(0, 9)
        seats_txt = "Último asiento!" if seats == 1 else ("Agotado" if seats == 0 else f"Quedan {seats} asientos")
        price_html = "" if r.random() < 0.05 else f'<span class="price">{escape(messy_price(price, r))}</span>'
        cards.append(f"""
<div class="result-card flight" data-id="mt-f-{slug(origin)}-{slug(destination)}-{n:03d}">
  <span class="airline"> {escape(airline)} </span>
  <span class="route">{escape(origin)} → {escape(destination)}</span>
  <time class="departure" datetime="{depart.isoformat()}">{depart.strftime('%d/%m %H:%M')}</time>
  {price_html}
  <span class="seats">{seats_txt}</span>
</div>""")
    base = "/flights?" + urlencode({"origin": origin, "destination": destination})
    return page_html(f"Vuelos {origin} - {destination}", cards, page, base)


@app.get("/hotels", response_class=HTMLResponse)
async def hotels(city: str, page: int = Query(1, ge=1, le=PAGES)):
    await simulate_network()
    cards = []
    for i in range(PER_PAGE):
        n = (page - 1) * PER_PAGE + i
        ident = stable_rng("H", city, n)
        r = rng_for("H", city, n)
        name = f"{ident.choice(HOTEL_PREFIX)} {ident.choice(HOTEL_NAMES)}"
        stars = ident.randint(2, 5)
        price = (stars * r.randint(4, 12)) * 10_000
        rooms = r.randint(0, 6)
        price_html = "" if r.random() < 0.05 else f'<span class="price">{escape(messy_price(price, r))}</span>'
        cards.append(f"""
<div class="result-card hotel" data-id="mt-h-{slug(city)}-{n:03d}">
  <h2 class="name">  {escape(name)} </h2>
  <span class="city">{escape(city)}</span>
  <span class="stars" title="{stars} estrellas">{'★' * stars}</span>
  {price_html}<span class="per">/ noche</span>
  <span class="rooms">{rooms} habitaciones disponibles</span>
</div>""")
    return page_html(f"Hoteles en {city}", cards, page, "/hotels?" + urlencode({"city": city}))


@app.get("/cars", response_class=HTMLResponse)
async def cars(city: str, page: int = Query(1, ge=1, le=PAGES)):
    await simulate_network()
    cards = []
    for i in range(PER_PAGE):
        n = (page - 1) * PER_PAGE + i
        ident = stable_rng("C", city, n)
        r = rng_for("C", city, n)
        model, category = ident.choice(CAR_MODELS)
        price = r.randint(8, 35) * 10_000
        units = r.randint(0, 5)
        price_html = "" if r.random() < 0.05 else f'<span class="price">{escape(messy_price(price, r))}</span>'
        cards.append(f"""
<div class="result-card car" data-id="mt-c-{slug(city)}-{n:03d}">
  <span class="model">{escape(model)}</span>
  <span class="category"> {escape(category)} </span>
  <span class="city">{escape(city)}</span>
  {price_html}<span class="per">/ día</span>
  <span class="units">{units} disponibles</span>
</div>""")
    return page_html(f"Autos en {city}", cards, page, "/cars?" + urlencode({"city": city}))
