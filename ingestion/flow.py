"""Flow de Prefect que orquesta la ingesta distribuida en Dask - WanderSync

Todos los datos son REALES; la fuente mock solo se usa como respaldo automático:
  Vuelos:   scrape-google-flights  (Google Flights, HTML del servidor)   --(si falla)--> fuente mock
  Hoteles:  scrape-google-hotels   (Google Hotels, HTML del servidor)    --(si falla)--> fuente mock
  Autos:    scrape-kayak-cars      (Kayak, renderizado con Chromium)     --(si falla)--> fuente mock
  Todo:     upsert-offers (retira las ofertas obsoletas)  +  market-stats (Dask Bag)
"""
import os
import random
import time
from datetime import date, timedelta

import dask.bag as db
import httpx
import psycopg
from prefect import flow, get_run_logger, task
from prefect.tasks import exponential_backoff
from prefect_dask import DaskTaskRunner, get_dask_client

import scraping

SOURCE_URL = os.environ["SOURCE_URL"]
DATABASE_URL = os.environ["DATABASE_URL"]
DASK_SCHEDULER = os.environ["DASK_SCHEDULER_ADDRESS"]
PREFECT_API_URL = os.environ["PREFECT_API_URL"]
DATA_SOURCE = os.getenv("DATA_SOURCE", "real")   # "real" (Google Flights/Hotels + Kayak) o "mock"

GOOGLE_FLIGHTS_URL = os.getenv("GOOGLE_FLIGHTS_URL", "https://www.google.com/travel/flights")
GOOGLE_HOTELS_URL = os.getenv("GOOGLE_HOTELS_URL", "https://www.google.com/travel/search")
KAYAK_CARS_URL = os.getenv("KAYAK_CARS_URL", "https://www.kayak.com.co/cars")
# Google y Kayak solo entregan los resultados a navegadores reconocidos
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36")
BROWSER_HEADERS = {"User-Agent": BROWSER_UA, "Accept-Language": "es-CO,es;q=0.9"}
RENTAL_DAYS = 3
IATA = {"Bogota": "BOG", "Medellin": "MDE", "Cali": "CLO",
        "Cartagena": "CTG", "Santa Marta": "SMR", "San Andres": "ADZ"}

DEFAULT_ROUTES = [
    ["Bogota", "Cartagena"], ["Bogota", "Santa Marta"], ["Bogota", "San Andres"],
    ["Medellin", "Cartagena"], ["Medellin", "Santa Marta"], ["Cali", "Cartagena"],
]
DEFAULT_CITIES = ["Cartagena", "Santa Marta", "San Andres"]

UPSERT_SQL = {
    "flights": """
        INSERT INTO flights.offers (source_id, origin, destination, airline, depart_at, price, seats)
        VALUES (%(source_id)s, %(origin)s, %(destination)s, %(airline)s, %(depart_at)s, %(price)s, %(seats)s)
        ON CONFLICT (source_id) DO UPDATE SET
          origin = EXCLUDED.origin, destination = EXCLUDED.destination, airline = EXCLUDED.airline,
          depart_at = EXCLUDED.depart_at, price = EXCLUDED.price, seats = EXCLUDED.seats,
          scraped_at = now()""",
    "hotels": """
        INSERT INTO hotels.offers (source_id, city, name, stars, price_per_night, rooms)
        VALUES (%(source_id)s, %(city)s, %(name)s, %(stars)s, %(price_per_night)s, %(rooms)s)
        ON CONFLICT (source_id) DO UPDATE SET
          city = EXCLUDED.city, name = EXCLUDED.name, stars = EXCLUDED.stars,
          price_per_night = EXCLUDED.price_per_night, rooms = EXCLUDED.rooms, scraped_at = now()""",
    "cars": """
        INSERT INTO cars.offers (source_id, city, model, category, price_per_day, units)
        VALUES (%(source_id)s, %(city)s, %(model)s, %(category)s, %(price_per_day)s, %(units)s)
        ON CONFLICT (source_id) DO UPDATE SET
          city = EXCLUDED.city, model = EXCLUDED.model, category = EXCLUDED.category,
          price_per_day = EXCLUDED.price_per_day, units = EXCLUDED.units, scraped_at = now()""",
}


# ----------------------------------------------------------------------
# Tareas (cada una se ejecuta en un worker de Dask)
# ----------------------------------------------------------------------
@task(name="scrape-page", tags=["scraping"],
      retries=4, retry_delay_seconds=exponential_backoff(backoff_factor=1), retry_jitter_factor=0.5,
      task_run_name="scrape {kind} {params} p{page}")
def scrape_page(kind: str, params: dict, page: int) -> str:
    """Descarga una página. Un 503/429 lanza excepción y Prefect REINTENTA con backoff."""
    logger = get_run_logger()
    r = httpx.get(f"{SOURCE_URL}/{kind}", params={**params, "page": page}, timeout=10,
                  headers={"User-Agent": "WanderSyncBot/1.0 (+proyecto academico)"})
    if r.status_code != 200:
        logger.warning("Fuente respondió %s en %s %s p%s -> se reintentará", r.status_code, kind, params, page)
    r.raise_for_status()
    return r.text


@task(name="scrape-google-flights", tags=["scraping", "google-flights"],
      retries=2, retry_delay_seconds=[10, 30], task_run_name="google-flights {origin}-{destination}")
def scrape_google_flights(origin: str, destination: str, travel_date: date) -> list[dict]:
    """Vuelos REALES desde Google Flights (resultados renderizados en el servidor)."""
    logger = get_run_logger()
    time.sleep(random.uniform(0, 3))   # cortesía: no lanzar todas las consultas en el mismo instante
    query = f"Flights to {IATA[destination]} from {IATA[origin]} on {travel_date:%Y-%m-%d} oneway"
    r = httpx.get(GOOGLE_FLIGHTS_URL, params={"q": query, "hl": "es", "gl": "co", "curr": "COP"},
                  headers=BROWSER_HEADERS, timeout=20, follow_redirects=True)
    check_google_response(r)
    rows = scraping.parse_google_flights(r.text, origin, destination, travel_date)
    if not rows:
        raise RuntimeError("Google Flights no devolvió vuelos (posible bloqueo o cambio de formato)")
    logger.info("Google Flights %s -> %s (%s): %s vuelos reales", origin, destination, travel_date, len(rows))
    return rows


@task(name="scrape-google-hotels", tags=["scraping", "google-hotels"],
      retries=2, retry_delay_seconds=[10, 30], task_run_name="google-hotels {city}")
def scrape_google_hotels(city: str) -> list[dict]:
    """Hoteles REALES desde Google Hotels (precio por noche y categoría)."""
    logger = get_run_logger()
    time.sleep(random.uniform(0, 3))
    r = httpx.get(GOOGLE_HOTELS_URL, params={"q": f"hoteles en {city}, Colombia", "hl": "es", "gl": "co",
                                             "curr": "COP"},
                  headers=BROWSER_HEADERS, timeout=20, follow_redirects=True)
    check_google_response(r)
    rows = scraping.parse_google_hotels(r.text, city)
    if not rows:
        raise RuntimeError("Google Hotels no devolvió hoteles (posible bloqueo o cambio de formato)")
    logger.info("Google Hotels %s: %s hoteles reales", city, len(rows))
    return rows


@task(name="scrape-kayak-cars", tags=["scraping", "kayak"],
      retries=2, retry_delay_seconds=[10, 30], task_run_name="kayak-cars {city}")
def scrape_kayak_cars(city: str, pickup: date) -> list[dict]:
    """Autos REALES desde Kayak. Kayak carga los resultados con JavaScript, así que la página
    se renderiza con un Chromium headless (Playwright) y se lee el HTML resultante."""
    from playwright.sync_api import sync_playwright

    logger = get_run_logger()
    dropoff = pickup + timedelta(days=RENTAL_DAYS)
    url = f"{KAYAK_CARS_URL}/{IATA[city]}/{pickup:%Y-%m-%d}/{dropoff:%Y-%m-%d}"
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-dev-shm-usage"])
        try:
            page = browser.new_page(user_agent=BROWSER_UA, locale="es-CO",
                                    viewport={"width": 1400, "height": 1000})
            page.goto(url, wait_until="domcontentloaded", timeout=45000)
            page.wait_for_selector('img[alt^="Tipo de auto:"]', timeout=30000)
            html = page.content()
        finally:
            browser.close()
    rows = scraping.parse_kayak_cars(html, city, RENTAL_DAYS)
    if not rows:
        raise RuntimeError("Kayak no devolvió autos (posible bloqueo o cambio de formato)")
    logger.info("Kayak %s (%s a %s): %s autos reales", city, pickup, dropoff, len(rows))
    return rows


def check_google_response(r: httpx.Response) -> None:
    r.raise_for_status()
    if "/sorry/" in str(r.url) or "unsupported" in str(r.url):
        raise RuntimeError(f"Google rechazó la consulta ({r.url.path})")


@task(name="parse-and-clean", task_run_name="clean {kind} p{page}")
def parse_and_clean(kind: str, html: str, page: int) -> list[dict]:
    rows, discarded = scraping.clean(kind, html)
    if discarded:
        get_run_logger().info("%s p%s: %s registro(s) descartado(s) por datos incompletos", kind, page, discarded)
    return rows


# Cómo se "retira" una oferta que ya no está en la fuente real: se deja sin cupos
RETIRE_SQL = {
    "flights": """UPDATE flights.offers SET seats = 0
                  WHERE origin = %s AND destination = %s AND NOT (source_id = ANY(%s))""",
    "hotels": "UPDATE hotels.offers SET rooms = 0 WHERE city = %s AND NOT (source_id = ANY(%s))",
    "cars": "UPDATE cars.offers SET units = 0 WHERE city = %s AND NOT (source_id = ANY(%s))",
}


@task(name="upsert-offers", retries=2, retry_delay_seconds=5, task_run_name="upsert {kind} ({n} filas)")
def upsert_offers(kind: str, rows: list[dict], n: int, replaced: list | None = None) -> int:
    """Guarda las ofertas. En cada ruta/ciudad actualizada con datos reales se retiran las que ya
    no están en la fuente (de otras fechas, de la fuente mock o semilla)."""
    if not rows:
        return 0
    with psycopg.connect(DATABASE_URL) as conn:
        # Mínimo privilegio: rol que solo puede escribir en las tablas de ofertas
        conn.execute("SET ROLE ingestion_writer")
        with conn.cursor() as cur:
            cur.executemany(UPSERT_SQL[kind], rows)
            for *place, ids in replaced or []:
                cur.execute(RETIRE_SQL[kind], (*place, ids))
    return len(rows)


@task(name="market-stats")
def market_stats(rows: list[dict]) -> dict:
    """Agregación distribuida con Dask Bag: precio promedio por destino y tipo de oferta."""
    def key(r):
        return f"{r['kind']}:{r.get('destination') or r.get('city')}"

    def price(r):
        return r.get("price") or r.get("price_per_night") or r.get("price_per_day")

    bag = (db.from_sequence(rows, npartitions=4)
           .foldby(key, lambda acc, r: (acc[0] + price(r), acc[1] + 1), (0, 0),
                   lambda a, b: (a[0] + b[0], a[1] + b[1]), (0, 0)))
    with get_dask_client() as client:
        stats = client.compute(bag).result()
    return {k: round(total / count) for k, (total, count) in sorted(stats)}


# ----------------------------------------------------------------------
# Flow
# ----------------------------------------------------------------------
@flow(name="ingesta-turistica", log_prints=True,
      task_runner=DaskTaskRunner(address=DASK_SCHEDULER))
def ingest_travel_data(routes: list[list[str]] = DEFAULT_ROUTES,
                       cities: list[str] = DEFAULT_CITIES,
                       pages: int = 3,
                       data_source: str = DATA_SOURCE,
                       days_ahead: int = 14) -> dict:
    logger = get_run_logger()
    travel_date = date.today() + timedelta(days=days_ahead)
    by_kind = {"flights": [], "hotels": [], "cars": []}
    replaced = {"flights": [], "hotels": [], "cars": []}
    real_ok = {"flights": 0, "hotels": 0, "cars": 0}
    fallback = []
    jobs = []   # páginas de la fuente mock (todo, o solo el respaldo de lo que falle)

    if data_source == "real":
        # 1) Fuentes REALES, todas en paralelo en los workers de Dask
        logger.info("Scraping real: %s rutas en Google Flights, %s ciudades en Google Hotels y Kayak "
                    "(fecha de viaje %s)", len(routes), len(cities), travel_date)
        real = [("flights", (o, d), scrape_google_flights.submit(o, d, travel_date)) for o, d in routes]
        real += [("hotels", (c,), scrape_google_hotels.submit(c)) for c in cities]
        real += [("cars", (c,), scrape_kayak_cars.submit(c, travel_date)) for c in cities]

        for kind, place, fut in real:
            fut.wait()
            if fut.state.is_completed():
                rows = fut.result()
                by_kind[kind] += rows
                replaced[kind].append([*place, [r["source_id"] for r in rows]])
                real_ok[kind] += 1
            else:
                # 2) Resiliencia: si la fuente real no responde, ese lugar se scrapea de la fuente mock
                logger.warning("La fuente real de %s no respondió para %s: se usa la fuente mock",
                               kind, " -> ".join(place))
                fallback.append(f"{kind}: {' -> '.join(place)}")
                params = {"origin": place[0], "destination": place[1]} if kind == "flights" else {"city": place[0]}
                jobs += [(kind, params, p) for p in range(1, pages + 1)]
    else:
        jobs += [("flights", {"origin": o, "destination": d}, p) for o, d in routes for p in range(1, pages + 1)]
        jobs += [(k, {"city": c}, p) for k in ("hotels", "cars") for c in cities for p in range(1, pages + 1)]

    # 3) Fuente mock en paralelo en Dask: scraping -> limpieza (cada limpieza espera solo a su página)
    if jobs:
        logger.info("Enviando %s páginas de la fuente mock al clúster de Dask (%s)", len(jobs), DASK_SCHEDULER)
    html = [scrape_page.submit(kind, params, page) for kind, params, page in jobs]
    cleaned = [parse_and_clean.submit(kind, h, page) for (kind, _, page), h in zip(jobs, html)]
    failed_pages = 0
    for (kind, _, _), fut in zip(jobs, cleaned):
        fut.wait()
        if fut.state.is_completed():
            by_kind[kind] += fut.result()
        else:
            failed_pages += 1

    # 4) Persistencia en paralelo  +  5) estadísticas de mercado con Dask Bag
    upserts = {k: upsert_offers.submit(k, rows, len(rows), replaced[k]) for k, rows in by_kind.items()}
    all_rows = [{**r, "kind": k} for k, rows in by_kind.items() for r in rows]
    stats = market_stats.submit(all_rows)

    summary = {
        "fuentes_reales_ok": {"vuelos (Google Flights)": f"{real_ok['flights']}/{len(routes)} rutas",
                              "hoteles (Google Hotels)": f"{real_ok['hotels']}/{len(cities)} ciudades",
                              "autos (Kayak)": f"{real_ok['cars']}/{len(cities)} ciudades"}
        if data_source == "real" else "fuente mock",
        "respaldo_mock": fallback,
        "fecha_viaje": str(travel_date),
        "paginas_mock": len(jobs),
        "paginas_fallidas": failed_pages,
        "guardados": {k: f.result() for k, f in upserts.items()},
        "precio_promedio": stats.result(),
    }
    logger.info("Resumen de la ingesta: %s", summary)
    if not any(by_kind.values()):
        raise RuntimeError("No se obtuvo ningún dato de ninguna fuente")
    return summary


def wait_for_prefect(timeout: int = 120) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(f"{PREFECT_API_URL}/health", timeout=3).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(2)
    raise RuntimeError("Prefect server no respondió")


if __name__ == "__main__":
    wait_for_prefect()
    # Ingesta inicial al arrancar, para no esperar 10 minutos a la primera ejecución programada
    try:
        ingest_travel_data()
    except Exception as exc:  # el deployment programado lo volverá a intentar
        print(f"La ingesta inicial falló: {exc}")
    # Deployment: se ejecuta cada 10 minutos y también se puede lanzar desde la UI de Prefect
    ingest_travel_data.serve(
        name="ingesta-cada-10-min",
        interval=600,
        description="Scraping distribuido en Dask: Google Flights, Google Hotels y Kayak (respaldo: fuente mock)",
        tags=["dask", "scraping"],
    )
