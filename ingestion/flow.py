"""Flow de Prefect que orquesta la ingesta distribuida en Dask - WanderSync

  Vuelos:          scrape_google_flights (Google Flights REAL, reintentos)  --(si falla)--> fuente mock
  Hoteles y autos: scrape_page (fuente mock, reintentos con backoff) -> parse_and_clean
  Todo:            upsert_offers (Dask)  +  market_stats (Dask Bag)
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
FLIGHT_SOURCE = os.getenv("FLIGHT_SOURCE", "google")   # "google" (real) o "mock"

GOOGLE_FLIGHTS_URL = os.getenv("GOOGLE_FLIGHTS_URL", "https://www.google.com/travel/flights")
# Google Flights solo entrega los resultados a navegadores reconocidos
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36")
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
                  headers={"User-Agent": BROWSER_UA, "Accept-Language": "es-CO,es;q=0.9"},
                  timeout=20, follow_redirects=True)
    r.raise_for_status()
    if "/sorry/" in str(r.url) or "unsupported" in str(r.url):
        raise RuntimeError(f"Google rechazó la consulta ({r.url.path})")
    rows = scraping.parse_google_flights(r.text, origin, destination, travel_date)
    if not rows:
        raise RuntimeError("Google Flights no devolvió vuelos (posible bloqueo o cambio de formato)")
    logger.info("Google Flights %s -> %s (%s): %s vuelos reales", origin, destination, travel_date, len(rows))
    return rows


@task(name="parse-and-clean", task_run_name="clean {kind} p{page}")
def parse_and_clean(kind: str, html: str, page: int) -> list[dict]:
    rows, discarded = scraping.clean(kind, html)
    if discarded:
        get_run_logger().info("%s p%s: %s registro(s) descartado(s) por datos incompletos", kind, page, discarded)
    return rows


@task(name="upsert-offers", retries=2, retry_delay_seconds=5, task_run_name="upsert {kind} ({n} filas)")
def upsert_offers(kind: str, rows: list[dict], n: int, replaced_routes: list | None = None) -> int:
    if not rows:
        return 0
    with psycopg.connect(DATABASE_URL) as conn:
        # Mínimo privilegio: rol que solo puede escribir en las tablas de ofertas
        conn.execute("SET ROLE ingestion_writer")
        with conn.cursor() as cur:
            cur.executemany(UPSERT_SQL[kind], rows)
            # En las rutas actualizadas con datos reales se retiran las ofertas que ya no están
            # en la fuente (de otras fechas, de la fuente mock o semilla): quedan sin cupos
            for origin, destination, ids in replaced_routes or []:
                cur.execute("""UPDATE flights.offers SET seats = 0
                               WHERE origin = %s AND destination = %s AND NOT (source_id = ANY(%s))""",
                            (origin, destination, ids))
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
                       flight_source: str = FLIGHT_SOURCE,
                       days_ahead: int = 14) -> dict:
    logger = get_run_logger()
    travel_date = date.today() + timedelta(days=days_ahead)

    # 1) Vuelos REALES desde Google Flights (una consulta por ruta, en paralelo en Dask)
    google = {}
    if flight_source == "google":
        logger.info("Consultando Google Flights para %s rutas (fecha de viaje %s)", len(routes), travel_date)
        google = {(o, d): scrape_google_flights.submit(o, d, travel_date) for o, d in routes}

    # 2) Hoteles y autos desde la fuente mock (Booking.com bloquea el acceso automatizado)
    jobs = [(kind, {"city": c}, p) for kind in ("hotels", "cars") for c in cities for p in range(1, pages + 1)]
    if flight_source != "google":
        jobs += [("flights", {"origin": o, "destination": d}, p) for o, d in routes for p in range(1, pages + 1)]

    by_kind = {"flights": [], "hotels": [], "cars": []}
    replaced_routes, fallback_routes = [], []
    for (o, d), fut in google.items():
        fut.wait()
        if fut.state.is_completed():
            rows = fut.result()
            by_kind["flights"] += rows
            replaced_routes.append([o, d, [r["source_id"] for r in rows]])
        else:   # Resiliencia: si Google no responde, esa ruta se scrapea de la fuente mock
            logger.warning("Google Flights no respondió para %s -> %s: se usa la fuente mock", o, d)
            fallback_routes.append([o, d])
            jobs += [("flights", {"origin": o, "destination": d}, p) for p in range(1, pages + 1)]

    logger.info("Enviando %s páginas de la fuente mock al clúster de Dask (%s)", len(jobs), DASK_SCHEDULER)
    # 3) Scraping de la fuente mock en paralelo en los workers de Dask
    html = [scrape_page.submit(kind, params, page) for kind, params, page in jobs]
    # 4) Limpieza en paralelo (cada una espera solo a su página)
    cleaned = [parse_and_clean.submit(kind, h, page) for (kind, _, page), h in zip(jobs, html)]

    # 5) Se agrupan los resultados válidos por tipo (las páginas que fallaron tras
    #    agotar los reintentos se omiten sin tumbar toda la ingesta)
    failed_pages = 0
    for (kind, _, _), fut in zip(jobs, cleaned):
        fut.wait()
        if fut.state.is_completed():
            by_kind[kind] += fut.result()
        else:
            failed_pages += 1

    # 6) Persistencia en paralelo
    upserts = {k: upsert_offers.submit(k, rows, len(rows), replaced_routes if k == "flights" else None)
               for k, rows in by_kind.items()}
    # 5) Estadísticas de mercado con Dask Bag
    all_rows = [{**r, "kind": k} for k, rows in by_kind.items() for r in rows]
    stats = market_stats.submit(all_rows)

    summary = {
        "vuelos_google_flights": {"rutas_ok": len(replaced_routes), "rutas_con_respaldo_mock": fallback_routes,
                                  "fecha_viaje": str(travel_date)},
        "paginas_mock": len(jobs),
        "paginas_fallidas": failed_pages,
        "guardados": {k: f.result() for k, f in upserts.items()},
        "precio_promedio": stats.result(),
    }
    logger.info("Resumen de la ingesta: %s", summary)
    if failed_pages == len(jobs) and not replaced_routes:
        raise RuntimeError("Ninguna página pudo descargarse")
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
        description="Scraping distribuido en Dask: vuelos reales de Google Flights, hoteles y autos",
        tags=["dask", "scraping"],
    )
