# Reporte de auditoría de dependencias (Supply Chain Security)

- **Herramienta:** `pip-audit 2.10.1` (base de datos de vulnerabilidades de PyPI / OSV)
- **Fecha de ejecución:** 2026-10-10 16:11 UTC
- **Alcance:** dependencias directas **y transitivas** de cada microservicio, resueltas desde su `requirements.txt`
- **Cómo reproducir:** `./scripts/audit.ps1` (Windows) o `./scripts/audit.sh` (Linux/macOS)

## Resumen por servicio

| Servicio | Archivo | Paquetes auditados | Vulnerabilidades | Resultado |
|---|---|---|---|---|
| flights | `services/flights/requirements.txt` | 22 | 0 | ✅ Sin vulnerabilidades conocidas |
| hotels | `services/hotels/requirements.txt` | 22 | 0 | ✅ Sin vulnerabilidades conocidas |
| cars | `services/cars/requirements.txt` | 22 | 0 | ✅ Sin vulnerabilidades conocidas |
| orders | `services/orders/requirements.txt` | 109 | 0 | ✅ Sin vulnerabilidades conocidas |
| gateway | `gateway/requirements.txt` | 35 | 0 | ✅ Sin vulnerabilidades conocidas |
| mock-source | `mock-source/requirements.txt` | 20 | 0 | ✅ Sin vulnerabilidades conocidas |
| ingestion | `ingestion/requirements.txt` | 125 | 0 | ✅ Sin vulnerabilidades conocidas |

**Total de vulnerabilidades en el proyecto: 0**

## Control positivo (verificación de la herramienta)

Para demostrar que la auditoría realmente detecta riesgos, se auditó a propósito `requests==2.25.0`, una versión antigua. Resultado: **13 vulnerabilidad(es) detectada(s)**.

| Paquete | Versión | ID | Alias | Corregido en |
|---|---|---|---|---|
| requests | 2.25.0 | PYSEC-2023-74 | GHSA-j8r2-6x86-q33q, CVE-2023-32681 | 2.31.0 |
| requests | 2.25.0 | PYSEC-2026-1873 | CVE-2024-35195, GHSA-9wx4-h78v-vm56 | 2.32.0 |
| requests | 2.25.0 | PYSEC-2026-1872 | GHSA-9hjg-9r4m-mvj7, CVE-2024-47081 | 2.32.4 |
| requests | 2.25.0 | PYSEC-2026-2275 | CVE-2026-25645, GHSA-gc5v-m9x4-r6x2 | 2.33.0 |
| idna | 2.10 | PYSEC-2024-60 | GHSA-jjg7-2v4v-x38h, CVE-2024-3651 | 3.7 |
| idna | 2.10 | PYSEC-2026-215 | GHSA-65pc-fj4g-8rjx, CVE-2026-45409 | 3.15 |
| urllib3 | 1.26.20 | PYSEC-2026-1999 | GHSA-pq67-6m6q-mj2v, CVE-2025-50181 | 2.5.0 |
| urllib3 | 1.26.20 | PYSEC-2026-1998 | CVE-2025-66418, GHSA-gm62-xv2j-4w53 | 2.6.0 |
| urllib3 | 1.26.20 | PYSEC-2026-1994 | GHSA-2xpw-w6gg-jr37, CVE-2025-66471 | 2.6.0 |
| urllib3 | 1.26.20 | PYSEC-2026-1996 | CVE-2026-21441, GHSA-38jv-5279-wg99 | 2.6.3 |
| urllib3 | 1.26.20 | PYSEC-2026-141 | GHSA-qccp-gfcp-xxvc, CVE-2026-44431 | 2.7.0 |
| urllib3 | 1.26.20 | PYSEC-2026-4177 | CVE-2026-97689, GHSA-vxq7-64xx-v4gw | 2.8.0 |
| urllib3 | 1.26.20 | PYSEC-2026-4175 | GHSA-8988-9cw3-xx77, CVE-2026-97687 | 2.8.0 |

## Medidas de seguridad de la cadena de suministro aplicadas

1. **Versiones fijadas (`==`)** en todos los `requirements.txt`: builds reproducibles, sin actualizaciones silenciosas.
2. **Auditoría automatizada** con `pip-audit` sobre el árbol completo de dependencias (incluye transitivas).
3. **Imágenes base oficiales y mínimas** (`python:3.12-slim`, `nginx-unprivileged:alpine`) con etiqueta fija.
4. **Contenedores sin root** (`USER appuser` / nginx-unprivileged): limita el impacto de una RCE en una librería.
5. **Superficie mínima de red**: solo el frontend, Prefect UI, el dashboard de Dask y la fuente mock se publican al host.
6. **Procedimiento ante hallazgos**: subir a la versión indicada en *Corregido en*, reconstruir (`docker compose up -d --build`) y volver a ejecutar la auditoría hasta obtener 0 vulnerabilidades.

## Detalle de paquetes auditados

<details><summary><b>flights</b> (22 paquetes)</summary>

| Paquete | Versión | Vulnerabilidades |
|---|---|---|
| annotated-doc | 0.0.5 | 0 |
| annotated-types | 0.8.0 | 0 |
| anyio | 4.15.1 | 0 |
| click | 8.5.0 | 0 |
| fastapi | 0.142.2 | 0 |
| h11 | 0.16.0 | 0 |
| httptools | 0.9.0 | 0 |
| idna | 3.20 | 0 |
| opentelemetry-api | 1.45.1 | 0 |
| psycopg | 3.3.6 | 0 |
| psycopg-binary | 3.3.6 | 0 |
| pydantic | 2.14.0 | 0 |
| pydantic-core | 2.50.0 | 0 |
| python-dotenv | 1.2.4 | 0 |
| pyyaml | 6.0.3 | 0 |
| starlette | 1.7.0 | 0 |
| typing-extensions | 4.16.0 | 0 |
| typing-inspection | 0.4.4 | 0 |
| uvicorn | 0.54.0 | 0 |
| uvloop | 0.23.0 | 0 |
| watchfiles | 1.3.0 | 0 |
| websockets | 17.2 | 0 |

</details>

<details><summary><b>hotels</b> (22 paquetes)</summary>

| Paquete | Versión | Vulnerabilidades |
|---|---|---|
| annotated-doc | 0.0.5 | 0 |
| annotated-types | 0.8.0 | 0 |
| anyio | 4.15.1 | 0 |
| click | 8.5.0 | 0 |
| fastapi | 0.142.2 | 0 |
| h11 | 0.16.0 | 0 |
| httptools | 0.9.0 | 0 |
| idna | 3.20 | 0 |
| opentelemetry-api | 1.45.1 | 0 |
| psycopg | 3.3.6 | 0 |
| psycopg-binary | 3.3.6 | 0 |
| pydantic | 2.14.0 | 0 |
| pydantic-core | 2.50.0 | 0 |
| python-dotenv | 1.2.4 | 0 |
| pyyaml | 6.0.3 | 0 |
| starlette | 1.7.0 | 0 |
| typing-extensions | 4.16.0 | 0 |
| typing-inspection | 0.4.4 | 0 |
| uvicorn | 0.54.0 | 0 |
| uvloop | 0.23.0 | 0 |
| watchfiles | 1.3.0 | 0 |
| websockets | 17.2 | 0 |

</details>

<details><summary><b>cars</b> (22 paquetes)</summary>

| Paquete | Versión | Vulnerabilidades |
|---|---|---|
| annotated-doc | 0.0.5 | 0 |
| annotated-types | 0.8.0 | 0 |
| anyio | 4.15.1 | 0 |
| click | 8.5.0 | 0 |
| fastapi | 0.142.2 | 0 |
| h11 | 0.16.0 | 0 |
| httptools | 0.9.0 | 0 |
| idna | 3.20 | 0 |
| opentelemetry-api | 1.45.1 | 0 |
| psycopg | 3.3.6 | 0 |
| psycopg-binary | 3.3.6 | 0 |
| pydantic | 2.14.0 | 0 |
| pydantic-core | 2.50.0 | 0 |
| python-dotenv | 1.2.4 | 0 |
| pyyaml | 6.0.3 | 0 |
| starlette | 1.7.0 | 0 |
| typing-extensions | 4.16.0 | 0 |
| typing-inspection | 0.4.4 | 0 |
| uvicorn | 0.54.0 | 0 |
| uvloop | 0.23.0 | 0 |
| watchfiles | 1.3.0 | 0 |
| websockets | 17.2 | 0 |

</details>

<details><summary><b>orders</b> (109 paquetes)</summary>

| Paquete | Versión | Vulnerabilidades |
|---|---|---|
| aiosqlite | 0.22.1 | 0 |
| alembic | 1.20.0 | 0 |
| amplitude-analytics | 1.2.3 | 0 |
| annotated-doc | 0.0.5 | 0 |
| annotated-types | 0.8.0 | 0 |
| anyio | 4.15.1 | 0 |
| apprise | 1.13.1 | 0 |
| asgi-lifespan | 2.1.0 | 0 |
| asyncpg | 0.32.0 | 0 |
| attrs | 26.1.0 | 0 |
| beartype | 0.23.0 | 0 |
| burner-redis | 0.1.7 | 0 |
| cachetools | 7.2.1 | 0 |
| certifi | 2026.7.22 | 0 |
| cffi | 2.1.1 | 0 |
| charset-normalizer | 3.5.2 | 0 |
| click | 8.5.0 | 0 |
| cloudpickle | 3.1.2 | 0 |
| colorama | 0.4.6 | 0 |
| coolname | 5.0.0 | 0 |
| cronsim | 2.7 | 0 |
| cryptography | 50.0.2 | 0 |
| cyclopts | 5.2.0 | 0 |
| dateparser | 1.4.3 | 0 |
| docker | 7.2.0 | 0 |
| docstring-parser | 0.18.0 | 0 |
| exceptiongroup | 1.3.1 | 0 |
| fastapi | 0.142.2 | 0 |
| fsspec | 2026.9.0 | 0 |
| graphviz | 0.21 | 0 |
| greenlet | 3.5.6 | 0 |
| griffe | 2.3.2 | 0 |
| griffecli | 2.3.2 | 0 |
| griffelib | 2.3.2 | 0 |
| h11 | 0.16.0 | 0 |
| h2 | 4.4.1 | 0 |
| hpack | 4.2.0 | 0 |
| httpcore | 1.0.9 | 0 |
| httptools | 0.9.0 | 0 |
| httpx | 0.28.1 | 0 |
| humanize | 4.16.0 | 0 |
| hyperframe | 6.1.0 | 0 |
| idna | 3.20 | 0 |
| jinja2 | 3.1.6 | 0 |
| jinja2-humanize-extension | 0.4.0 | 0 |
| jsonpatch | 1.34 | 0 |
| jsonpointer | 3.2.0 | 0 |
| jsonschema | 4.26.0 | 0 |
| jsonschema-specifications | 2025.9.1 | 0 |
| mako | 1.4.3 | 0 |
| markdown | 3.11 | 0 |
| markdown-it-py | 4.2.0 | 0 |
| markupsafe | 3.0.4 | 0 |
| mdurl | 0.1.2 | 0 |
| oauthlib | 4.0.0 | 0 |
| opentelemetry-api | 1.45.1 | 0 |
| orjson | 3.13.0 | 0 |
| pathspec | 1.1.1 | 0 |
| pendulum | 3.2.0 | 0 |
| pluggy | 1.6.0 | 0 |
| prefect | 3.8.8 | 0 |
| prometheus-client | 0.26.0 | 0 |
| psycopg | 3.3.6 | 0 |
| psycopg-binary | 3.3.6 | 0 |
| py-key-value-aio | 0.4.6 | 0 |
| pycparser | 3.11 | 0 |
| pydantic | 2.14.0 | 0 |
| pydantic-core | 2.50.0 | 0 |
| pydantic-extra-types | 2.11.1 | 0 |
| pydantic-settings | 2.15.0 | 0 |
| pydocket | 0.27.0 | 0 |
| pygments | 2.21.0 | 0 |
| python-dateutil | 2.9.0.post0 | 0 |
| python-dotenv | 1.2.4 | 0 |
| python-json-logger | 4.2.0 | 0 |
| python-slugify | 9.0.0 | 0 |
| pytz | 2026.5 | 0 |
| pyyaml | 6.0.3 | 0 |
| readchar | 4.2.2 | 0 |
| redis | 8.1.0 | 0 |
| referencing | 0.37.0 | 0 |
| regex | 2026.9.29 | 0 |
| requests | 2.34.2 | 0 |
| requests-oauthlib | 2.0.0 | 0 |
| rfc3339-validator | 0.1.4 | 0 |
| rich | 15.0.0 | 0 |
| rich-rst | 2.2.0 | 0 |
| rpds-py | 2026.9.1 | 0 |
| ruamel-yaml | 0.19.1 | 0 |
| ruamel-yaml-clib | 0.2.15 | 0 |
| semver | 3.1.0 | 0 |
| shellingham | 1.5.4 | 0 |
| six | 1.17.0 | 0 |
| sniffio | 1.3.1 | 0 |
| sqlalchemy | 2.1.4 | 0 |
| starlette | 1.7.0 | 0 |
| text-unidecode | 1.3 | 0 |
| toml | 0.10.2 | 0 |
| typer | 0.27.3 | 0 |
| typing-extensions | 4.16.0 | 0 |
| typing-inspection | 0.4.4 | 0 |
| tzdata | 2026.5 | 0 |
| tzlocal | 5.4.4 | 0 |
| uncalled-for | 0.4.1 | 0 |
| urllib3 | 2.8.0 | 0 |
| uvicorn | 0.54.0 | 0 |
| uvloop | 0.23.0 | 0 |
| watchfiles | 1.3.0 | 0 |
| websockets | 16.1.1 | 0 |

</details>

<details><summary><b>gateway</b> (35 paquetes)</summary>

| Paquete | Versión | Vulnerabilidades |
|---|---|---|
| annotated-doc | 0.0.5 | 0 |
| annotated-types | 0.8.0 | 0 |
| anyio | 4.15.1 | 0 |
| argon2-cffi | 25.1.0 | 0 |
| argon2-cffi-bindings | 26.1.0 | 0 |
| certifi | 2026.7.22 | 0 |
| cffi | 2.1.1 | 0 |
| click | 8.5.0 | 0 |
| cross-web | 0.7.0 | 0 |
| fastapi | 0.142.2 | 0 |
| graphql-core | 3.3.0 | 0 |
| h11 | 0.16.0 | 0 |
| httpcore | 1.0.9 | 0 |
| httptools | 0.9.0 | 0 |
| httpx | 0.28.1 | 0 |
| idna | 3.20 | 0 |
| opentelemetry-api | 1.45.1 | 0 |
| psycopg | 3.3.6 | 0 |
| psycopg-binary | 3.3.6 | 0 |
| pycparser | 3.11 | 0 |
| pydantic | 2.14.0 | 0 |
| pydantic-core | 2.50.0 | 0 |
| python-dateutil | 2.9.0.post0 | 0 |
| python-dotenv | 1.2.4 | 0 |
| pyyaml | 6.0.3 | 0 |
| redis | 8.1.0 | 0 |
| six | 1.17.0 | 0 |
| starlette | 1.7.0 | 0 |
| strawberry-graphql | 0.330.0 | 0 |
| typing-extensions | 4.16.0 | 0 |
| typing-inspection | 0.4.4 | 0 |
| uvicorn | 0.54.0 | 0 |
| uvloop | 0.23.0 | 0 |
| watchfiles | 1.3.0 | 0 |
| websockets | 17.2 | 0 |

</details>

<details><summary><b>mock-source</b> (20 paquetes)</summary>

| Paquete | Versión | Vulnerabilidades |
|---|---|---|
| annotated-doc | 0.0.5 | 0 |
| annotated-types | 0.8.0 | 0 |
| anyio | 4.15.1 | 0 |
| click | 8.5.0 | 0 |
| fastapi | 0.142.2 | 0 |
| h11 | 0.16.0 | 0 |
| httptools | 0.9.0 | 0 |
| idna | 3.20 | 0 |
| opentelemetry-api | 1.45.1 | 0 |
| pydantic | 2.14.0 | 0 |
| pydantic-core | 2.50.0 | 0 |
| python-dotenv | 1.2.4 | 0 |
| pyyaml | 6.0.3 | 0 |
| starlette | 1.7.0 | 0 |
| typing-extensions | 4.16.0 | 0 |
| typing-inspection | 0.4.4 | 0 |
| uvicorn | 0.54.0 | 0 |
| uvloop | 0.23.0 | 0 |
| watchfiles | 1.3.0 | 0 |
| websockets | 17.2 | 0 |

</details>

<details><summary><b>ingestion</b> (125 paquetes)</summary>

| Paquete | Versión | Vulnerabilidades |
|---|---|---|
| aiosqlite | 0.22.1 | 0 |
| alembic | 1.20.0 | 0 |
| amplitude-analytics | 1.2.3 | 0 |
| annotated-doc | 0.0.5 | 0 |
| annotated-types | 0.8.0 | 0 |
| anyio | 4.15.1 | 0 |
| apprise | 1.13.1 | 0 |
| asgi-lifespan | 2.1.0 | 0 |
| asyncpg | 0.32.0 | 0 |
| attrs | 26.1.0 | 0 |
| beartype | 0.23.0 | 0 |
| beautifulsoup4 | 4.15.0 | 0 |
| bokeh | 3.10.0 | 0 |
| burner-redis | 0.1.7 | 0 |
| cachetools | 7.2.1 | 0 |
| certifi | 2026.7.22 | 0 |
| cffi | 2.1.1 | 0 |
| charset-normalizer | 3.5.2 | 0 |
| click | 8.5.0 | 0 |
| cloudpickle | 3.1.2 | 0 |
| colorama | 0.4.6 | 0 |
| coolname | 5.0.0 | 0 |
| cronsim | 2.7 | 0 |
| cryptography | 50.0.2 | 0 |
| cyclopts | 5.2.0 | 0 |
| dask | 2026.8.0 | 0 |
| dateparser | 1.4.3 | 0 |
| distributed | 2026.8.0 | 0 |
| docker | 7.2.0 | 0 |
| docstring-parser | 0.18.0 | 0 |
| exceptiongroup | 1.3.1 | 0 |
| fastapi | 0.143.0 | 0 |
| fsspec | 2026.9.0 | 0 |
| graphviz | 0.21 | 0 |
| greenlet | 3.5.6 | 0 |
| griffe | 2.3.2 | 0 |
| griffecli | 2.3.2 | 0 |
| griffelib | 2.3.2 | 0 |
| h11 | 0.16.0 | 0 |
| h2 | 4.4.1 | 0 |
| hpack | 4.2.0 | 0 |
| httpcore | 1.0.9 | 0 |
| httpx | 0.28.1 | 0 |
| humanize | 4.16.0 | 0 |
| hyperframe | 6.1.0 | 0 |
| idna | 3.20 | 0 |
| jinja2 | 3.1.6 | 0 |
| jinja2-humanize-extension | 0.4.0 | 0 |
| jsonpatch | 1.34 | 0 |
| jsonpointer | 3.2.0 | 0 |
| jsonschema | 4.26.0 | 0 |
| jsonschema-specifications | 2025.9.1 | 0 |
| locket | 1.0.0 | 0 |
| mako | 1.4.3 | 0 |
| markdown | 3.11 | 0 |
| markdown-it-py | 4.2.0 | 0 |
| markupsafe | 3.0.4 | 0 |
| mdurl | 0.1.2 | 0 |
| msgpack | 1.2.3 | 0 |
| narwhals | 2.27.1 | 0 |
| numpy | 2.5.3 | 0 |
| oauthlib | 4.0.0 | 0 |
| opentelemetry-api | 1.45.1 | 0 |
| orjson | 3.13.0 | 0 |
| partd | 1.4.2 | 0 |
| pathspec | 1.1.1 | 0 |
| pendulum | 3.2.0 | 0 |
| pillow | 12.3.0 | 0 |
| pluggy | 1.6.0 | 0 |
| prefect | 3.8.8 | 0 |
| prefect-dask | 0.3.7 | 0 |
| prometheus-client | 0.26.0 | 0 |
| psutil | 7.2.2 | 0 |
| psycopg | 3.3.6 | 0 |
| psycopg-binary | 3.3.6 | 0 |
| py-key-value-aio | 0.4.6 | 0 |
| pycparser | 3.11 | 0 |
| pydantic | 2.14.0 | 0 |
| pydantic-core | 2.50.0 | 0 |
| pydantic-extra-types | 2.11.1 | 0 |
| pydantic-settings | 2.15.0 | 0 |
| pydocket | 0.27.0 | 0 |
| pygments | 2.21.0 | 0 |
| python-dateutil | 2.9.0.post0 | 0 |
| python-dotenv | 1.2.4 | 0 |
| python-json-logger | 4.2.0 | 0 |
| python-slugify | 9.0.0 | 0 |
| pytz | 2026.5 | 0 |
| pyyaml | 6.0.3 | 0 |
| readchar | 4.2.2 | 0 |
| redis | 8.1.0 | 0 |
| referencing | 0.37.0 | 0 |
| regex | 2026.9.29 | 0 |
| requests | 2.34.2 | 0 |
| requests-oauthlib | 2.0.0 | 0 |
| rfc3339-validator | 0.1.4 | 0 |
| rich | 15.0.0 | 0 |
| rich-rst | 2.2.0 | 0 |
| rpds-py | 2026.9.1 | 0 |
| ruamel-yaml | 0.19.1 | 0 |
| ruamel-yaml-clib | 0.2.15 | 0 |
| semver | 3.1.0 | 0 |
| shellingham | 1.5.4 | 0 |
| six | 1.17.0 | 0 |
| sniffio | 1.3.1 | 0 |
| sortedcontainers | 2.4.0 | 0 |
| soupsieve | 3.0 | 0 |
| sqlalchemy | 2.1.4 | 0 |
| starlette | 1.7.0 | 0 |
| tblib | 3.2.2 | 0 |
| text-unidecode | 1.3 | 0 |
| toml | 0.10.2 | 0 |
| toolz | 1.2.0 | 0 |
| tornado | 6.5.10 | 0 |
| typer | 0.27.3 | 0 |
| typing-extensions | 4.16.0 | 0 |
| typing-inspection | 0.4.4 | 0 |
| tzdata | 2026.5 | 0 |
| tzlocal | 5.4.4 | 0 |
| uncalled-for | 0.4.1 | 0 |
| urllib3 | 2.8.0 | 0 |
| uvicorn | 0.54.0 | 0 |
| websockets | 16.1.1 | 0 |
| xyzservices | 2026.9.1 | 0 |
| zict | 3.0.0 | 0 |

</details>
