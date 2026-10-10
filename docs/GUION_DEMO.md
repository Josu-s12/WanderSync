# Guion de la demostración en vivo

**Autor:** Josue Sarmiento · WanderSync Travel Solutions

Cubre los 4 puntos que exige el enunciado: **(a)** Prefect monitoreando los flujos, **(b)** tareas distribuidas en
Dask, **(c)** el frontend consumiendo la API GraphQL y **(d)** un fallo transaccional con compensaciones SAGA.
Al final hay un bloque de seguridad. Duración estimada: 12 a 15 minutos.

## 0. Preparación (antes de empezar)

1. Abre **Docker Desktop** y espera a que diga *Engine running*.
2. En la terminal de VS Code, desde la carpeta del proyecto:
   ```powershell
   docker compose up -d --build
   docker compose ps
   ```
   Deben aparecer 14 contenedores `Up`.
3. Deja abiertas estas pestañas en el navegador:
   - http://localhost:3000 (frontend)
   - http://localhost:4200 (Prefect)
   - http://localhost:8788/status (Dask)
4. En VS Code abre **dos terminales extra** y déjalas mostrando logs:
   ```powershell
   docker compose logs -f orders
   ```
   ```powershell
   docker compose logs -f gateway
   ```
5. *(Opcional, para que se vean más reintentos)*: en `docker-compose.yml` cambia `FAILURE_RATE: "0.2"` por `"0.4"` y
   ejecuta `docker compose up -d mock-source`.

---

## 1. Arquitectura (1 minuto)

Muestra el diagrama de [ARQUITECTURA.md](ARQUITECTURA.md#2-vista-general-de-la-arquitectura) y `docker compose ps`.

> "Todo se levanta con `docker compose up`. Son 14 contenedores. Los microservicios, la base de datos y Redis no
> tienen puertos publicados: el único acceso al backend es el API Gateway GraphQL, a través del frontend."

---

## 2. (b) + (a) Ingesta distribuida en Dask, monitoreada en Prefect (4 minutos)

1. En **Prefect** (http://localhost:4200) ve a **Deployments** → `ingesta-cada-10-min`.
   > "Este deployment corre cada 10 minutos. Lo lanzo a mano para verlo en vivo."
2. Clic en **Run** → **Quick run**.
3. Cambia **de inmediato** a la pestaña de **Dask** (http://localhost:8788/status):
   > "Aquí se ve el *task stream*: cada barra es una tarea de scraping, limpieza o upsert corriendo en paralelo en
   > los 2 workers de Dask."
   - Opcional: la pestaña **Workers** muestra los 2 workers.
4. Vuelve a **Prefect** → **Runs** → abre la ejecución en curso:
   - El **grafo de tareas**: `scrape-google-flights` (vuelos **reales**), `scrape-page` → `parse-and-clean`
     (hoteles y autos), `upsert-offers` y `market-stats`.
   - En **Logs**, las líneas `Google Flights Bogota -> Cartagena (...): 31 vuelos reales`:
     > "Los vuelos se scrapean en vivo de Google Flights. Los hoteles y autos vienen de una fuente simulada, porque
     > Booking.com bloquea el acceso automatizado con un desafío anti-bots."
   - Busca tareas en estado **Retrying / AwaitingRetry**, o con varios intentos:
     > "La fuente responde 503 o 429 el 20% de las veces. Prefect reintenta automáticamente con backoff
     > exponencial y jitter, hasta 4 veces."
   - En **Logs** filtra por `Fuente respondió` para ver los fallos y por `descartado` para ver la limpieza de
     datos incompletos.
   - Al final, el log **Resumen de la ingesta** muestra páginas, páginas fallidas, filas guardadas y los precios
     promedio calculados con **Dask Bag**.
5. *(Opcional)* Muestra la fuente cruda de hoteles en http://localhost:8080/hotels?city=Cartagena:
   > "Es HTML con precios en formatos distintos. El pipeline los normaliza."

---

## 3. (c) Frontend consumiendo la API GraphQL (3 minutos)

1. En http://localhost:3000, fíjate en la **huella de sesión** de arriba a la derecha (por ejemplo `0200f90677`).
2. **Iniciar sesión → Regístrate** con un correo y una contraseña de al menos 10 caracteres, con letras y números.
   Luego inicia sesión.
   > "La huella de sesión cambió (por ejemplo `0200f90677 → 3caa4e07d7`): el servidor **regeneró el ID de sesión**
   > al autenticarme. Eso mitiga Session Fixation. Lo retomo al final."
3. Busca **Bogota → Cartagena**, 3 noches y 3 días → **Buscar paquetes**.
4. Muestra la terminal de logs del **gateway**:
   > "El frontend hizo una sola consulta GraphQL, `searchPackages`. El gateway la tradujo en **una sola**
   > consulta a `pg_graphql` de Supabase, pidiendo **solo** las columnas que el frontend seleccionó. Eso es cero
   > over-fetching de extremo a extremo."
5. Abre http://localhost:3000/graphql (GraphiQL) y ejecuta:
   ```graphql
   { flights(destination: "Cartagena", limit: 3) { airline price } }
   ```
   En el log del gateway se ve `node { airline id price }`.

---

## 4. (d) Fallo transaccional y compensaciones SAGA (4 minutos)

### 4.1 Happy path
1. En el frontend deja **Simular fallo en: Ninguno** → **Reservar** en el primer paquete.
2. Resultado: **CONFIRMED** con 4 pasos en verde (Vuelo, Hotel, Auto y Pago).
3. Muestra la terminal de **orders**: `INICIO SAGA` → `FLIGHT EXECUTE OK` → ... → `PAYMENT EXECUTE OK`.

### 4.2 Falla el auto
1. Elige **Simular fallo en: Auto** → **Reservar**.
2. Resultado: **COMPENSATED**.
   ```
   ✓ Vuelo  Ejecución
   ✓ Hotel  Ejecución
   ✗ Auto   Ejecución      HTTP 503 - Fallo simulado en el servicio de autos
   ↺ Hotel  Compensación   reserva cancelada
   ↺ Vuelo  Compensación   reserva cancelada
   ```
   > "El auto falló y el orquestador deshizo automáticamente el hotel y el vuelo **en orden inverso**. No quedan
   > reservas huérfanas, que era el problema original de WanderSync."
3. Clic en **"Ver esta SAGA en Prefect ↗"**. Se abre el flow `saga-xxxxxxxx-fallo-auto` con estado **Compensated**:
   > "Cada paso de la SAGA es una tarea de Prefect. `reservar-CAR` está en rojo y las compensaciones en verde. Si
   > una compensación fallara, Prefect la reintentaría hasta 3 veces, y aquí se verían esos reintentos."
   - En **Flows** de Prefect se ven los dos flujos del sistema: `ingesta-turistica` y `saga-reserva-paquete`.
4. *(Opcional)* Repite con **Pago**: se compensan las 3 reservas.

### 4.3 Fallo real (no simulado)
```powershell
docker compose stop cars
```
Reserva sin simular fallo → **COMPENSATED** con `error de red (ConnectError)`. Luego:
```powershell
docker compose start cars
```

### 4.4 Evidencia en la base de datos
```powershell
docker compose exec db psql -U postgres -d wandersync -P pager=off -c "select o.status, l.step, l.action, l.result, l.detail from orders.saga_log l join orders.orders o on o.id = l.order_id order by l.id desc limit 15;"
```
> "Cada paso queda en la bitácora `saga_log`. Si el orquestador se cayera a mitad de una SAGA, al reiniciar lee esta
> bitácora y compensa las órdenes pendientes."

---

## 5. Ciberseguridad (2 minutos)

| Qué mostrar | Cómo |
|---|---|
| **Session Fixation** | La huella cambia al hacer login (paso 3.2). En DevTools (F12 → Application → Cookies), `ws_session` es `HttpOnly` y `SameSite=Strict` |
| **Argon2id** | `docker compose exec db psql -U postgres -d wandersync -c "select email, left(password_hash, 40) from orders.users;"` muestra `$argon2id$v=19$m=65536,t=3,p=4$...` (configuración en `gateway/security.py`) |
| **Rate limiting** | Cierra sesión e intenta entrar 6 veces con una contraseña errada → *"Demasiados intentos. Intenta de nuevo en 59 s"* |
| **Supply chain** | Abre [`docs/security/REPORTE_AUDITORIA_DEPENDENCIAS.md`](security/REPORTE_AUDITORIA_DEPENDENCIAS.md): 0 vulnerabilidades en los 7 servicios, más un control positivo con 13 CVE detectadas. Se puede re-ejecutar con `.\scripts\audit.ps1` |
| **Superficie mínima** | `docker compose ps`: solo el frontend, Prefect, Dask y la fuente mock tienen puertos publicados |

---

## Preguntas frecuentes del jurado

- **¿Por qué orquestación y no coreografía?** Ver D2 en [ARQUITECTURA.md](ARQUITECTURA.md#10-decisiones-de-arquitectura-y-justificación).
- **¿Y si se cae Prefect?** Las reservas siguen funcionando: el servicio `orders` detecta que Prefect no responde y
  ejecuta la misma SAGA en modo directo. Prefect aporta observabilidad, no es un punto único de falla.
- **¿Qué pasa si falla una compensación?** Se reintenta hasta 3 veces con backoff. Si aun así falla, la orden queda
  `COMPENSATION_PENDING` y la recuperación automática la vuelve a intentar.
- **¿Qué pasa si reintentan la misma reserva?** Los endpoints son idempotentes por `order_id`, así que no se duplica.
- **¿Los vuelos son reales?** Sí, se scrapean de Google Flights en cada ingesta. Si Google bloqueara, esa ruta se
  llena automáticamente desde la fuente mock (ver D9).
- **¿Por qué los hoteles no son de Booking?** Booking.com responde con un desafío anti-bots (AWS WAF). Evadirlo no
  es correcto, así que los hoteles y autos vienen de la fuente simulada, que reproduce sus dificultades.
- **¿Cómo se evita el over-fetching hacia la base?** El gateway propaga la selección de campos del cliente a la
  consulta de `pg_graphql` (sección 6.1).
