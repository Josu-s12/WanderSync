// WanderSync - Frontend
// Toda la comunicación con el backend pasa EXCLUSIVAMENTE por el API Gateway GraphQL (/graphql).

const $ = (sel) => document.querySelector(sel);
const money = new Intl.NumberFormat("es-CO", { style: "currency", currency: "COP", maximumFractionDigits: 0 });
const dateFmt = new Intl.DateTimeFormat("es-CO", { dateStyle: "medium", timeStyle: "short" });

const STEP_LABEL = { FLIGHT: "Vuelo", HOTEL: "Hotel", CAR: "Auto", PAYMENT: "Pago" };
let currentUser = null;
let lastSearch = null;

// ---------------------------------------------------------------------------
// Cliente GraphQL (siempre con variables: nunca se concatenan datos en la consulta)
// ---------------------------------------------------------------------------
async function gql(query, variables = {}) {
  const res = await fetch("/graphql", {
    method: "POST",
    credentials: "same-origin",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
  });
  if (res.status === 429) throw new Error("Demasiadas solicitudes. Espera un momento.");
  const body = await res.json();
  if (body.errors?.length) throw new Error(body.errors[0].message);
  return body.data;
}

// ---------------------------------------------------------------------------
// Utilidades de UI
// ---------------------------------------------------------------------------
function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") node.className = v;
    else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v);
  }
  for (const child of children.flat()) {
    if (child == null) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));  // textContent: sin XSS
  }
  return node;
}

let toastTimer;
function toast(message, isError = false) {
  const t = $("#toast");
  t.textContent = message;
  t.className = "toast" + (isError ? " error" : "");
  t.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => (t.hidden = true), 4000);
}

// ---------------------------------------------------------------------------
// Sesión
// ---------------------------------------------------------------------------
async function refreshSession() {
  const { session } = await gql("query { session { authenticated email fingerprint } }");
  $("#fingerprint").textContent = session.fingerprint;
  currentUser = session.authenticated ? session.email : null;
  $("#user-label").textContent = currentUser ?? "";
  $("#btn-open-auth").hidden = !!currentUser;
  $("#btn-logout").hidden = !currentUser;
  return session;
}

let authMode = "login";
function setAuthMode(mode) {
  authMode = mode;
  $("#auth-title").textContent = mode === "login" ? "Iniciar sesión" : "Crear cuenta";
  $("#auth-submit").textContent = mode === "login" ? "Entrar" : "Registrarme";
  $("#auth-switch").textContent = mode === "login" ? "¿No tienes cuenta? Regístrate" : "¿Ya tienes cuenta? Inicia sesión";
  $("#auth-form").password.autocomplete = mode === "login" ? "current-password" : "new-password";
  $("#auth-error").textContent = "";
}

async function submitAuth(event) {
  event.preventDefault();
  const form = $("#auth-form");
  const email = form.email.value;
  const password = form.password.value;
  $("#auth-submit").disabled = true;
  $("#auth-error").textContent = "";
  try {
    if (authMode === "register") {
      await gql("mutation ($email: String!, $password: String!) { register(email: $email, password: $password) { id } }",
        { email, password });
      toast("Cuenta creada. Ahora inicia sesión.");
      setAuthMode("login");
      return;
    }
    const before = $("#fingerprint").textContent;
    await gql("mutation ($email: String!, $password: String!) { login(email: $email, password: $password) { id email } }",
      { email, password });
    const session = await refreshSession();
    $("#auth-dialog").close();
    form.reset();
    toast(`Sesión iniciada. ID de sesión regenerado: ${before} → ${session.fingerprint}`);
    loadOrders();
  } catch (err) {
    $("#auth-error").textContent = err.message;
  } finally {
    $("#auth-submit").disabled = false;
  }
}

async function logout() {
  await gql("mutation { logout }");
  await refreshSession();
  $("#orders").replaceChildren(el("p", { class: "muted" }, "Inicia sesión para ver tus reservas."));
  toast("Sesión cerrada");
}

// ---------------------------------------------------------------------------
// Búsqueda de paquetes (consulta compleja del gateway)
// ---------------------------------------------------------------------------
const SEARCH_QUERY = `
  query ($origin: String!, $destination: String!, $nights: Int!, $days: Int!, $maxBudget: Float) {
    searchPackages(origin: $origin, destination: $destination, nights: $nights, days: $days,
                   maxBudget: $maxBudget, limit: 6) {
      totalPrice nights days
      flight { id airline departAt price }
      hotel  { id name stars pricePerNight }
      car    { id model category pricePerDay }
    }
  }`;

async function search(event) {
  event?.preventDefault();
  const f = $("#search-form");
  lastSearch = {
    origin: f.origin.value,
    destination: f.destination.value,
    nights: Number(f.nights.value),
    days: Number(f.days.value),
    maxBudget: f.maxBudget.value ? Number(f.maxBudget.value) : null,
  };
  $("#results").replaceChildren(el("p", { class: "muted" }, "Buscando…"));
  try {
    const { searchPackages } = await gql(SEARCH_QUERY, lastSearch);
    renderPackages(searchPackages);
  } catch (err) {
    $("#results").replaceChildren(el("p", { class: "error" }, err.message));
  }
}

function renderPackages(packages) {
  if (!packages.length) {
    $("#results").replaceChildren(el("p", { class: "muted" }, "No hay paquetes para esa ruta o presupuesto."));
    return;
  }
  $("#results").replaceChildren(...packages.map((p) => el("article", { class: "package" },
    el("div", { class: "item" },
      el("small", {}, "Vuelo"),
      el("strong", {}, p.flight.airline),
      el("span", {}, `${dateFmt.format(new Date(p.flight.departAt))} · ${money.format(p.flight.price)}`)),
    el("div", { class: "item" },
      el("small", {}, `Hotel · ${p.nights} noche(s)`),
      el("strong", {}, p.hotel.name),
      el("span", {}, el("span", { class: "stars" }, "★".repeat(p.hotel.stars)), ` · ${money.format(p.hotel.pricePerNight)}/noche`)),
    el("div", { class: "item" },
      el("small", {}, `Auto · ${p.days} día(s)`),
      el("strong", {}, p.car.model),
      el("span", {}, `${p.car.category} · ${money.format(p.car.pricePerDay)}/día`)),
    el("div", { class: "total" },
      el("strong", {}, money.format(p.totalPrice)),
      el("button", { class: "btn btn-primary", onclick: (e) => book(p, e.currentTarget) }, "Reservar")),
  )));
}

// ---------------------------------------------------------------------------
// Reserva (mutación -> SAGA)
// ---------------------------------------------------------------------------
const BOOK_MUTATION = `
  mutation ($input: BookPackageInput!) {
    bookPackage(input: $input) {
      id status total prefectRunUrl
      steps { step action result detail at }
    }
  }`;

async function book(pkg, button) {
  if (!currentUser) {
    toast("Inicia sesión para reservar", true);
    openAuth();
    return;
  }
  const failAt = $("#fail-at").value || null;
  button.disabled = true;
  button.textContent = "Reservando…";
  try {
    const { bookPackage } = await gql(BOOK_MUTATION, {
      input: {
        flightOfferId: pkg.flight.id, hotelOfferId: pkg.hotel.id, carOfferId: pkg.car.id,
        nights: pkg.nights, days: pkg.days, simulateFailureAt: failAt,
      },
    });
    renderSaga(bookPackage);
    loadOrders();
    if (lastSearch) search();   // refresca disponibilidad
  } catch (err) {
    toast(err.message, true);
  } finally {
    button.disabled = false;
    button.textContent = "Reservar";
  }
}

function timeline(steps) {
  return el("ol", { class: "timeline" }, steps.map((s) => {
    const kind = s.action === "COMPENSATE" ? "compensate" : s.result === "OK" ? "ok" : "failed";
    const icon = kind === "compensate" ? "↺" : kind === "ok" ? "✓" : "✗";
    const action = s.action === "COMPENSATE" ? "Compensación" : "Ejecución";
    return el("li", { class: kind },
      el("span", { class: "icon" }, icon),
      el("strong", {}, STEP_LABEL[s.step] ?? s.step),
      el("span", {}, action),
      el("span", { class: "detail" }, s.detail ?? ""));
  }));
}

function renderSaga(order) {
  const ok = order.status === "CONFIRMED";
  $("#saga-panel").hidden = false;
  $("#saga-result").replaceChildren(
    el("div", { class: "saga-summary" },
      el("span", { class: `status ${order.status}` }, order.status),
      el("span", {}, ok
        ? `Reserva confirmada por ${money.format(order.total)}`
        : "La reserva falló y la SAGA deshizo automáticamente los pasos ya confirmados (sin reservas huérfanas)."),
      el("code", { class: "muted" }, order.id),
      order.prefectRunUrl
        ? el("a", { class: "prefect-link", href: order.prefectRunUrl, target: "_blank", rel: "noopener" },
            "Ver esta SAGA en Prefect ↗")
        : null),
    timeline(order.steps));
  $("#saga-panel").scrollIntoView({ behavior: "smooth", block: "start" });
}

// ---------------------------------------------------------------------------
// Mis reservas
// ---------------------------------------------------------------------------
async function loadOrders() {
  if (!currentUser) return;
  try {
    const { myOrders } = await gql("query { myOrders(limit: 10) { id status total createdAt } }");
    if (!myOrders.length) {
      $("#orders").replaceChildren(el("p", { class: "muted" }, "Todavía no tienes reservas."));
      return;
    }
    $("#orders").replaceChildren(el("table", { class: "orders" },
      el("thead", {}, el("tr", {}, el("th", {}, "Fecha"), el("th", {}, "Orden"), el("th", {}, "Estado"),
        el("th", {}, "Total"), el("th", {}, ""))),
      el("tbody", {}, myOrders.map((o) => el("tr", {},
        el("td", {}, dateFmt.format(new Date(o.createdAt))),
        el("td", {}, el("code", {}, o.id.slice(0, 8))),
        el("td", {}, el("span", { class: `status ${o.status}` }, o.status)),
        el("td", {}, o.total != null ? money.format(o.total) : "—"),
        el("td", {}, el("button", { class: "btn btn-ghost", onclick: () => showOrder(o.id) }, "Ver SAGA")))))));
  } catch (err) {
    toast(err.message, true);
  }
}

async function showOrder(id) {
  try {
    const { order } = await gql(
      "query ($id: ID!) { order(id: $id) { id status total steps { step action result detail at } } }", { id });
    renderSaga(order);
  } catch (err) {
    toast(err.message, true);
  }
}

// ---------------------------------------------------------------------------
// Inicio
// ---------------------------------------------------------------------------
function openAuth() {
  setAuthMode("login");
  $("#auth-dialog").showModal();
}

document.addEventListener("DOMContentLoaded", async () => {
  $("#search-form").addEventListener("submit", search);
  $("#btn-open-auth").addEventListener("click", openAuth);
  $("#btn-logout").addEventListener("click", logout);
  $("#btn-refresh-orders").addEventListener("click", loadOrders);
  $("#auth-form").addEventListener("submit", submitAuth);
  $("#auth-cancel").addEventListener("click", () => $("#auth-dialog").close());
  $("#auth-switch").addEventListener("click", () => setAuthMode(authMode === "login" ? "register" : "login"));
  try {
    await refreshSession();
    await loadOrders();
    await search();
  } catch (err) {
    toast(`No se pudo conectar con el gateway: ${err.message}`, true);
  }
});
