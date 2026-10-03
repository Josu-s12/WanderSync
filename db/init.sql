-- =====================================================
-- WanderSync - Esquema inicial
-- Un schema por microservicio (database-per-service)
-- =====================================================

CREATE EXTENSION IF NOT EXISTS pg_graphql;

CREATE SCHEMA IF NOT EXISTS flights;
CREATE SCHEMA IF NOT EXISTS hotels;
CREATE SCHEMA IF NOT EXISTS cars;
CREATE SCHEMA IF NOT EXISTS orders;

-- ---------- CATÁLOGO (lo llena el scraping con Dask) ----------
CREATE TABLE flights.offers (
  id          SERIAL PRIMARY KEY,
  source_id   TEXT UNIQUE NOT NULL,
  origin      TEXT NOT NULL,
  destination TEXT NOT NULL,
  airline     TEXT NOT NULL,
  depart_at   TIMESTAMPTZ NOT NULL,
  price       NUMERIC(10,2) NOT NULL,
  seats       INT NOT NULL,
  scraped_at  TIMESTAMPTZ DEFAULT   now()
);

CREATE TABLE hotels.offers (
  id              SERIAL PRIMARY KEY,
  source_id       TEXT UNIQUE NOT NULL,
  city            TEXT NOT NULL,
  name            TEXT NOT NULL,
  stars           INT NOT NULL,
  price_per_night NUMERIC(10,2) NOT NULL,
  rooms           INT NOT NULL,
  scraped_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE cars.offers (
  id            SERIAL PRIMARY KEY,
  source_id     TEXT UNIQUE NOT NULL,
  city          TEXT NOT NULL,
  model         TEXT NOT NULL,
  category      TEXT NOT NULL,
  price_per_day NUMERIC(10,2) NOT NULL,
  units         INT NOT NULL,
  scraped_at    TIMESTAMPTZ DEFAULT now()
);

-- ---------- RESERVAS (las crea y compensa la SAGA) ----------
CREATE TABLE flights.bookings (
  id         UUID PRIMARY KEY,
  order_id   UUID NOT NULL,
  offer_id   INT NOT NULL REFERENCES flights.offers(id),
  status     TEXT NOT NULL,          -- CONFIRMED / CANCELLED
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE hotels.bookings (
  id         UUID PRIMARY KEY,
  order_id   UUID NOT NULL,
  offer_id   INT NOT NULL REFERENCES hotels.offers(id),
  status     TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE cars.bookings (
  id         UUID PRIMARY KEY,
  order_id   UUID NOT NULL,
  offer_id   INT NOT NULL REFERENCES cars.offers(id),
  status     TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- ---------- USUARIOS, ÓRDENES Y LOG DE LA SAGA ----------
CREATE TABLE orders.users (
  id            SERIAL PRIMARY KEY,
  email         TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,       -- Argon2id
  created_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE orders.orders (
  id         UUID PRIMARY KEY,
  user_id    INT REFERENCES orders.users(id),
  status     TEXT NOT NULL,          -- PENDING / CONFIRMED / COMPENSATED
  total      NUMERIC(10,2),
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE orders.saga_log (
  id       SERIAL PRIMARY KEY,
  order_id UUID NOT NULL REFERENCES orders.orders(id),
  step     TEXT NOT NULL,            -- FLIGHT / HOTEL / CAR / PAYMENT
  action   TEXT NOT NULL,            -- EXECUTE / COMPENSATE
  result   TEXT NOT NULL,            -- OK / FAILED
  detail   TEXT,
  at       TIMESTAMPTZ DEFAULT now()
);