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


-- ---------- PERMISOS PARA LOS MICROSERVICIOS ----------
-- Los microservicios se conectan como "postgres", que en Supabase NO es superusuario
GRANT USAGE ON SCHEMA flights, hotels, cars, orders TO postgres;
GRANT ALL ON ALL TABLES    IN SCHEMA flights, hotels, cars, orders TO postgres;
GRANT ALL ON ALL SEQUENCES IN SCHEMA flights, hotels, cars, orders TO postgres;

-- ---------- DATOS SEMILLA (para probar antes de tener el scraping) ----------
INSERT INTO flights.offers  (source_id, origin, destination, airline, depart_at, price, seats) VALUES
  ('seed-f1', 'Bogota',   'Cartagena',   'Avianca',  now() + interval '7 days',  420000, 5),
  ('seed-f2', 'Bogota',   'Cartagena',   'LATAM',    now() + interval '7 days',  380000, 3),
  ('seed-f3', 'Medellin', 'Santa Marta', 'Wingo',    now() + interval '10 days', 290000, 4);

INSERT INTO hotels.offers (source_id, city, name, stars, price_per_night, rooms) VALUES
  ('seed-h1', 'Cartagena',   'Hotel Caribe',       5, 650000, 2),
  ('seed-h2', 'Cartagena',   'Casa Getsemani',     3, 210000, 4),
  ('seed-h3', 'Santa Marta', 'Hotel Rodadero Mar', 4, 330000, 3);

INSERT INTO cars.offers (source_id, city, model, category, price_per_day, units) VALUES
  ('seed-c1', 'Cartagena',   'Kia Picanto',  'Economico', 120000, 3),
  ('seed-c2', 'Cartagena',   'Mazda CX-30',  'SUV',       260000, 2),
  ('seed-c3', 'Santa Marta', 'Renault Logan','Economico', 110000, 2);

  
-- ---------- PASO 3: FACTURACIÓN Y TRAZABILIDAD DE LA SAGA ----------
ALTER TABLE orders.saga_log ADD COLUMN booking_id UUID;

CREATE TABLE orders.payments (
  id         UUID PRIMARY KEY,
  order_id   UUID NOT NULL REFERENCES orders.orders(id),
  amount     NUMERIC(12,2) NOT NULL,
  status     TEXT NOT NULL,          -- CAPTURED
  created_at TIMESTAMPTZ DEFAULT now()
);

GRANT ALL ON orders.payments TO postgres;