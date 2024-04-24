CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.restaurants(
    id SERIAL PRIMARY KEY,
    restaurant_id VARCHAR(100) UNIQUE NOT NULL,
    restaurant_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.couriers(
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR(100) UNIQUE NOT NULL,
    courier_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.orders(
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(100) UNIQUE NOT NULL,
    order_ts TIMESTAMP NOT NULL,
    restaurant_id INT DEFAULT 1,
    sum NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (sum >= 0)
);


CREATE TABLE IF NOT EXISTS dds.deliveries(
    id SERIAL PRIMARY KEY,
    delivery_id VARCHAR(100) UNIQUE NOT NULL,
    courier_id INT,
    order_id INT,
    address VARCHAR(255) NOT NULL,
    delivery_ts TIMESTAMP,
    rate INT4 NOT NULL CHECK (rate >= 1 AND rate <= 5),
    tip_sum NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (tip_sum >= 0)
);

CREATE TABLE IF NOT EXISTS dds.timestamps
(
    id SERIAL PRIMARY KEY,
    ts TIMESTAMP UNIQUE NOT NULL,
    year SMALLINT NOT NULL CHECK (year >= 2022 AND year < 2500),
    month SMALLINT NOT NULL CHECK (month >= 1 AND month <= 12),
    day SMALLINT NOT NULL CHECK (day >= 1 AND day <= 31),
    time TIME NOT NULL,
    date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.settings (
    id INT4 NOT NULL GENERATED ALWAYS AS IDENTITY,
    workflow_key VARCHAR,
    workflow_settings TEXT,
    CONSTRAINT settings_pkey PRIMARY KEY (id)
);

ALTER TABLE dds.deliveries DROP CONSTRAINT IF EXISTS deliveries_courier_id_fkey;
ALTER TABLE dds.deliveries DROP CONSTRAINT IF EXISTS deliveries_timestamps_id_fkey;
ALTER TABLE dds.deliveries DROP CONSTRAINT IF EXISTS deliveries_orders_id_fkey;
ALTER TABLE dds.orders DROP CONSTRAINT IF EXISTS orders_timestamps_id_fkey;
ALTER TABLE dds.orders DROP CONSTRAINT IF EXISTS orders_restaurants_id_fkey;

ALTER TABLE dds.deliveries 
    ADD CONSTRAINT deliveries_courier_id_fkey 
    FOREIGN KEY (courier_id) 
    REFERENCES dds.couriers(id);

ALTER TABLE dds.deliveries  
    ADD CONSTRAINT deliveries_timestamps_id_fkey
    FOREIGN KEY (delivery_ts) 
    REFERENCES dds.timestamps (ts);

ALTER TABLE dds.deliveries  
    ADD CONSTRAINT deliveries_orders_id_fkey 
    FOREIGN KEY (order_id) 
    REFERENCES dds.orders (id);

ALTER TABLE dds.orders  
    ADD CONSTRAINT orders_timestamps_id_fkey
    FOREIGN KEY (order_ts) 
    REFERENCES dds.timestamps (ts);

ALTER TABLE dds.orders  
    ADD CONSTRAINT orders_restaurants_id_fkey 
    FOREIGN KEY (restaurant_id) 
    REFERENCES dds.restaurants (id);
