/* 
--- DROP STG + TABLES ---
DROP SCHEMA IF EXISTS stg CASCADE;
DROP TABLE IF EXISTS stg.restaurants;
DROP TABLE IF EXISTS stg.couriers;
DROP TABLE IF EXISTS stg.deliveries;
DROP TABLE IF EXISTS stg.settings;
*/

CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.restaurants (
    id SERIAL,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS stg.couriers (
    id SERIAL,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS stg.deliveries (
    id SERIAL,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS stg.settings (
    id INT4 NOT NULL GENERATED ALWAYS AS IDENTITY,
    workflow_key VARCHAR,
    workflow_settings TEXT,
    CONSTRAINT settings_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS stg.last_offset (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL UNIQUE,
    offset_param INT NOT NULL DEFAULT 0
);

INSERT INTO stg.last_offset (table_name) VALUES 
    ('restaurants'),
    ('couriers'),
    ('deliveries')
ON CONFLICT (table_name) DO NOTHING;

