CREATE SCHEMA IF NOT EXISTS cdm;

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger(
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR(100) NOT NULL,
    courier_name VARCHAR(100) NOT NULL,
    settlement_year INTEGER NOT NULL,
    settlement_month INT NOT NULL,
    orders_count INTEGER NOT NULL DEFAULT 0 CHECK (orders_count >= 0),
    order_total_sum NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (order_total_sum >= 0),
    rate_avg FLOAT NOT NULL DEFAULT 0 CHECK (rate_avg >= 0),
    order_processing_fee NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= 0),
    courier_order_sum NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (courier_order_sum >= 0),
    courier_tips_sum NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (courier_tips_sum >= 0),
    courier_reward_sum NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (courier_reward_sum >= 0)
);

ALTER TABLE cdm.dm_courier_ledger DROP CONSTRAINT IF EXISTS date_check, DROP CONSTRAINT IF EXISTS dm_courier_ledger_unique;

ALTER TABLE cdm.dm_courier_ledger
ADD CONSTRAINT date_check 
CHECK (
    settlement_year >= 2020 AND settlement_year <= 2500 AND
    settlement_month >= 1 AND settlement_month <= 12
);

ALTER TABLE cdm.dm_courier_ledger
ADD CONSTRAINT dm_courier_ledger_unique
UNIQUE (courier_id, settlement_year, settlement_month);

CREATE TABLE IF NOT EXISTS cdm.settings (
    id INT4 NOT NULL GENERATED ALWAYS AS IDENTITY,
    workflow_key VARCHAR,
    workflow_settings TEXT,
    CONSTRAINT settings_pkey PRIMARY KEY (id)
);
