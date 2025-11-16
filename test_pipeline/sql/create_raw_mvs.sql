-- Materialized Views для автоматического перелива данных из Kafka в MergeTree

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.location_events_mv TO raw.location_events AS
SELECT * FROM raw.location_events_kafka;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.browser_events_mv TO raw.browser_events AS
SELECT * FROM raw.browser_events_kafka;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.device_events_mv TO raw.device_events AS
SELECT * FROM raw.device_events_kafka;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.geo_events_mv TO raw.geo_events AS
SELECT * FROM raw.geo_events_kafka;
