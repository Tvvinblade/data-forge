-- MergeTree таблицы для хранения raw данных

CREATE DATABASE IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.location_events (
    event_id String,
    page_url String,
    page_url_path String,
    referer_url String,
    referer_medium String,
    utm_medium String,
    utm_source String,
    utm_content String,
    utm_campaign String,
    _loaded_at DateTime DEFAULT now()
) ENGINE = MergeTree() 
ORDER BY (event_id, _loaded_at)
PARTITION BY toYYYYMM(_loaded_at);

CREATE TABLE IF NOT EXISTS raw.browser_events (
    event_id String,
    event_timestamp String,
    event_type String,
    click_id String,
    browser_name String,
    browser_user_agent String,
    browser_language String,
    _loaded_at DateTime DEFAULT now()
) ENGINE = MergeTree() 
ORDER BY (event_id, _loaded_at)
PARTITION BY toYYYYMM(_loaded_at);

CREATE TABLE IF NOT EXISTS raw.device_events (
    click_id String,
    os String,
    os_name String,
    os_timezone String,
    device_type String,
    device_is_mobile Nullable(Bool),
    user_custom_id String,
    user_domain_id String,
    _loaded_at DateTime DEFAULT now()
) ENGINE = MergeTree() 
ORDER BY (click_id, _loaded_at)
PARTITION BY toYYYYMM(_loaded_at);

CREATE TABLE IF NOT EXISTS raw.geo_events (
    click_id String,
    geo_latitude String,
    geo_longitude String,
    geo_country String,
    geo_timezone String,
    geo_region_name String,
    ip_address String,
    _loaded_at DateTime DEFAULT now()
) ENGINE = MergeTree() 
ORDER BY (click_id, _loaded_at)
PARTITION BY toYYYYMM(_loaded_at);
