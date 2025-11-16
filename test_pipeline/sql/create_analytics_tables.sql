-- Analytics таблицы для хранения обогащенных данных

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.user_activity (
    user_id String,
    device_type Nullable(String),
    os_name Nullable(String),
    geo_country Nullable(String),
    geo_region_name Nullable(String),
    total_events UInt64,
    unique_pages UInt64,
    first_event_time Nullable(String),
    last_event_time Nullable(String),
    session_count UInt64,
    conversion_flag UInt8,
    _updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY user_id;

CREATE TABLE IF NOT EXISTS analytics.traffic_source_metrics (
    utm_source String,
    utm_campaign String,
    utm_medium String,
    device_type Nullable(String),
    geo_country Nullable(String),
    visits UInt64,
    unique_users UInt64,
    conversions UInt64,
    conversion_rate Float64,
    _updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (utm_source, utm_campaign, utm_medium);

CREATE TABLE IF NOT EXISTS analytics.funnel_analysis (
    funnel_step String,
    visits UInt64,
    unique_users UInt64,
    step_order UInt8,
    _updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY step_order;
