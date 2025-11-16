-- Kafka Engine таблицы для чтения из Kafka topics

CREATE TABLE IF NOT EXISTS raw.location_events_kafka (
    event_id String,
    page_url String,
    page_url_path String,
    referer_url String,
    referer_medium String,
    utm_medium String,
    utm_source String,
    utm_content String,
    utm_campaign String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'location-events',
    kafka_group_id = 'clickhouse-raw-location',
    kafka_format = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS raw.browser_events_kafka (
    event_id String,
    event_timestamp String,
    event_type String,
    click_id String,
    browser_name String,
    browser_user_agent String,
    browser_language String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'browser-events',
    kafka_group_id = 'clickhouse-raw-browser',
    kafka_format = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS raw.device_events_kafka (
    click_id String,
    os String,
    os_name String,
    os_timezone String,
    device_type String,
    device_is_mobile Nullable(Bool),
    user_custom_id String,
    user_domain_id String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'device-events',
    kafka_group_id = 'clickhouse-raw-device',
    kafka_format = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS raw.geo_events_kafka (
    click_id String,
    geo_latitude String,
    geo_longitude String,
    geo_country String,
    geo_timezone String,
    geo_region_name String,
    ip_address String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'geo-events',
    kafka_group_id = 'clickhouse-raw-geo',
    kafka_format = 'JSONEachRow';