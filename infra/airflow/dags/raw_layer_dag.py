from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

clickhouse_raw_commands = """
clickhouse-client --host 10.8.0.1 --user admin --password admin --query "
CREATE TABLE IF NOT EXISTS raw_browser_events (
    event_time DateTime,
    user_id String,
    page String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = '10.8.0.1:29092',
         kafka_topic_list = 'browser_events',
         kafka_group_name = 'raw_consumer',
         kafka_format = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS raw_browser_events_buffer AS raw_browser_events
ENGINE = MergeTree()
ORDER BY event_time;

INSERT INTO raw_browser_events_buffer SELECT * FROM raw_browser_events;
"
"""

with DAG(
    dag_id="raw_layer_dag",
    start_date=datetime.now()
    schedule_interval=None,
    catchup=False,
    tags=["clickhouse", "raw"]
) as dag:
    load_raw = BashOperator(
        task_id="load_raw_data",
        bash_command=clickhouse_raw_commands
    )
