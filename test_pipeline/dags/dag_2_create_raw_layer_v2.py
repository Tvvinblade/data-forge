# dags/dag_2_create_raw_layer.py (обновленный)
"""
DAG 2: Создание Raw Layer в ClickHouse
- Читает SQL файлы из /sql директории
- Создаёт Kafka Engine таблицы
- Создаёт Raw MergeTree таблицы
- Создаёт Materialized Views
- Использует ClickHouseOperator
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.clickhouse.operators.clickhouse import ClickHouseOperator
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

# ==================== ФУНКЦИЯ ЗАГРУЗКИ SQL ====================

def load_sql_file(filename: str) -> str:
    """Загружает SQL файл из /sql директории"""
    sql_path = Path('/opt/airflow/dags/../sql') / filename
    # Для локальной разработки:
    # sql_path = Path(__file__).parent.parent / 'sql' / filename
    
    logger.info(f"Loading SQL from {sql_path}")
    with open(sql_path, 'r') as f:
        return f.read()

# ==================== ЗАГРУЗКА SQL СКРИПТОВ ====================

SQL_CREATE_DATABASES = "CREATE DATABASE IF NOT EXISTS raw;"

SQL_CREATE_KAFKA_TABLES = load_sql_file('create_kafka_tables.sql')
SQL_CREATE_RAW_TABLES = load_sql_file('create_raw_tables.sql')
SQL_CREATE_MATERIALIZED_VIEWS = load_sql_file('create_raw_mvs.sql')

# ==================== DAG DEFINITION ====================

default_args = {
    'owner': 'data-engineer',
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    'dag_2_create_raw_layer',
    default_args=default_args,
    description='Create Raw Layer in ClickHouse (Kafka → MergeTree)',
    schedule_interval=None,  # Triggered by DAG 1
    start_date=datetime.now(),
    catchup=False,
    tags=['clickhouse', 'raw-layer', 'etl'],
)

# ==================== TASKS ====================

# 1. Создание БД
t_create_db = ClickHouseOperator(
    task_id='create_raw_database',
    sql=SQL_CREATE_DATABASES,
    clickhouse_conn_id='clickhouse_default',
    dag=dag,
)

# 2. Создание Kafka Engine таблиц
t_create_kafka_tables = ClickHouseOperator(
    task_id='create_kafka_tables',
    sql=SQL_CREATE_KAFKA_TABLES,
    clickhouse_conn_id='clickhouse_default',
    dag=dag,
)

# 3. Создание Raw таблиц (MergeTree)
t_create_raw_tables = ClickHouseOperator(
    task_id='create_raw_tables',
    sql=SQL_CREATE_RAW_TABLES,
    clickhouse_conn_id='clickhouse_default',
    dag=dag,
)

# 4. Создание Materialized Views
t_create_mvs = ClickHouseOperator(
    task_id='create_materialized_views',
    sql=SQL_CREATE_MATERIALIZED_VIEWS,
    clickhouse_conn_id='clickhouse_default',
    dag=dag,
)

# ==================== DEPENDENCIES ====================

t_create_db >> t_create_kafka_tables >> t_create_raw_tables >> t_create_mvs
