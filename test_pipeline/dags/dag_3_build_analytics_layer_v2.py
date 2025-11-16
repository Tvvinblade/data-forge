# dags/dag_3_build_analytics_layer.py (обновленный)
"""
DAG 3: Построение Analytics Layer
- Читает SQL файлы из /sql директории
- Создаёт analytics таблицы
- Читает данные из raw слоя
- Обогащает данные в Pandas
- Записывает результат в analytics таблицы ClickHouse
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.clickhouse.operators.clickhouse import ClickHouseOperator
from airflow.operators.python import PythonOperator
from pathlib import Path
import pandas as pd
import logging
from clickhouse_driver import Client

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

# ==================== ЗАГРУЗКА SQL ====================

SQL_CREATE_ANALYTICS_TABLES = load_sql_file('create_analytics_tables.sql')

# SQL для читания сырых данных
SQL_READ_RAW_DATA = """
SELECT 
    b.click_id,
    b.event_id,
    l.page_url_path,
    l.utm_source,
    l.utm_campaign,
    l.utm_medium,
    d.device_type,
    d.os_name,
    g.geo_country,
    g.geo_region_name,
    b.event_timestamp
FROM raw.browser_events b
LEFT JOIN raw.location_events l ON b.event_id = l.event_id
LEFT JOIN raw.device_events d ON b.click_id = d.click_id
LEFT JOIN raw.geo_events g ON b.click_id = g.click_id
ORDER BY b.click_id, b.event_timestamp
"""

# ==================== ФУНКЦИИ ====================

def enrich_and_build_analytics(**context):
    """Обогащение данных pandas-ом и построение витрин"""
    logger.info("=== Building Analytics Layer with Pandas ===")
    
    # Читаем сырые данные из ClickHouse
    client = Client('clickhouse')
    
    logger.info("Reading raw data from ClickHouse...")
    result = client.execute(SQL_READ_RAW_DATA)
    
    # Преобразуем в DataFrame
    columns = [
        'user_id', 'event_id', 'page_url_path', 'utm_source', 'utm_campaign',
        'utm_medium', 'device_type', 'os_name', 'geo_country', 'geo_region_name',
        'event_timestamp'
    ]
    
    df = pd.DataFrame(result, columns=columns)
    logger.info(f"✓ Loaded {len(df)} rows from raw layer")
    
    # ==================== ОБОГАЩЕНИЕ ДАННЫХ ====================
    
    # 1. Преобразование типов
    df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
    
    # 2. Заполнение NULL значений
    df['device_type'] = df['device_type'].fillna('Unknown')
    df['os_name'] = df['os_name'].fillna('Unknown')
    df['geo_country'] = df['geo_country'].fillna('Unknown')
    
    # 3. Создание флага конверсии
    df['conversion_flag'] = (df['page_url_path'] == '/confirmation').astype(int)
    
    # 4. Группировка по пользователям для user_activity
    logger.info("Building user_activity...")
    user_activity = df.groupby('user_id').agg({
        'device_type': 'first',
        'os_name': 'first',
        'geo_country': 'first',
        'geo_region_name': 'first',
        'event_id': 'count',  # total_events
        'page_url_path': 'nunique',  # unique_pages
        'event_timestamp': ['min', 'max'],
        'conversion_flag': 'max',  # conversion_flag
    }).reset_index()
    
    user_activity.columns = [
        'user_id', 'device_type', 'os_name', 'geo_country', 'geo_region_name',
        'total_events', 'unique_pages', 'first_event_time', 'last_event_time',
        'conversion_flag'
    ]
    
    # Добавляем session_count (условно: по количеству часов)
    user_activity['session_count'] = (
        (user_activity['last_event_time'] - user_activity['first_event_time']).dt.total_seconds() / 3600
    ).fillna(1).astype(int) + 1
    
    logger.info(f"✓ Built user_activity with {len(user_activity)} users")
    
    # 5. Построение traffic_source_metrics
    logger.info("Building traffic_source_metrics...")
    traffic_metrics = df.groupby(
        ['utm_source', 'utm_campaign', 'utm_medium', 'device_type', 'geo_country']
    ).agg({
        'event_id': 'count',  # visits
        'user_id': 'nunique',  # unique_users
        'conversion_flag': 'sum',  # conversions
    }).reset_index()
    
    traffic_metrics.columns = [
        'utm_source', 'utm_campaign', 'utm_medium', 'device_type', 'geo_country',
        'visits', 'unique_users', 'conversions'
    ]
    
    traffic_metrics['conversion_rate'] = (
        traffic_metrics['conversions'] / traffic_metrics['visits'] * 100
    ).round(2)
    
    logger.info(f"✓ Built traffic_source_metrics with {len(traffic_metrics)} rows")
    
    # 6. Построение funnel_analysis
    logger.info("Building funnel_analysis...")
    funnel_order = {
        '/home': 1,
        '/product_a': 2,
        '/product_b': 2,
        '/cart': 3,
        '/payment': 4,
        '/confirmation': 5,
    }
    
    df['step_order'] = df['page_url_path'].map(funnel_order).fillna(0)
    
    funnel_analysis = df.groupby(['page_url_path', 'step_order']).agg({
        'event_id': 'count',  # visits
        'user_id': 'nunique',  # unique_users
    }).reset_index()
    
    funnel_analysis.columns = ['funnel_step', 'step_order', 'visits', 'unique_users']
    funnel_analysis = funnel_analysis.sort_values('step_order')
    
    logger.info(f"✓ Built funnel_analysis with {len(funnel_analysis)} steps")
    
    # ==================== ВСТАВКА В CLICKHOUSE ====================
    
    # Вставка user_activity
    client.execute("TRUNCATE TABLE analytics.user_activity")
    client.execute(
        "INSERT INTO analytics.user_activity VALUES",
        [
            (
                row['user_id'],
                row['device_type'],
                row['os_name'],
                row['geo_country'],
                row['geo_region_name'],
                row['total_events'],
                row['unique_pages'],
                row['first_event_time'].isoformat(),
                row['last_event_time'].isoformat(),
                row['session_count'],
                row['conversion_flag'],
            )
            for _, row in user_activity.iterrows()
        ]
    )
    logger.info(f"✓ Inserted {len(user_activity)} rows to analytics.user_activity")
    
    # Вставка traffic_source_metrics
    client.execute("TRUNCATE TABLE analytics.traffic_source_metrics")
    client.execute(
        "INSERT INTO analytics.traffic_source_metrics VALUES",
        [
            (
                row['utm_source'],
                row['utm_campaign'],
                row['utm_medium'],
                row['device_type'],
                row['geo_country'],
                row['visits'],
                row['unique_users'],
                int(row['conversions']),
                row['conversion_rate'],
            )
            for _, row in traffic_metrics.iterrows()
        ]
    )
    logger.info(f"✓ Inserted {len(traffic_metrics)} rows to analytics.traffic_source_metrics")
    
    # Вставка funnel_analysis
    client.execute("TRUNCATE TABLE analytics.funnel_analysis")
    client.execute(
        "INSERT INTO analytics.funnel_analysis VALUES",
        [
            (
                row['funnel_step'],
                row['visits'],
                row['unique_users'],
                row['step_order'],
            )
            for _, row in funnel_analysis.iterrows()
        ]
    )
    logger.info(f"✓ Inserted {len(funnel_analysis)} rows to analytics.funnel_analysis")
    
    client.disconnect()
    logger.info("✓ Analytics layer built successfully!")


# ==================== DAG DEFINITION ====================

default_args = {
    'owner': 'data-engineer',
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    'dag_3_build_analytics_layer',
    default_args=default_args,
    description='Build Analytics Layer (Pandas enrichment + ClickHouse)',
    schedule_interval=None,  # Triggered by DAG 1
    start_date=datetime.now(),
    catchup=False,
    tags=['clickhouse', 'analytics', 'pandas', 'etl'],
)

# ==================== TASKS ====================

t_create_analytics_tables = ClickHouseOperator(
    task_id='create_analytics_tables',
    sql=SQL_CREATE_ANALYTICS_TABLES,
    clickhouse_conn_id='clickhouse_default',
    dag=dag,
)

t_enrich_and_build = PythonOperator(
    task_id='enrich_and_build_analytics',
    python_callable=enrich_and_build_analytics,
    provide_context=True,
    dag=dag,
)

# ==================== DEPENDENCIES ====================

t_create_analytics_tables >> t_enrich_and_build
