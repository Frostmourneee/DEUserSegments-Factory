from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta


def check_data_completeness():
    """Проверка полноты данных"""
    hook = PostgresHook(postgres_conn_id='postgres_default')

    tables = ['users', 'orders', 'user_activity', 'analytics.user_segments', 'analytics.marketing_mart']

    for table in tables:
        count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
        if count == 0:
            raise ValueError(f"Table {table} is empty!")
        print(f"Table {table}: {count} records")


def check_data_freshness():
    """Проверка актуальности данных"""
    hook = PostgresHook(postgres_conn_id='postgres_default')

    today = datetime.now().date()

    latest_calc = hook.get_first("SELECT MAX(calculation_date) FROM analytics.user_segments")[0]
    if latest_calc != today:
        raise ValueError(f"User segments are not up to date! Latest: {latest_calc}, Expected: {today}")

    latest_mart = hook.get_first("SELECT MAX(calculation_date) FROM analytics.marketing_mart")[0]
    if latest_mart != today:
        raise ValueError(f"Marketing mart is not up to date! Latest: {latest_mart}, Expected: {today}")

    print("All data is fresh!")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'data_quality_dag',
        default_args=default_args,
        description='Проверка качества данных после ETL',
        schedule=None,
        catchup=False,
        tags=['data-quality'],
) as dag:
    pass
    # completeness_check = PythonOperator(
    #     task_id='check_data_completeness',
    #     python_callable=check_data_completeness,
    # )
    #
    # freshness_check = PythonOperator(
    #     task_id='check_data_freshness',
    #     python_callable=check_data_freshness,
    # )
    #
    # check_segments_integrity = PostgresOperator(
    #     task_id='check_segments_integrity',
    #     sql="""
    #     SELECT COUNT(*) as invalid_records
    #     FROM analytics.user_segments
    #     WHERE user_id NOT IN (SELECT user_id FROM users)
    #     """,
    #     postgres_conn_id='postgres_default'
    # )
    #
    # check_mart_integrity = PostgresOperator(
    #     task_id='check_mart_integrity',
    #     sql="""
    #     SELECT COUNT(*) as null_records
    #     FROM analytics.marketing_mart
    #     WHERE user_id IS NULL OR segment_name IS NULL
    #     """,
    #     postgres_conn_id='postgres_default'
    # )
    #
    # completeness_check >> freshness_check >> [check_segments_integrity, check_mart_integrity]