import os
import psycopg2
from datetime import datetime


def check_data_quality():
    """Проверка качества данных после ETL"""
    db_config = {
        'dbname': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('POSTGRES_HOST', 'postgres_db'),
        'port': os.getenv('POSTGRES_PORT', '5432')
    }

    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    try:
        # 1. Проверка полноты данных
        print("Checking data completeness...")
        tables = ['users', 'orders', 'user_activity', 'analytics.user_segments', 'analytics.marketing_mart']

        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            if count == 0:
                raise ValueError(f"Table {table} is empty!")
            print(f"Table {table}: {count} records")

        # 2. Проверка актуальности данных
        print("Checking data freshness...")
        today = datetime.now().date()

        cursor.execute("SELECT MAX(calculation_date) FROM analytics.user_segments")
        latest_calc = cursor.fetchone()[0]
        if latest_calc != today:
            raise ValueError(f"User segments are not up to date! Latest: {latest_calc}, Expected: {today}")

        cursor.execute("SELECT MAX(calculation_date) FROM analytics.marketing_mart")
        latest_mart = cursor.fetchone()[0]
        if latest_mart != today:
            raise ValueError(f"Marketing mart is not up to date! Latest: {latest_mart}, Expected: {today}")

        # 3. Проверка целостности сегментов
        print("Checking segments integrity...")
        cursor.execute("""
            SELECT COUNT(*) as invalid_records
            FROM analytics.user_segments 
            WHERE user_id NOT IN (SELECT user_id FROM users)
        """)
        invalid_segments = cursor.fetchone()[0]
        if invalid_segments > 0:
            raise ValueError(f"Found {invalid_segments} invalid user segments!")

        # 4. Проверка целостности маркетинговой витрины
        print("Checking marketing mart integrity...")
        cursor.execute("""
            SELECT COUNT(*) as null_records
            FROM analytics.marketing_mart 
            WHERE user_id IS NULL OR segment_name IS NULL
        """)
        null_records = cursor.fetchone()[0]
        if null_records > 0:
            raise ValueError(f"Found {null_records} records with NULL values in marketing mart!")

        print("All data quality checks passed successfully!")

    except Exception as e:
        print(f"Data quality check failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    check_data_quality()