import os


def get_db_config():
    config = (
        f"jdbc:postgresql://postgres_db:{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'etl_pipeline')}",
        os.getenv("POSTGRES_USER", "airflow"),
        os.getenv("POSTGRES_PASSWORD", "airflow"),
    )

    return config
