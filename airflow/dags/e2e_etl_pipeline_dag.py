from datetime import datetime, timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag


@dag(
    dag_id="e2e_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(minutes=2),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["etl", "pipeline"],
    is_paused_upon_creation=False,
)
def e2e_etl_pipeline():
    docker_env = {
        "POSTGRES_DB": "{{ var.value.POSTGRES_DB }}",
        "POSTGRES_USER": "{{ var.value.POSTGRES_USER }}",
        "POSTGRES_PASSWORD": "{{ var.value.POSTGRES_PASSWORD }}",
        "POSTGRES_HOST": "postgres_db",
        "POSTGRES_PORT": "5432",
    }

    docker_kwargs = {
        "api_version": "auto",
        "auto_remove": True,
        "docker_url": "unix://var/run/docker.sock",
        "network_mode": "deusersegments-factory_etl-network",
        "mount_tmp_dir": False,
    }

    generate_data_task = DockerOperator(
        task_id="generate_test_data",
        image="deusersegments-factory-data_generator:latest",
        environment=docker_env,
        command="python3 data_generator.py",
        **docker_kwargs
    )

    spark_etl_task = BashOperator(
        task_id="run_spark_etl",
        bash_command="docker exec spark /opt/spark/bin/spark-submit --master spark://spark:7077 /opt/spark/scripts/etl_script.py",
    )

    data_quality_check_task = DockerOperator(
        task_id="data_quality_check",
        image="deusersegments-factory-data_quality_checker:latest",
        environment=docker_env,
        command="python3 data_quality_check.py",
        **docker_kwargs
    )

    generate_data_task >> spark_etl_task >> data_quality_check_task


e2e_etl_pipeline()
