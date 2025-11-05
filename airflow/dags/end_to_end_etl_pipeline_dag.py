from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'e2e_etl_pipeline',
    default_args=default_args,
    description="Запуск всего пайплайна",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(minutes=2),
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    generate_data_task = DockerOperator(
        task_id='generate_test_data',
        image='usersegments-factory-data_generator:latest',
        environment={
            'POSTGRES_DB': '{{ var.value.POSTGRES_DB }}',
            'POSTGRES_USER': '{{ var.value.POSTGRES_USER }}',
            'POSTGRES_PASSWORD': '{{ var.value.POSTGRES_PASSWORD }}',
            'POSTGRES_HOST': 'postgres_db',
            'POSTGRES_PORT': '5432'
        },
        api_version='auto',
        auto_remove=True,
        command='python3 data_generator.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='usersegments-factory_etl-network',
        mount_tmp_dir=False
    )

    spark_etl_task = BashOperator(
        task_id='run_spark_etl',
        bash_command='docker exec spark /opt/spark/bin/spark-submit --master spark://spark:7077 /opt/spark/scripts/etl_script.py'
    )

    data_quality_check_task = DockerOperator(
        task_id='data_quality_check',
        image='usersegments-factory-data_quality_checker:latest',
        environment={
            'POSTGRES_DB': '{{ var.value.POSTGRES_DB }}',
            'POSTGRES_USER': '{{ var.value.POSTGRES_USER }}',
            'POSTGRES_PASSWORD': '{{ var.value.POSTGRES_PASSWORD }}',
            'POSTGRES_HOST': 'postgres_db',
            'POSTGRES_PORT': '5432'
        },
        api_version='auto',
        auto_remove=True,
        command='python3 data_quality_check.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='usersegments-factory_etl-network',
        mount_tmp_dir=False
    )

    generate_data_task >> spark_etl_task >> data_quality_check_task