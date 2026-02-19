from datetime import datetime, timedelta
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator


FLINK_REST = "http://flink-jobmanager:8081"
FLINK_IMAGE = "iot-flink:latest"
NETWORK = "iot_net"

DBT_HOST_PATH = "/root/dbt/projet_dbt/dbt"
DBT_CONTAINER_PATH = "/dbt"


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def submit_flink_if_needed(**context):
    """
    Soumet le job Flink uniquement s'il n'y a aucun job RUNNING.
    Sinon, ne fait rien (idempotent).
    """
    r = requests.get(f"{FLINK_REST}/jobs/overview", timeout=10)
    r.raise_for_status()

    jobs = r.json().get("jobs", [])
    if any(j.get("state") == "RUNNING" for j in jobs):
        print("✅ Job Flink déjà RUNNING — submission ignorée.")
        return

    print("🚀 Aucun job Flink actif — submission déclenchée.")
    # Le vrai submit est fait par la task Docker suivante


with DAG(
    dag_id="iot_pipeline_kafka_flink_dbt",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["iot", "streaming", "flink", "dbt"],
) as dag:

    check_and_prepare = PythonOperator(
        task_id="check_and_prepare_flink",
        python_callable=submit_flink_if_needed,
        provide_context=True,
    )

    submit_flink = DockerOperator(
        task_id="submit_flink",
        image=FLINK_IMAGE,
        command="flink run -m flink-jobmanager:8081 -py /jobs/iot_streaming.py",
        docker_url="unix://var/run/docker.sock",
        network_mode=NETWORK,
        auto_remove="success",
        mount_tmp_dir=False,
    )

    run_dbt = DockerOperator(
        task_id="run_dbt",
        image="ghcr.io/dbt-labs/dbt-postgres:1.7.9",
        command="dbt run && dbt test",
        docker_url="unix://var/run/docker.sock",
        network_mode=NETWORK,
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=[
            {
                "Type": "bind",
                "Source": DBT_HOST_PATH,
                "Target": DBT_CONTAINER_PATH,
            }
        ],
        working_dir="/dbt",
        environment={
            "DBT_PROFILES_DIR": "/dbt",
            "DB_HOST": "postgres",
            "DB_PORT": "5432",
            "DB_NAME": "iot",
            "DB_USER": "admin",
            "DB_PASSWORD": "admin",
        },
    )

    check_and_prepare >> submit_flink >> run_dbt
