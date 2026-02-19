from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="flink_iot_stream",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    start_flink_job = BashOperator(
        task_id="start_flink_job",
        bash_command="""
        docker exec flink_jobmanager \
        flink run -d /opt/flink/jobs/iot_streaming.jar
        """
    )
