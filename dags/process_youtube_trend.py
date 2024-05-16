from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
        dag_id="process_youtube_trend",
        start_date=datetime(year=2022, month=11, day=4),
        schedule_interval=None,
        catchup=False
) as dag:
    # Start task
    task_start = DummyOperator(
        task_id="Start",
    )

    # End task
    task_end = DummyOperator(
        task_id="End",
    )

    task_process_data = SparkSubmitOperator(
        task_id=f"process_youtube_trend_process_save_to_dw",
        conn_id="apache_spark_master",
        application="/opt/airflow/applications/youtube_trend_process_save_to_dw.py",
    )

    task_start >> task_process_data >> task_end
