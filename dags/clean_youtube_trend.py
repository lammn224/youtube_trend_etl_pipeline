from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

region_list = ["CA", "DE", "FR", "GB", "IN", "JP", "KR", "MX", "RU", "US"]

with DAG(
        dag_id="clean_youtube_trend",
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

    for region in region_list:
        task_clean_data = SparkSubmitOperator(
            task_id=f"clean_data_region_{region}",
            conn_id="apache_spark_master",
            application="/opt/airflow/applications/youtube_trend_clean_data.py",
            application_args=[f"{region}"],
        )

        task_start >> task_clean_data >> task_end
