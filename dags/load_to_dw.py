from io import StringIO
from minio import Minio
import pandas as pd
from sqlalchemy import create_engine
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

MINIO_HOST = 'nginx:9000'
MINIO_ACCESS_KEY = 'WySCTIvfieAsGDPyAset'
MINIO_SECRET_KEY = 'YD3m0pszyLu7X4Ls8ZCzxVwe7rOJbH4qr8kxhPzs'
MINIO_BUCKET_NAME = 'youtube-video-dw'

POSTGRES_CONN_ID = 'postgresql://postgres:2242001@ytube_dw_postgres:5432/youtube_video_dw'

minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)


def load_csv_data_from_minio_to_warehouse_table(prefix, table_name):
    objects = minio_client.list_objects(MINIO_BUCKET_NAME, prefix=prefix, recursive=True)
    dfs = []
    for obj in objects:
        # Check if the object is a CSV file
        if obj.object_name.endswith('.csv'):
            csv_content = minio_client.get_object(MINIO_BUCKET_NAME, obj.object_name)

            df = pd.read_csv(StringIO(csv_content.data.decode('utf-8')))
            dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)

    engine = create_engine(POSTGRES_CONN_ID)
    combined_df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=20000)


with DAG(
        dag_id="load_to_dw",
        start_date=datetime(year=2022, month=11, day=4),
        schedule_interval=None,
        catchup=False
) as dag:
    # Start task
    task_start = DummyOperator(
        task_id="start",
    )

    # End task
    task_end = DummyOperator(
        task_id="end",
    )

    task_load_to_video_dim = PythonOperator(
        task_id='load_data_to_video_dim_table',
        python_callable=load_csv_data_from_minio_to_warehouse_table,
        op_kwargs={'prefix': 'video_dim', 'table_name': 'video_dim'},
    )

    task_load_to_channel_dim = PythonOperator(
        task_id='load_data_to_channel_dim_table',
        python_callable=load_csv_data_from_minio_to_warehouse_table,
        op_kwargs={'prefix': 'channel_dim', 'table_name': 'channel_dim'},
    )

    task_load_to_category_dim = PythonOperator(
        task_id='load_data_to_category_dim_table',
        python_callable=load_csv_data_from_minio_to_warehouse_table,
        op_kwargs={'prefix': 'category_dim', 'table_name': 'category_dim'},
    )

    task_load_to_region_dim = PythonOperator(
        task_id='load_data_to_region_dim_table',
        python_callable=load_csv_data_from_minio_to_warehouse_table,
        op_kwargs={'prefix': 'region_dim', 'table_name': 'region_dim'},
    )

    task_load_to_date_dim = PythonOperator(
        task_id='load_data_to_date_dim_table',
        python_callable=load_csv_data_from_minio_to_warehouse_table,
        op_kwargs={'prefix': 'publish_date_dim', 'table_name': 'date_dim'},
    )

    task_load_to_youtube_video_fact = PythonOperator(
        task_id='load_data_to_youtube_video_fact_table',
        python_callable=load_csv_data_from_minio_to_warehouse_table,
        op_kwargs={'prefix': 'youtube_video_fact', 'table_name': 'youtube_video_fact'},
    )

    task_start >> [task_load_to_video_dim, task_load_to_channel_dim, task_load_to_category_dim, task_load_to_region_dim,
                   task_load_to_date_dim] >> task_load_to_youtube_video_fact >> task_end
