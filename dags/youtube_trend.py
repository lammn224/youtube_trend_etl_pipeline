from io import StringIO
from minio import Minio
import pandas as pd
from sqlalchemy import create_engine
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

region_list = ["CA", "DE", "FR", "GB", "IN", "JP", "KR", "MX", "RU", "US"]

MINIO_HOST = 'nginx:9000'
MINIO_ACCESS_KEY = 'WySCTIvfieAsGDPyAset'
MINIO_SECRET_KEY = 'YD3m0pszyLu7X4Ls8ZCzxVwe7rOJbH4qr8kxhPzs'
POSTGRES_CONN_ID = 'postgresql://postgres:2242001@ytube_dw_postgres:5432/youtube_video_dw'
MINIO_DW_BUCKET_NAME = 'youtube-video-dw'

minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)


def load_csv_data_from_minio_to_warehouse_table(prefix, table_name):
    objects = minio_client.list_objects(MINIO_DW_BUCKET_NAME, prefix=prefix, recursive=True)
    dfs = []
    for obj in objects:
        # Check if the object is a CSV file
        if obj.object_name.endswith('.csv'):
            csv_content = minio_client.get_object(MINIO_DW_BUCKET_NAME, obj.object_name)

            df = pd.read_csv(StringIO(csv_content.data.decode('utf-8')))
            dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)

    engine = create_engine(POSTGRES_CONN_ID)
    combined_df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=20000)


with DAG(
        dag_id="youtube_trend",
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

    task_process_to_dim_fact = BashOperator(
        task_id=f"process_to_dim_fact",
        bash_command="bash /opt/airflow/bash/process_data_to_dim_fact.sh none"
    )

    for region in region_list:
        task_clean_data = BashOperator(
            task_id=f"clean_data_region_{region}",
            bash_command=f"bash /opt/airflow/bash/clean_data_youtube_video.sh {region}"
        )

        task_start >> task_clean_data >> task_process_to_dim_fact

    task_create_table = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="ytube_dw_postgres",
        sql="""
            create table if not exists video_dim(
                video_id varchar(255) not null,
                video_title varchar(255) not null,
                video_key int primary key not null
            );

            create table if not exists channel_dim(
                channel_title varchar(255) not null,
                channel_key int primary key not null
            );

            create table if not exists category_dim(
                category_title varchar(255) not null,
                category_key int primary key not null
            );

            create table if not exists date_dim(
                publish_time date not null,
                publish_time_key int primary key not null,
                year int not null,
                month int not null,
                day int not null
            );

            create table if not exists region_dim(
                region varchar(255) not null,
                region_key int primary key not null
            );

            create table if not exists youtube_video_fact(
                video_key int not null,
                channel_key int not null,
                category_key int not null,
                publish_time_key int not null,
                region_key int not null,
                views int not null,
                likes int not null,
                unlikes int not null,
                comment_count int not null,
                start_trending_date varchar(255) not null,
                end_trending_date varchar(255) not null,
                primary key (video_key, channel_key, category_key, region_key, publish_time_key)
            )
        """
    )

    task_process_to_dim_fact >> task_create_table

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

    task_create_table >> [task_load_to_video_dim, task_load_to_channel_dim, task_load_to_category_dim,
                          task_load_to_region_dim,
                          task_load_to_date_dim] >> task_load_to_youtube_video_fact >> task_end
