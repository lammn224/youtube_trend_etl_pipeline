This repo design **ETL pipeline** to ingest YouTube data into data warehouse.
This pipline is scheduled by using **Apache Airflow**

## Description

This repo design **ETL pipeline** to ingest YouTube data into data warehouse.
This pipline is scheduled by using Apache Airflow
![data-flow](assets/data-flow.png)

- This data warehouse help to find out what videos, categories...are of most interest based on views, likes, dislikes.

- This will be helpful in enabling YouTube reach some of its analytical goals, make some business decision that will
  improve user experiences.

## About the dataset

- This dataset includes several months (and counting) of data on daily trending YouTube videos. Data is included for the
  US, GB, DE, CA, and FR regions (USA, Great Britain, Germany, Canada, and France, respectively), with up to 200 listed
  trending videos per day.
- Each region’s data is in a separate file. Data includes the video title, channel title, publish time, tags, views,
  likes
  and dislikes, description, and comment count.
  ![raw_data_set](assets/raw_data_set.png)
- The data also includes a `category_id` field, which varies between regions. To retrieve the categories for a specific
  video, find it in the associated `JSON`. One such file is included for each of the five regions in the dataset.

Source: [Kaggle - Trending YouTube Video Statistics](https://www.kaggle.com/datasets/datasnaek/youtube-new)

## Data warehouse design

![entity_relationship](assets/entity_relationship.png)

## ETL Pipeline

![airflow_graph](assets/airflow_graph.png)

## Run the project

### Prerequisite

- install minio for storage
- install hive-metastore
- install trino or presto to query
- install data warehouse (The DW in this project is postgres)
  ![data-platform](assets/data-platform.png)

Check this repo: [Setup datalake platform](https://github.com/lammn224/docker_datalake_platform)

### Build the Airflow image

```bash
docker build -t airflow-spark .
```

### Start and run the Airflow containers

```bash
docker-compose up -d
```

### Airflow Web UI

#### Access address http://localhost:8088

![airflow_ui](assets/airflow_ui.png)

#### Monitor DAGs

![airflow_dag](assets/airflow_dag.png)

[1]: http://www.github.com/lammn224

[2]: https://www.linkedin.com/in/lammn

--- 

**Connect with me**

[<img alt="github" height="50" src="https://cloud.githubusercontent.com/assets/17016297/18839843/0e06a67a-83d2-11e6-993a-b35a182500e0.png" width="50"/>][1]
[<img alt="linkedin" height="50" src="https://cloud.githubusercontent.com/assets/17016297/18839848/0fc7e74e-83d2-11e6-8c6a-277fc9d6e067.png" width="50"/>][2]

---



