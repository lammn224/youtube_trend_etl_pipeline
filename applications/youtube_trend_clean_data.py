from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

conf = SparkConf()
conf.set("spark.hadoop.fs.s3a.endpoint", "http://nginx:9000")
# conf.set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
conf.set("spark.hadoop.fs.s3a.access.key", "WySCTIvfieAsGDPyAset")
conf.set("spark.hadoop.fs.s3a.secret.key", "YD3m0pszyLu7X4Ls8ZCzxVwe7rOJbH4qr8kxhPzs")
conf.set("spark.hadoop.fs.s3a.path.style.access", True)
conf.set("spark.hadoop.fs.s3a.S3AFileSystem", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("fs.s3a.connection.ssl.enabled", "false")

conf.set("spark.sql.catalogImplementation", "hive")
conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
conf.set("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
# conf.set("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083")

spark = SparkSession.builder.config(conf=conf).appName('clean_youtube_trend_data_spark_docker').getOrCreate()

# region = 'US'
region = sys.argv[1]

create_table_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS youtube_videos (
    video_id STRING,
    title STRING,
    channel_title STRING,
    category_title STRING,
    publish_time STRING,
    trending_date STRING,
    views INT,
    likes INT,
    unlikes INT,
    comment_count INT
)
PARTITIONED BY (region STRING, year INT, month INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\\""
)
STORED AS TEXTFILE
LOCATION 's3a://youtube-trend-cleansed/_data/';"""

# print(create_table_query)
spark.sql(create_table_query)

json_file_path = f's3a://youtube-trend-raw/{region}/{region}_category_id.json'
json_category_df = spark.read.option('multiline', 'true').json(json_file_path)
items_df = json_category_df.select(explode('items').alias('item'))

category_df = items_df \
    .select(
    items_df['item.id'].cast('int').alias('id'),
    items_df['item.snippet.title'].alias('category_title')) \
    .orderBy('id')

video_df = spark.read \
    .options(delimiter=",", header=True) \
    .option("multiline", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("ignoreLeadingWhiteSpace", True) \
    .option("inferSchema", True) \
    .csv(f's3a://youtube-trend-raw/{region}/*.csv')

video_df = video_df.select("video_id", "title", "channel_title", "category_id",
                           col("publish_time").cast("date").alias("publish_time"),
                           "trending_date", "views", "likes", "unlikes", "comment_count") \
    .withColumn("title", trim(col("title"))) \
    .withColumn("region", lit(region)) \
    .withColumn('year', year('publish_time')) \
    .withColumn('month', month('publish_time')) \
    .withColumn("trending_date", to_date(video_df["trending_date"], "yy.dd.MM"))
video_df = video_df.join(category_df, category_df['id'] == video_df.category_id, 'inner')
video_df = video_df.drop("id", "category_id").na.drop()

video_df \
    .select("video_id", "title", "channel_title", "category_title", "publish_time", "trending_date",
            "views", "likes", "unlikes", "comment_count", "region", "year", "month") \
    .write \
    .format('hive') \
    .mode('overwrite').insertInto("youtube_videos")

# video_df \
#     .write \
#     .partitionBy("region", "year", "month") \
#     .mode('overwrite') \
#     .options(header=True, delimiter=',') \
#     .csv(f's3a://youtube-trend-cleansed/')

spark.stop()
print("done")
