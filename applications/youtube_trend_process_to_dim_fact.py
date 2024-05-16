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

spark = SparkSession.builder.config(conf=conf).appName('process_youtube_trend_data').getOrCreate()

print(spark.catalog.listTables())
hive_df = spark.read.table("youtube_videos")

video_dim_df = hive_df \
    .select("video_id", "title") \
    .distinct().orderBy('video_id', 'title') \
    .withColumn("video_key", monotonically_increasing_id() + 1)

channel_dim_df = hive_df \
    .select("channel_title") \
    .distinct().orderBy('channel_title') \
    .withColumn("channel_key", monotonically_increasing_id() + 1)

category_dim_df = hive_df \
    .select('category_title') \
    .distinct().orderBy('category_title') \
    .withColumn("category_key", monotonically_increasing_id() + 1)

date_dim_df = hive_df.select("publish_time").distinct().orderBy('publish_time')
date_dim_df = date_dim_df \
    .withColumn("publish_time_key", date_format(col("publish_time"), "yyyyMMdd")) \
    .withColumn("year", year(col("publish_time"))) \
    .withColumn("month", month("publish_time")) \
    .withColumn("day", dayofmonth("publish_time"))

region_dim_df = hive_df \
    .select('region') \
    .distinct().orderBy('region') \
    .withColumn("region_key", monotonically_increasing_id() + 1)

youtube_video_joined_df = hive_df \
    .join(video_dim_df, hive_df.video_id == video_dim_df.video_id, 'inner') \
    .join(category_dim_df, hive_df.category_title == category_dim_df.category_title, 'inner') \
    .join(channel_dim_df, hive_df.channel_title == channel_dim_df.channel_title, 'inner') \
    .join(region_dim_df, hive_df.region == region_dim_df.region, 'inner') \
    .join(date_dim_df, hive_df.publish_time == date_dim_df.publish_time, 'inner') \
    .select("video_key", "category_key", "channel_key", "publish_time_key", "region_key", "trending_date",
            "views", "likes", "dislikes", "comment_count")

youtube_video_fact_df = youtube_video_joined_df \
    .groupby("video_key", "category_key", "channel_key", "publish_time_key", "region_key") \
    .agg(max("views").alias("views"), max("likes").alias("likes"), max("dislikes").alias("dislikes"),
         max("comment_count").alias("comment_count"),
         min("trending_date").alias("start_trending_date"),
         max("trending_date").alias("end_trending_date")
         )

video_dim_df.write.mode("overwrite") \
    .options(header=True, delimiter=',') \
    .option('quote', '"') \
    .option("escape", '"') \
    .csv('s3a://youtube-video-dw/video_dim')

channel_dim_df.write.mode("overwrite") \
    .options(header=True, delimiter=',') \
    .option('quote', '"') \
    .option("escape", '"') \
    .csv('s3a://youtube-video-dw/channel_dim')

category_dim_df.write.mode("overwrite") \
    .options(header=True, delimiter=',') \
    .option('quote', '"') \
    .option("escape", '"') \
    .csv('s3a://youtube-video-dw/category_dim')

region_dim_df.write.mode("overwrite") \
    .options(header=True, delimiter=',') \
    .option('quote', '"') \
    .option("escape", '"') \
    .csv('s3a://youtube-video-dw/region_dim')

date_dim_df.write.mode("overwrite") \
    .options(header=True, delimiter=',') \
    .option('quote', '"') \
    .option("escape", '"') \
    .csv('s3a://youtube-video-dw/publish_date_dim')

youtube_video_fact_df.write.mode("overwrite") \
    .options(header=True, delimiter=',') \
    .option('quote', '"') \
    .option("escape", '"') \
    .csv('s3a://youtube-video-dw/youtube_video_fact')

spark.stop()

print("done")
