#!/usr/bin/python

import os
import time
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

from pyspark.sql.types import *

Hdf_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]


sparkMongo = SparkSession.builder.appName("currated").config("spark.mongodb.output.uri", "mongodb://mongodb:27017/currated-zone").getOrCreate()
# spark = SparkSession.builder.config("spark.jars", "/home/hduser/mysql-connector-java-5.1.47/mysql-connector-java-5.1.47.jar") \
#     .master("local").appName("PySpark_MySQL_test2").getOrCreate()

print(Hdf_NAMENODE)
while True:
    try:
        print("==================================================================================================")
        df_ca = sparkMongo.read.option("multiline", "true").option("sep", ",").option("header", "true").option("inferSchema", "true").csv(Hdf_NAMENODE+"/transformation_zone/CAvideos.csv")
        print("Spark read transformed df\n")
        print("==================================================================================================")
        break
    except Exception as e:
        print(e)
        print("Reading failed. Trying again in 5s\n")
        time.sleep(5)


df_cat = df_ca.groupBy("category_id").avg("views").withColumnRenamed("avg(views)", "avg_views")
df_vid_count = df_ca.groupBy("category_id").count().withColumnRenamed("count", "video_count")
df_like_avg = df_ca.groupBy("category_id").avg("likes").withColumnRenamed("avg(likes)", "avg_likes")
df_dislike_avg = df_ca.groupBy("category_id").avg("dislikes").withColumnRenamed("avg(dislikes)", "avg_dislikes")
df_pt_0_8 = df_ca.filter(((F.col('hour') >= 0) & (F.col('hour') < 8))).groupBy("category_id").count().withColumnRenamed("count","publish_time_0_8_count")
df_pt_9_16 = df_ca.filter(((F.col('hour') >= 8) & (F.col('hour') < 16))).groupBy("category_id").count().withColumnRenamed("count","publish_time_9_16_count")
df_pt_17_0 = df_ca.filter(((F.col('hour') >= 16) & (F.col('hour') < 24))).groupBy("category_id").count().withColumnRenamed("count","publish_time_16_0_count")
df_highlight_count = df_ca.where(F.lower(F.col('title')).like('%highlights%')).groupBy("category_id").count().withColumnRenamed("count", "highlight_count")
df_highlight_avg_views = df_ca.where(F.lower(F.col('title')).like('%highlights%')).groupBy("category_id").avg('views').withColumnRenamed("avg(views)", "highlight_avg_views")
df_no_highlight_avg_views = df_ca.where(~F.lower(F.col('title')).like('%highlights%')).groupBy("category_id").avg('views').withColumnRenamed("avg(views)", "no_highlight_avg_views")
df_featuring_count = df_ca.where(F.lower(F.col('title')).like('%ft.%')).groupBy("category_id").count().withColumnRenamed("count", "featuring_count")
df_no_tags_count = df_ca.where(F.lower(F.col('tags')).like('[none]')).groupBy("category_id").count().withColumnRenamed("count", "no_tags_count")
df_tags_count = df_ca.where(~F.lower(F.col('tags')).like('[none]')).groupBy("category_id").count().withColumnRenamed("count", "with_tags_count")
df_no_tags_avg_views = df_ca.where(F.lower(F.col('tags')).like('[none]')).groupBy("category_id").avg('views').withColumnRenamed("avg(views)", "no_tags_avg_views")
df_tags_avg_views = df_ca.where(~F.lower(F.col('tags')).like('[none]')).groupBy("category_id").avg('views').withColumnRenamed("avg(views)", "with_tags_avg_views")




df = df_cat \
    .join(df_vid_count, ['category_id']) \
    .join(df_like_avg, ['category_id']) \
    .join(df_dislike_avg, ['category_id']) \
    .join(df_tags_count, ['category_id']) \
    .join(df_tags_avg_views, ['category_id']) \
    .join(df_no_tags_avg_views, ['category_id']) \
    .join(df_highlight_count, ['category_id']) \
    .join(df_highlight_avg_views, ['category_id']) \
    .join(df_pt_0_8, ['category_id']) \
    .join(df_pt_9_16, ['category_id']) \
    .join(df_pt_17_0, ['category_id']) \
    .join(df_featuring_count, ['category_id']) \
    .join(df_no_highlight_avg_views, ['category_id'])



df.show()

df.write.format("mongo").mode("overwrite").option("database", "currated-zone").option("collection", "yt-stats").save()
