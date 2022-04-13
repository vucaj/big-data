#!/usr/bin/python

import os
import time
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

from pyspark.sql.types import *

Hdf_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

print("======================================== Transformation zone - Beggining ========================================\n")
spark = SparkSession.builder.appName("transformation").getOrCreate()

schemaString = "video_id trending_date title channel_title category_id publish_time tags views likes dislikes comment_count thumbnail_link comments_disabled ratings_disabled video_error_or_removed description"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

print(Hdf_NAMENODE)
print("====================================================================================================================")
# df_ca = spark.read.csv(Hdf_NAMENODE+"/videos/CAvideos.csv", header=True, mode="DROPMALFORMED", schema=schema)
df_ca = spark.read.option("multiline", "true").option("sep", ",").option("header", "true").option("inferSchema", "true").csv(Hdf_NAMENODE+"/videos/CAvideos.csv")
print("====================================================================================================================")


print("Casting views to int \n")
df_ca = df_ca.withColumn("views", df_ca["views"].cast(IntegerType()))

print("Casting likes to int \n")
df_ca = df_ca.withColumn("likes", df_ca["likes"].cast(IntegerType()))

print("Casting dislikes to int \n")
df_ca = df_ca.withColumn("dislikes", df_ca["dislikes"].cast(IntegerType()))

print("Casting timestamp to int")
df_ca = df_ca.withColumn("hour", df_ca['publish_time'].substr(12, 2).cast(IntegerType()))

print("======================================== Transformation zone - End ========================================\n")


while True:
    try: 
        print('==================================== writing ================================================')
        df_ca.write.option("header", "true").csv(Hdf_NAMENODE+"/transformation_zone/CAvideos.csv", mode="ignore")
        print("Spark wrote transformed df to HDFS\n")
        print('==================================== writing ++++++++++++++++++++++++++++++++++================================================')

        break
    except: 
        print("Writting failed. Trying again in 5s\n")
        time.sleep(5)


