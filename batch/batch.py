#!/usr/bin/python

import math
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, expr, when, coalesce, greatest, udf, substring, regexp_replace
import pyspark.sql.functions as F
from pyspark.sql.types import *


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession.builder.appName("Youtube statistics").getOrCreate()
quiet_logs(spark)

######################################### Transformation zone ######################################### 

schemaString = "video_id trending_date title channel_title category_id publish_time tags views likes dislikes comment_count thumbnail_link comments_disabled ratings_disabled video_error_or_removed description"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

df_ca = spark.read.csv("hdfs://localhost:9000/videos/CAvideos.csv", header=True, mode="DROPMALFORMED", schema=schema)
df_ca = df_ca.withColumn("views", df_ca["views"].cast(IntegerType()))
df_ca = df_ca.withColumn("likes", df_ca["likes"].cast(IntegerType()))
df_ca = df_ca.withColumn("dislikes", df_ca["dislikes"].cast(IntegerType()))



df_ca_distinct = df_ca.distinct()

df_ca_distinct = df_ca_distinct.withColumn("hour", df_ca_distinct['publish_time'].substr(12, 2).cast(IntegerType()))

df_ca_distinct.createOrReplaceTempView('CA_VIDEOS')

######################################### End of Transformation zone ######################################### 

######################################### Currated zone ######################################### 
sparkMongo = SparkSession.builder.appName("currated-app").config("spark.mongodb.output.uri", "mongodb://mongodb:27017/currated-data").getOrCreate()

# LIKES AND VIEWS RATIO
print("==================================Likes/Views Ratio==================================")

avg_views = df_ca_distinct.groupBy("video_id").avg("views").agg({'avg(views)': 'avg'}).collect()[0][0]
avg_likes = df_ca_distinct.groupBy("video_id").avg("likes").agg({'avg(likes)': 'avg'}).collect()[0][0]

avg_views_df = df_ca_distinct.groupBy("video_id").avg("views").agg({'avg(views)': 'avg'})
avg_likes_df = df_ca_distinct.groupBy("video_id").avg("likes").agg({'avg(likes)': 'avg'})

# print('Writting started')
# avg_likes_df.write.format("mongo").mode("overwrite").option("database", "currated-data").option("collection", "avg_likes").save()
# avg_views_df.write.format("mongo").mode("overwrite").option("database", "currated-data").option("collection", "avg_views").save()
# print('Writing finished')

likes_views_ratio = avg_likes/avg_views

print('Average likes: {}\nAverage views: {}\nLikes/Views ratio: {}%'.format(round(avg_likes, 2), round(avg_views, 2), round(likes_views_ratio*100, 2)))

print("======================================================================================")


# LIKES AND DISLIKES RATIO
print("=================================Likes/Dislikes Ratio=================================")
#we can use avg_likes from previous calculation => we just need avg number of dislikes

avg_dislikes = df_ca_distinct.groupBy("video_id").avg("dislikes").agg({'avg(dislikes)': 'avg'}).collect()[0][0]

likes_dislikes_ratio = avg_likes/avg_dislikes

likes_percents = round(avg_likes/(avg_likes+avg_dislikes) * 100, 2)

print('Average likes: {0} ({3}%)\nAverage dislikes: {1} ({4}%)\nLikes/Dislikes ratio: {2}'.format(round(avg_likes, 2), round(avg_dislikes, 2), round(likes_dislikes_ratio, 2), likes_percents, round(100-likes_percents,2)))

print("======================================================================================")

# AVERAGE VIEWS PER VIDEO CATEGORY
print("==================================Views per Category==================================")

avg_views_per_category = df_ca_distinct.groupBy("category_id").avg("views")
avg_views_per_category.show()

print("======================================================================================")

# AVERAGE VIEWS ON SPORTS VIDEOS DEPENDING ON IF THEY HAVE "HIGHLIGHTS" IN THEIR TITLES"
print("==================================Sports Highlights==================================")

query_sports_highlights = "SELECT *\
                FROM CA_VIDEOS\
                WHERE category_id='17' AND LOWER(title) LIKE '%highlights%';"

query_sports_no_highlights = "SELECT *\
                              FROM CA_VIDEOS\
                              WHERE category_id='17' AND LOWER(title) NOT LIKE '%highlights%';"

sql_sports_higlights = spark.sql(query_sports_highlights)
sql_sports_no_highlights = spark.sql(query_sports_no_highlights)

avg_views_highlights = sql_sports_higlights.groupBy('video_id').avg('views').agg({'avg(views)': 'avg'}).collect()[0][0]
avg_views_no_highlights = sql_sports_no_highlights.groupBy('video_id').avg('views').agg({'avg(views)': 'avg'}).collect()[0][0]

highlight_no_highlight_ratio = avg_views_highlights/avg_views_no_highlights

print('Views on videos with "highlight" : {0}\nViews on videos without "highlight": {1}\nHiglight / No Highlight ratio: {2}'.format(round(avg_views_highlights, 2), round(avg_views_no_highlights, 2), round(highlight_no_highlight_ratio, 2)))
print("======================================================================================")

# AVERAGE VIEWS ON VIDEOS DEPENDING ON IF THEY OR DON'T HAVE TAGS
print("==================================Video Tags==================================")

query_with_tags = "SELECT *\
                   FROM CA_VIDEOS\
                   WHERE tags NOT LIKE '[none]';"

query_without_tags = "SELECT *\
                      FROM CA_VIDEOS\
                      WHERE tags LIKE '[none]'"

sql_with_tags = spark.sql(query_with_tags)
sql_without_tags = spark.sql(query_without_tags)

avg_views_with_tags = sql_with_tags.groupBy('video_id').avg('views').agg({'avg(views)': 'avg'}).collect()[0][0]
avg_views_without_tags = sql_without_tags.groupBy('video_id').avg('views').agg({'avg(views)': 'avg'}).collect()[0][0]

with_without_tags_ratio = avg_views_with_tags/avg_views_without_tags
print('Average views on videos with tags : {0}\nAverage views on videos without tags: {1}\nWith tags / Without tags ratio: {2}'.format(round(avg_views_with_tags, 2), round(avg_views_without_tags, 2), round(with_without_tags_ratio, 2)))


print("======================================================================================")

# AVERAGE VIEWS ON VIDEOS DEPENDING ON IF THEY OR DON'T HAVE FT IN THEIR TITLE
print("===================================Video Featuring===================================")

query_music_featurig = "SELECT *\
                        FROM CA_VIDEOS\
                        WHERE category_id='10' AND LOWER(title) LIKE '%ft%';"

query_music = "SELECT *\
              FROM CA_VIDEOS\
              WHERE category_id='10' AND LOWER(title) NOT LIKE '%ft%';"

sql_featuring = spark.sql(query_music_featurig)
sql_not_featuring = spark.sql(query_music)

avg_views_featuring = sql_featuring.groupBy('video_id').avg('views').agg({'avg(views)': 'avg'}).collect()[0][0]
avg_views_not_featuring = sql_not_featuring.groupBy('video_id').avg('views').agg({'avg(views)': 'avg'}).collect()[0][0]

print(avg_views_featuring)
print(avg_views_not_featuring)

with_without_feat = avg_views_featuring/avg_views_not_featuring
print('Average views on videos with featuring : {0}\nAverage views on videos without featuring: {1}\nWith featuring / Without featuring ratio: {2}'.format(round(avg_views_featuring, 2), round(avg_views_not_featuring, 2), round(with_without_feat, 2)))

print("======================================================================================")

# NUMBER OF VIDEOS PUBLISHED IN SPECIFIC PART OF THE DAY
print("===================================Part of the day===================================")


query_0_8 = "SELECT count(video_id)\
             FROM CA_VIDEOS\
             WHERE hour >= 0 AND hour < 8"

query_9_16 = "SELECT count(video_id)\
             FROM CA_VIDEOS\
             WHERE hour >= 8 AND hour < 16"

query_17_0 = "SELECT count(video_id)\
             FROM CA_VIDEOS\
             WHERE hour >= 16"

sql_0_8 = spark.sql(query_0_8).collect()[0][0]
sql_9_16 = spark.sql(query_9_16).collect()[0][0]
sql_17_0 = spark.sql(query_17_0).collect()[0][0]

sum_24 = sql_0_8+sql_9_16+sql_17_0

percents_0_8 = sql_0_8/sum_24 * 100
percents_9_16 = sql_9_16/sum_24 * 100
percents_17_0 = sql_17_0/sum_24 * 100

print('Video published 00-08h : {0} ({3}%)\nVideo published 08-16h: {1} ({4}%)\nVideo published 16-00h: {2} ({5}%)'
.format(sql_0_8, sql_9_16, sql_17_0, round(percents_0_8,2), round(percents_9_16,2), round(percents_17_0,2)))

print("=====================================================================================")

# Views/likes ration depending on number of views
print("============================Views/Likes with views rising============================")

query_to_100k   = "SELECT * FROM CA_VIDEOS WHERE views <= 100000"
query_100k_1m   = "SELECT * FROM CA_VIDEOS WHERE views > 100000 AND views <= 1000000"
query_1m_2m     = "SELECT * FROM CA_VIDEOS WHERE views > 1000000 AND views <= 2000000"
query_2m_5m     = "SELECT * FROM CA_VIDEOS WHERE views > 2000000 AND views <= 5000000"
query_5m_10m    = "SELECT * FROM CA_VIDEOS WHERE views > 5000000 AND views <= 10000000"
query_10m_20m   = "SELECT * FROM CA_VIDEOS WHERE views > 10000000 AND views <= 20000000"
query_more_20m  = "SELECT * FROM CA_VIDEOS WHERE views > 20000000"

sql_to_100k = spark.sql(query_to_100k)
sql_100k_1m = spark.sql(query_100k_1m)
sql_1m_2m = spark.sql(query_1m_2m)
sql_2m_5m = spark.sql(query_2m_5m)
sql_5m_10m = spark.sql(query_5m_10m)
sql_10m_20m = spark.sql(query_10m_20m)
sql_more_20m = spark.sql(query_more_20m)

avg_views_to_100k = sql_to_100k.groupBy("video_id").avg("views").agg({'avg(views)': 'avg'}).collect()[0][0]
avg_likes_to_100k = sql_to_100k.groupBy("video_id").avg("likes").agg({'avg(likes)': 'avg'}).collect()[0][0]

avg_views_100k_1m = sql_100k_1m.groupBy("video_id").avg("views").agg({'avg(views)': 'avg'}).collect()[0][0]
avg_likes_100k_1m = sql_100k_1m.groupBy("video_id").avg("likes").agg({'avg(likes)': 'avg'}).collect()[0][0]

avg_views_1m_2m = sql_1m_2m.groupBy("video_id").avg("views").agg({'avg(views)': 'avg'}).collect()[0][0]
avg_likes_1m_2m = sql_1m_2m.groupBy("video_id").avg("likes").agg({'avg(likes)': 'avg'}).collect()[0][0]

avg_views_2m_5m = sql_2m_5m.groupBy("video_id").avg("views").agg({'avg(views)': 'avg'}).collect()[0][0]
avg_likes_2m_5m = sql_2m_5m.groupBy("video_id").avg("likes").agg({'avg(likes)': 'avg'}).collect()[0][0]

avg_views_5m_10m = sql_5m_10m.groupBy("video_id").avg("views").agg({'avg(views)': 'avg'}).collect()[0][0]
avg_likes_5m_10m = sql_5m_10m.groupBy("video_id").avg("likes").agg({'avg(likes)': 'avg'}).collect()[0][0]

avg_views_10m_20m = sql_10m_20m.groupBy("video_id").avg("views").agg({'avg(views)': 'avg'}).collect()[0][0]
avg_likes_10m_20m = sql_10m_20m.groupBy("video_id").avg("likes").agg({'avg(likes)': 'avg'}).collect()[0][0]

avg_views_more_20m = sql_more_20m.groupBy("video_id").avg("views").agg({'avg(views)': 'avg'}).collect()[0][0]
avg_likes_more_20m = sql_more_20m.groupBy("video_id").avg("likes").agg({'avg(likes)': 'avg'}).collect()[0][0]

likes_views_to_100k = avg_likes_to_100k / avg_views_to_100k * 100
likes_views_100k_1m = avg_likes_100k_1m / avg_views_100k_1m * 100
likes_views_1m_2m = avg_likes_1m_2m / avg_views_1m_2m * 100
likes_views_2m_5m = avg_likes_2m_5m / avg_views_2m_5m * 100
likes_views_5m_10m = avg_likes_5m_10m / avg_views_5m_10m * 100
likes_views_10m_20m = avg_likes_10m_20m / avg_views_10m_20m * 100
likes_views_more_20m = avg_likes_more_20m / avg_views_more_20m * 100

print('Videos with up to 100k likes/views ratio: {0}%'
.format(round(likes_views_to_100k, 2)))
print('Videos with 100k-1m likes/views ratio: {0}%'
.format(round(likes_views_100k_1m, 2)))
print('Videos with 1m-2m likes/views ratio: {0}%'
.format(round(likes_views_1m_2m, 2)))
print('Videos with 2m-5m likes/views ratio: {0}%'
.format(round(likes_views_2m_5m, 2)))
print('Videos with 5m-10m likes/views ratio: {0}%'
.format(round(likes_views_5m_10m, 2)))
print('Videos with 10m-20m likes/views ratio: {0}%'
.format(round(likes_views_10m_20m, 2)))
print('Videos with more then 20m likes/views ratio: {0}%'
.format(round(likes_views_more_20m, 2)))


print("=====================================================================================")


