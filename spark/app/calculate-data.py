import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create spark session
spark = (SparkSession
    .builder
    .getOrCreate()
)

####################################
# Parameters
####################################
postgres_db = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]

####################################
# Read Postgres
####################################
print("######################################")
print("READING POSTGRES TABLES")
print("######################################")

df_video_views = (
    spark.read
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "public.video_views")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .load()
)

df_videos = (
    spark.read
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "public.videos")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .load()
)

####################################
# Enrichs data
####################################
df_video_views_csv = df_video_views.alias("v2")
df_videos_json = df_videos.alias("v")

df_join = df_video_views_csv.join(df_videos_json,
                                                    df_video_views_csv.video_id == df_videos_json._id['$oid'],
                                                    how='left').select(*)


####################################
# 1. Calculate view percentage of every view
####################################
spark.sql("SELECT ROUND(SUM(watch_time) / SUM(duration) * 100, 2) AS view_percentage FROM video_views").show()

print("######################################")
print("EXECUTING QUERY AND SAVING RESULTS")
print("######################################")
# Save result to a CSV file
df_result.coalesce(1).write.format("csv").mode("overwrite").save("/usr/local/spark/resources/data/output_postgres", header=True)

