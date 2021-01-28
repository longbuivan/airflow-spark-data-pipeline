import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, to_timestamp
from pyspark.sql.types import DoubleType

# Create spark session
spark = (SparkSession
    .builder
    .getOrCreate()
)

####################################
# Parameters
####################################
video_views = sys.argv[1]
videos = sys.argv[2]
postgres_db = sys.argv[3]
postgres_user = sys.argv[4]
postgres_pwd = sys.argv[5]

####################################
# Read CSV Data
####################################
print("######################################")
print("READING CSV FILES")
print("######################################")

df_video_views_csv = (
    spark.read
    .format("csv")
    .option("header", True)
    .load(video_views)
)

df_videos_json = (
    spark.read
    .format("json")
    .option("header", True)
    .load(videos)
)

####################################
# Load data to Postgres
####################################
print("######################################")
print("LOADING POSTGRES TABLES")
print("######################################")

(
    df_video_views_csv.write
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "public.video_views")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .mode("overwrite")
    .save()
)

(
     df_videos_json
     .write
     .format("jdbc")
     .option("url", postgres_db)
     .option("dbtable", "public.videos")
     .option("user", postgres_user)
     .option("password", postgres_pwd)
     .mode("overwrite")
     .save()
)