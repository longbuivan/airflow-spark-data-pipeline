from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("Production Workload") \
        .master("spark://spark:7077") \
        .config("spark.executor.instances", "2") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.memory", "1024m") \
        .config("spark.default.parallelism", "2") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def monitor_execution(df, stage_name):
    logger.info(f"\n=== {stage_name} ===")
    logger.info(f"Partitions: {df.rdd.getNumPartitions()}")
    logger.info("Execution Plan:")
    df.explain()

def process_data(spark):
    # Verify workers
    worker_count = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size()
    logger.info(f"Processing with {worker_count} workers")

    # Create test data
    start_time = time.time()
    
    data = [(i, f"value_{i}", i % 5) for i in range(10000)]
    df = spark.createDataFrame(data, ["id", "value", "category"]) \
        .repartition(2)
    
    monitor_execution(df, "Initial Data Loading")

    # Complex processing
    result_df = df \
        .groupBy("category") \
        .agg(
            count("*").alias("count"),
            collect_list("value").alias("values"),
            avg("id").alias("avg_id")
        ) \
        .cache()

    monitor_execution(result_df, "After Processing")

    # Force computation
    result_df.show()
    
    end_time = time.time()
    logger.info(f"Total processing time: {end_time - start_time:.2f} seconds")

    return result_df

def main():
    spark = None
    try:
        spark = create_spark_session()
        process_data(spark)
    except Exception as e:
        logger.error(f"Error in processing: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
