from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Spark session
spark = SparkSession.builder \
    .appName("Test Workers") \
    .master("spark://spark:7077") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "1024m") \
    .getOrCreate()

# Verify connection
logger.info(f"Active Workers: {spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size()}")

# Create test data with enough partitions to distribute
test_data = range(1000)
rdd = spark.sparkContext.parallelize(test_data, 2)  # 2 partitions for 2 workers

# Run a simple computation
result = rdd.map(lambda x: x * 2).reduce(lambda a, b: a + b)
logger.info(f"Computation result: {result}")

# Show partition distribution
logger.info(f"Number of partitions: {rdd.getNumPartitions()}")
logger.info("Partition locations:")
logger.info(rdd.glom().map(len).collect())

spark.stop()
