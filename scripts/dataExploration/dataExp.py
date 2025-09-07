import pyspark
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import col

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, '..', '..', 'data', 'processed', 'combinedData.parquet')

spark = SparkSession.builder.appName("DataExploration") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

print("\nReading Parquet files\n")
df_combined = spark.read.parquet(PROCESSED_DATA_PATH)

print("\nDataFrame Schema:")
df_combined.printSchema()

print("\nFirst 5 rows:")
df_combined.show(5)

# Number of movies and TV series
print("\nContent type counts:")
df_combined.groupBy("content_type").count().show()

# Most popular movie
print("\nMost popular movie:")
df_combined.filter(col("content_type") == "movie") \
    .orderBy(col("popularity").desc()) \
    .show(1, truncate=False)

# Most popular TV Show
print("\nTop rated TV series:")
df_combined.filter(col("content_type") == "tv_series") \
    .orderBy(col("vote_average").desc()) \
    .show(1, truncate=False)

spark.stop()