import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MOVIES_RAW_DATA = os.path.join(BASE_DIR, '..', '..', 'data', 'raw', 'movies_popular.json')
SERIES_RAW_DATA = os.path.join(BASE_DIR, '..', '..', 'data', 'raw', 'tv_popular.json')
OUTPUT_PATH = os.path.join(BASE_DIR, '..', '..', 'data', 'processed', 'combinedData.parquet')

spark = SparkSession.builder.appName("MoviesAPI") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

df_movies = spark.read.option("multiline", "true").json(MOVIES_RAW_DATA)
df_series = spark.read.option("multiline", "true").json(SERIES_RAW_DATA)

df_movies_clean = df_movies.select(
    "id",
    "title",
    "overview",
    "popularity",
    "genre_ids",
    "vote_average",
    "vote_count",
    "backdrop_path",
    "poster_path",
    "original_language",
    col("release_date").alias("air_date")
).withColumn("content_type", lit("movie"))

df_series_clean = df_series.select(
    "id",
    col("name").alias("title"),
    "overview",
    "popularity",
    "genre_ids",
    "vote_average",
    "vote_count",
    "backdrop_path",
    "poster_path",
    "original_language",
    col("first_air_date").alias("air_date")
).withColumn("content_type", lit("tv_series"))

df_combined = df_movies_clean.unionByName(df_series_clean)

print("\nCombined dataset schema:\n:")
df_combined.printSchema()

print("\nCombined dataset content:\n")
df_combined.show(5)

df_combined.write.mode("overwrite").parquet(OUTPUT_PATH)

print(f"Processed data successfully saved to '{OUTPUT_PATH}'")