import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, explode, collect_list
import os
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MOVIES_RAW_DATA = os.path.join(BASE_DIR, '..', '..', 'data', 'raw', 'movies_popular.json')
SERIES_RAW_DATA = os.path.join(BASE_DIR, '..', '..', 'data', 'raw', 'tv_popular.json')
MOVIE_GENRES_DATA = os.path.join(BASE_DIR, '..', '..', 'data', 'raw', 'movieGenres.json')
TV_GENRES_DATA = os.path.join(BASE_DIR, '..', '..', 'data', 'raw', 'tvGenres.json')
OUTPUT_PATH = os.path.join(BASE_DIR, '..', '..', 'data', 'processed', 'combinedData.parquet')

spark = SparkSession.builder.appName("MoviesAPI") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

df_movies = spark.read.option("multiline", "true").json(MOVIES_RAW_DATA)
df_series = spark.read.option("multiline", "true").json(SERIES_RAW_DATA)

df_movie_genres = spark.read.option("multiline", "true").json(MOVIE_GENRES_DATA)
df_movie_genres_processed = df_movie_genres.select(explode(col("genres")).alias("genre")).select(col("genre.id").alias("genre_id"), col("genre.name").alias("genre_name"))

df_tv_genres = spark.read.option("multiline", "true").json(TV_GENRES_DATA)
df_tv_genres_processed = df_tv_genres.select(explode(col("genres")).alias("genre")).select(col("genre.id").alias("genre_id"), col("genre.name").alias("genre_name"))

df_movies_exploded = df_movies.select(
    "id", "title", "overview", "popularity", explode("genre_ids").alias("genre_id"),
    "vote_average", "vote_count", "backdrop_path", "poster_path", "original_language",
    col("release_date").alias("air_date")
).withColumn("content_type", lit("movie"))

df_series_exploded = df_series.select(
    "id", col("name").alias("title"), "overview", "popularity", explode("genre_ids").alias("genre_id"),
    "vote_average", "vote_count", "backdrop_path", "poster_path", "original_language",
    col("first_air_date").alias("air_date")
).withColumn("content_type", lit("tv_series"))

df_movies_with_genres = df_movies_exploded.join(df_movie_genres_processed, on="genre_id")
df_series_with_genres = df_series_exploded.join(df_tv_genres_processed, on="genre_id")

df_combined = df_movies_with_genres.unionByName(df_series_with_genres)

df_final = df_combined.groupBy(
    "id", "title", "overview", "popularity", "vote_average", "vote_count",
    "backdrop_path", "poster_path", "original_language", "air_date", "content_type"
).agg(
    collect_list("genre_name").alias("genres_string")
)

print("\nFinal DataFrame Schema:")
df_final.printSchema()

print("\nFinal DataFrame:")
df_final.show(5, truncate=False)

df_final.coalesce(1).write.mode("overwrite").parquet(OUTPUT_PATH)

print(f"\nProcessed data with genres successfully saved to '{OUTPUT_PATH}'")
spark.stop()