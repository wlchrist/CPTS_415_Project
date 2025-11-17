"""
Spark-based data processing using map/filter/reduce operations.
This module implements distributed data processing algorithms using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, sum as spark_sum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import os

def create_spark_session(app_name="MovieRatingsProcessing"):
    """Create and return a Spark session"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", "spark-warehouse") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_csv_to_spark(spark, file_path):
    """Load CSV file into Spark DataFrame"""
    schema = StructType([
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("genre", StringType(), True),
        StructField("rating", IntegerType(), True)
    ])
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(schema) \
        .csv(file_path)
    
    return df

def spark_reduce(df, max_users=None, max_movies=None):
    """
    Reduce dataset using Spark operations
    """
    if max_users:
        # Get unique users and limit
        unique_users = df.select("userID").distinct().limit(max_users)
        user_list = [row.userID for row in unique_users.collect()]
        df = df.filter(col("userID").isin(user_list))
    
    if max_movies:
        # Get unique movies and limit
        unique_movies = df.select("movieID").distinct().limit(max_movies)
        movie_list = [row.movieID for row in unique_movies.collect()]
        df = df.filter(col("movieID").isin(movie_list))
    
    return df

def spark_cleanse(df):
    """
    Cleanse data using Spark operations
    """
    # Remove null values using filter
    df = df.filter(
        col("userID").isNotNull() &
        col("movieID").isNotNull() &
        col("title").isNotNull() &
        col("genre").isNotNull() &
        col("rating").isNotNull()
    )
    
    # Filter valid ratings (0-5)
    df = df.filter((col("rating") >= 0) & (col("rating") <= 5))
    
    df = df.withColumn("genre", col("genre").trim())
    df = df.withColumn("rating", col("rating").cast(IntegerType()))
    
    return df

def spark_aggregate_statistics(df):
    """
    Aggregate statistics using Spark reduce operations
    """
    print("\n=== Spark Aggregation Statistics ===")
    
    # Count operations
    total_ratings = df.count()
    print(f"Total ratings: {total_ratings}")
    
    # Group by operations (reduce pattern)
    user_stats = df.groupBy("userID").agg(
        count("rating").alias("rating_count"),
        avg("rating").alias("avg_rating")
    )
    
    movie_stats = df.groupBy("movieID", "title", "genre").agg(
        count("rating").alias("rating_count"),
        avg("rating").alias("avg_rating")
    )
    
    genre_stats = df.groupBy("genre").agg(
        count("rating").alias("movie_count"),
        avg("rating").alias("avg_rating")
    )
    
    print(f"\nUnique users: {df.select('userID').distinct().count()}")
    print(f"Unique movies: {df.select('movieID').distinct().count()}")
    print(f"Unique genres: {df.select('genre').distinct().count()}")
    
    # Top genres by count
    print("\nTop 5 genres by movie count:")
    top_genres = genre_stats.orderBy(col("movie_count").desc()).limit(5)
    for row in top_genres.collect():
        print(f"  {row.genre}: {row.movie_count} movies, avg rating: {row.avg_rating:.2f}")
    
    # Rating distribution using reduce operations
    print("\nRating distribution:")
    rating_dist = df.groupBy("rating").agg(count("rating").alias("count"))
    for row in rating_dist.orderBy("rating").collect():
        print(f"  Rating {row.rating}: {row.count} ratings")
    
    return user_stats, movie_stats, genre_stats

def spark_map_filter_reduce_example(df):
    """
    Map/filter/reduce operations in Spark
    """
    print("\n=== Map/Filter/Reduce Operations ===")
    
    df_mapped = df.withColumn(
        "rating_category",
        when(col("rating") >= 4, "High")
        .when(col("rating") >= 3, "Medium")
        .otherwise("Low")
    )
    
    high_rated = df_mapped.filter(col("rating") >= 4)
    print(f"High-rated movies (>=4): {high_rated.count()} ratings")
    
    genre_rating_stats = df_mapped.groupBy("genre", "rating_category").agg(
        count("rating").alias("count"),
        avg("rating").alias("avg_rating")
    )
    
    print("\nGenre-Rating Category Statistics:")
    for row in genre_rating_stats.orderBy("genre", "rating_category").collect():
        print(f"  {row.genre} - {row.rating_category}: {row.count} ratings, avg: {row.avg_rating:.2f}")
    
    return df_mapped, genre_rating_stats

def save_to_parquet(df, output_path):
    """Save DataFrame to Parquet format"""
    df.write.mode("overwrite").parquet(output_path)
    print(f"\nData saved to Parquet format: {output_path}")

def main():
    """Main function to demonstrate Spark processing"""
    print("=" * 60)
    print("SPARK DATA PROCESSING")
    print("=" * 60)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Check if ratings.csv exists
        if not os.path.exists("ratings.csv"):
            print("ERROR: ratings.csv not found. Please run process_imdb_data.py first.")
            return
        
        # Load data
        print("\n1. Loading data into Spark...")
        df = load_csv_to_spark(spark, "ratings.csv")
        print(f"   Initial dataset: {df.count()} rows")
        
        # Reduce
        print("\n2. Applying Spark reduction (max_users=100, max_movies=500)...")
        df_reduced = spark_reduce(df, max_users=100, max_movies=500)
        print(f"   Reduced dataset: {df_reduced.count()} rows")
        
        # Cleanse
        print("\n3. Applying Spark cleansing...")
        df_cleansed = spark_cleanse(df_reduced)
        print(f"   Cleaned dataset: {df_cleansed.count()} rows")
        
        # Aggregate statistics
        print("\n4. Computing aggregations...")
        user_stats, movie_stats, genre_stats = spark_aggregate_statistics(df_cleansed)
        
        # Map/Filter/Reduce example
        print("\n5. Map/Filter/Reduce operations...")
        df_mapped, genre_rating_stats = spark_map_filter_reduce_example(df_cleansed)
        
        print("\n6. Saving to Parquet format...")
        os.makedirs("spark_output", exist_ok=True)
        save_to_parquet(df_cleansed, "spark_output/ratings_processed.parquet")
        
        print("\n" + "=" * 60)
        print("SPARK PROCESSING COMPLETE")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

