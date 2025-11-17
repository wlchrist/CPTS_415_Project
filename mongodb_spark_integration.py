"""
Integration between MongoDB and Spark
Reads data from MongoDB, processes in Spark, and writes results back
"""

from pyspark.sql import SparkSession
from pymongo import MongoClient
from pyspark.sql.functions import col, avg, count, desc
import pandas as pd

def create_spark_session(app_name="MongoDBSparkIntegration"):
    """Create Spark session with MongoDB connector"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", "spark-warehouse") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_from_mongodb_to_spark(spark, database="movie_ratings_db", collection="ratings"):
    """
    Read data from MongoDB into Spark DataFrame
    """
    print(f"\n=== Reading from MongoDB: {database}.{collection} ===")
    
    try:
        # Connect to MongoDB
        client = MongoClient('mongodb://localhost:27017')
        db = client[database]
        collection_obj = db[collection]
        
        cursor = collection_obj.find({})
        data = list(cursor)
        
        if not data:
            print("No data found in MongoDB collection")
            return None
        
        df_pandas = pd.DataFrame(data)
        
        if '_id' in df_pandas.columns:
            df_pandas = df_pandas.drop('_id', axis=1)
        
        df_spark = spark.createDataFrame(df_pandas)
        
        print(f"Loaded {df_spark.count()} records from MongoDB")
        print(f"Columns: {df_spark.columns}")
        
        client.close()
        return df_spark
        
    except Exception as e:
        print(f"Error reading from MongoDB: {e}")
        print("Make sure MongoDB is running and contains data")
        return None

def process_mongodb_data_in_spark(df):
    """Process MongoDB data using Spark operations"""
    print("\n=== Processing MongoDB Data in Spark ===")
    
    print("\nUser Statistics:")
    user_stats = df.groupBy("user_id").agg(
        count("rating").alias("rating_count"),
        avg("rating").alias("avg_rating")
    )
    
    top_users = user_stats.orderBy(desc("rating_count")).limit(5)
    print("Top 5 users by rating count:")
    for row in top_users.collect():
        print(f"  User {row.user_id}: {row.rating_count} ratings, avg: {row.avg_rating:.2f}")
    
    print("\nMovie Statistics:")
    movie_stats = df.groupBy("movie_id", "title", "genre").agg(
        count("rating").alias("rating_count"),
        avg("rating").alias("avg_rating")
    )
    
    top_movies = movie_stats.orderBy(desc("avg_rating")).limit(5)
    print("Top 5 movies by average rating:")
    for row in top_movies.collect():
        print(f"  {row.title} (ID: {row.movie_id}): {row.avg_rating:.2f} from {row.rating_count} ratings")
    
    print("\nGenre Statistics:")
    genre_stats = df.groupBy("genre").agg(
        count("rating").alias("rating_count"),
        avg("rating").alias("avg_rating")
    )
    
    top_genres = genre_stats.orderBy(desc("rating_count")).limit(5)
    print("Top 5 genres by rating count:")
    for row in top_genres.collect():
        print(f"  {row.genre}: {row.rating_count} ratings, avg: {row.avg_rating:.2f}")
    
    return user_stats, movie_stats, genre_stats

def write_spark_results_to_mongodb(df, database="movie_ratings_db", collection="spark_results"):
    """Write Spark processing results back to MongoDB"""
    print(f"\n=== Writing Spark Results to MongoDB: {database}.{collection} ===")
    
    try:
        df_pandas = df.toPandas()
        
        client = MongoClient('mongodb://localhost:27017')
        db = client[database]
        collection_obj = db[collection]
        
        collection_obj.delete_many({})
        
        records = df_pandas.to_dict('records')
        if records:
            collection_obj.insert_many(records)
            print(f"Inserted {len(records)} records into MongoDB")
        
        client.close()
        
    except Exception as e:
        print(f"Error writing to MongoDB: {e}")

def main():
    """Main function for MongoDB-Spark integration"""
    print("=" * 60)
    print("MONGODB-SPARK INTEGRATION")
    print("=" * 60)
    
    spark = create_spark_session()
    
    try:
        df = read_from_mongodb_to_spark(spark, "movie_ratings_db", "ratings")
        
        if df is None:
            print("\nCannot proceed without MongoDB data.")
            print("Please run mongodb_database.py first to populate MongoDB.")
            return
        
        user_stats, movie_stats, genre_stats = process_mongodb_data_in_spark(df)
        
        print("\nWriting aggregated results to MongoDB...")
        write_spark_results_to_mongodb(user_stats, "movie_ratings_db", "spark_user_stats")
        write_spark_results_to_mongodb(movie_stats, "movie_ratings_db", "spark_movie_stats")
        write_spark_results_to_mongodb(genre_stats, "movie_ratings_db", "spark_genre_stats")
        
        print("\n" + "=" * 60)
        print("MONGODB-SPARK INTEGRATION COMPLETE")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

