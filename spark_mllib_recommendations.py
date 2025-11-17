"""
Spark MLlib Collaborative Filtering for Movie Recommendations
Uses Alternating Least Squares (ALS) algorithm for recommendation system.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import os

def create_spark_session(app_name="MovieRecommendations"):
    """Create and return a Spark session"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", "spark-warehouse") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_ratings_data(spark, file_path, max_users=None, max_movies=None):
    """Load and prepare ratings data for MLlib"""
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
    
    # Clean data
    df = df.filter(
        col("userID").isNotNull() &
        col("movieID").isNotNull() &
        col("rating").isNotNull() &
        (col("rating") >= 1) &
        (col("rating") <= 5)
    )
    
    # Reduce if specified
    if max_users:
        unique_users = df.select("userID").distinct().limit(max_users)
        user_list = [row.userID for row in unique_users.collect()]
        df = df.filter(col("userID").isin(user_list))
    
    if max_movies:
        unique_movies = df.select("movieID").distinct().limit(max_movies)
        movie_list = [row.movieID for row in unique_movies.collect()]
        df = df.filter(col("movieID").isin(movie_list))
    
    user_mapping = df.select("userID").distinct().rdd.zipWithIndex().collectAsMap()
    movie_mapping = df.select("movieID").distinct().rdd.zipWithIndex().collectAsMap()
    
    user_id_to_index = {user_id: idx for user_id, idx in user_mapping.items()}
    movie_id_to_index = {movie_id: idx for movie_id, idx in movie_mapping.items()}
    index_to_user_id = {idx: user_id for user_id, idx in user_id_to_index.items()}
    index_to_movie_id = {idx: movie_id for movie_id, idx in movie_id_to_index.items()}
    
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType as SparkIntegerType
    
    user_id_map = {k: int(v) for k, v in user_id_to_index.items()}
    movie_id_map = {k: int(v) for k, v in movie_id_to_index.items()}
    
    user_map_udf = udf(lambda x: user_id_map.get(x, -1), SparkIntegerType())
    movie_map_udf = udf(lambda x: movie_id_map.get(x, -1), SparkIntegerType())
    
    ratings_df = df.withColumn("user_index", user_map_udf(col("userID"))) \
                   .withColumn("movie_index", movie_map_udf(col("movieID"))) \
                   .filter((col("user_index") >= 0) & (col("movie_index") >= 0))
    
    ratings_for_als = ratings_df.select(
        col("user_index").alias("user_id"),
        col("movie_index").alias("movie_id"),
        col("rating").cast("float")
    )
    
    return ratings_for_als, index_to_user_id, index_to_movie_id, df

def train_als_model(ratings_df, rank=10, max_iter=10, reg_param=0.1):
    """
    Train ALS (Alternating Least Squares) model for collaborative filtering
    """
    print("\n=== Training ALS Model ===")
    print(f"Rank: {rank}, Max Iterations: {max_iter}, Regularization: {reg_param}")
    
    # Split data
    (training, test) = ratings_df.randomSplit([0.8, 0.2], seed=42)
    
    print(f"Training set: {training.count()} ratings")
    print(f"Test set: {test.count()} ratings")
    
    # Build ALS model
    als = ALS(
        maxIter=max_iter,
        rank=rank,
        regParam=reg_param,
        userCol="user_id",
        itemCol="movie_id",
        ratingCol="rating",
        coldStartStrategy="drop",
        implicitPrefs=False
    )
    
    print("Training model...")
    model = als.fit(training)
    
    # Evaluate model
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    
    rmse = evaluator.evaluate(predictions)
    print(f"\nModel RMSE: {rmse:.4f}")
    
    return model, rmse, predictions

def generate_recommendations(model, num_recommendations=10):
    """Generate top movie recommendations for all users"""
    print("\n=== Generating Recommendations ===")
    
    user_ids = model.userFactors.select("id").rdd.map(lambda x: x[0]).collect()
    sample_users = user_ids[:5]
    
    recommendations = model.recommendForUserSubset(
        spark.createDataFrame([(uid,) for uid in sample_users], ["user_id"]),
        num_recommendations
    )
    
    print(f"\nTop {num_recommendations} recommendations for sample users:")
    for row in recommendations.collect():
        print(f"\nUser {row.user_id}:")
        for rec in row.recommendations[:5]:
            print(f"  Movie {rec.movie_id}: predicted rating {rec.rating:.2f}")
    
    return recommendations

def get_top_movies(model, num_movies=10):
    """Get top rated movies overall"""
    print("\n=== Top Movies Overall ===")
    
    movie_ids = model.itemFactors.select("id").rdd.map(lambda x: x[0]).collect()
    top_movies = model.recommendForAllItems(num_movies)
    
    print(f"\nTop {num_movies} recommended movies:")
    sample = top_movies.limit(5).collect()
    for row in sample:
        for rec in row.recommendations[:3]:
            print(f"  Movie {rec.user_id}: score {rec.rating:.2f}")
    
    return top_movies

def main():
    """Main function for MLlib recommendations"""
    print("=" * 60)
    print("SPARK MLlib COLLABORATIVE FILTERING")
    print("=" * 60)
    
    spark = create_spark_session()
    
    try:
        if not os.path.exists("ratings.csv"):
            print("ERROR: ratings.csv not found. Please run process_imdb_data.py first.")
            return
        
        # Load and prepare data
        print("\n1. Loading and preparing data...")
        ratings_df, user_map, movie_map, original_df = load_ratings_data(
            spark, "ratings.csv", max_users=500, max_movies=1000
        )
        
        print(f"   Ratings loaded: {ratings_df.count()}")
        print(f"   Unique users: {ratings_df.select('user_id').distinct().count()}")
        print(f"   Unique movies: {ratings_df.select('movie_id').distinct().count()}")
        
        # Train model
        print("\n2. Training ALS model...")
        model, rmse, predictions = train_als_model(ratings_df, rank=10, max_iter=10)
        
        # Generate recommendations
        print("\n3. Generating recommendations...")
        recommendations = generate_recommendations(model, num_recommendations=10)
        
        # Get top movies
        print("\n4. Computing top movies...")
        top_movies = get_top_movies(model, num_movies=10)
        
        print("\n" + "=" * 60)
        print("MLlib RECOMMENDATIONS COMPLETE")
        print("=" * 60)
        print(f"\nModel Performance:")
        print(f"  RMSE: {rmse:.4f}")
        print(f"  Lower RMSE indicates better predictions")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

