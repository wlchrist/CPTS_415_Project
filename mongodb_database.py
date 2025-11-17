import pandas as pd
from pymongo import MongoClient
import json
from pathlib import Path

def connect_to_mongodb():
    """connect to local mongodb instance"""
    try:
        client = MongoClient('mongodb://localhost:27017')
        client.admin.command('ping')
        print("connected to mongodb successfully")
        return client
    except Exception as e:
        print(f"failed to connect to mongodb: {e}")
        print("make sure mongodb is running on localhost:27017")
        return None

def create_database_and_collections(client):
    """create database and collections for movie ratings"""
    db = client['movie_ratings_db']
    
    # create collections
    users_collection = db['users']
    movies_collection = db['movies']
    ratings_collection = db['ratings']
    
    print(f"created database: {db.name}")
    print(f"created collections: users, movies, ratings")
    
    return db, users_collection, movies_collection, ratings_collection

def load_csv_data():
    """load cleaned data from ratings.csv"""
    try:
        df = pd.read_csv('ratings.csv')
        print(f"loaded {len(df)} rows from ratings.csv")
        return df
    except Exception as e:
        print(f"failed to load ratings.csv: {e}")
        return None

def create_user_documents(df):
    """create user documents from csv data"""
    user_docs = []
    
    for user_id, user_data in df.groupby('userID'):
        user_doc = {
            'user_id': int(user_id),
            'total_ratings': len(user_data),
            'average_rating': float(user_data['rating'].mean()),
            'rating_history': []
        }
        
        # add individual ratings to history
        for _, row in user_data.iterrows():
            rating_entry = {
                'movie_id': int(row['movieID']),
                'title': str(row['title']),
                'genre': str(row['genre']),
                'rating': int(row['rating'])
            }
            user_doc['rating_history'].append(rating_entry)
        
        user_docs.append(user_doc)
    
    print(f"created {len(user_docs)} user documents")
    return user_docs

def create_movie_documents(df):
    """create movie documents from csv data"""
    movie_docs = []
    
    for movie_id, movie_data in df.groupby('movieID'):
        movie_doc = {
            'movie_id': int(movie_id),
            'title': str(movie_data['title'].iloc[0]),
            'genre': str(movie_data['genre'].iloc[0]),
            'total_ratings': len(movie_data),
            'average_rating': float(movie_data['rating'].mean()),
            'rating_distribution': {
                '1': int((movie_data['rating'] == 1).sum()),
                '2': int((movie_data['rating'] == 2).sum()),
                '3': int((movie_data['rating'] == 3).sum()),
                '4': int((movie_data['rating'] == 4).sum()),
                '5': int((movie_data['rating'] == 5).sum())
            }
        }
        movie_docs.append(movie_doc)
    
    print(f"created {len(movie_docs)} movie documents")
    return movie_docs

def create_rating_documents(df):
    """create individual rating documents from csv data"""
    rating_docs = []
    
    for _, row in df.iterrows():
        rating_doc = {
            'user_id': int(row['userID']),
            'movie_id': int(row['movieID']),
            'title': str(row['title']),
            'genre': str(row['genre']),
            'rating': int(row['rating'])
        }
        rating_docs.append(rating_doc)
    
    print(f"created {len(rating_docs)} rating documents")
    return rating_docs

def insert_documents_to_mongodb(users_collection, movies_collection, ratings_collection, 
                               user_docs, movie_docs, rating_docs):
    """insert all documents into mongodb collections"""
    
    # insert user documents
    if user_docs:
        users_result = users_collection.insert_many(user_docs)
        print(f"inserted {len(users_result.inserted_ids)} user documents")
    
    # insert movie documents
    if movie_docs:
        movies_result = movies_collection.insert_many(movie_docs)
        print(f"inserted {len(movies_result.inserted_ids)} movie documents")
    
    # insert rating documents
    if rating_docs:
        ratings_result = ratings_collection.insert_many(rating_docs)
        print(f"inserted {len(ratings_result.inserted_ids)} rating documents")

def create_indexes(users_collection, movies_collection, ratings_collection):
    """create indexes for better query performance"""
    
    # user collection indexes
    users_collection.create_index("user_id")
    users_collection.create_index("average_rating")
    
    # movie collection indexes
    movies_collection.create_index("movie_id", unique=True)
    movies_collection.create_index("genre")
    movies_collection.create_index("average_rating")
    
    # ratings collection indexes
    ratings_collection.create_index("user_id")
    ratings_collection.create_index("movie_id")
    ratings_collection.create_index("rating")
    
    print("created indexes for all collections")

def demonstrate_queries(users_collection, movies_collection, ratings_collection):
    """demonstrate various mongodb queries"""
    print("\ndatabase query examples:")
    print("=" * 30)
    
    # count total documents
    user_count = users_collection.count_documents({})
    movie_count = movies_collection.count_documents({})
    rating_count = ratings_collection.count_documents({})
    
    print(f"total users: {user_count}")
    print(f"total movies: {movie_count}")
    print(f"total ratings: {rating_count}")
    
    # find users with high average ratings
    high_rated_users = users_collection.count_documents({"average_rating": {"$gte": 4.0}})
    print(f"users with average rating >= 4.0: {high_rated_users}")
    
    # find most popular genre
    genre_pipeline = [
        {"$group": {"_id": "$genre", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]
    popular_genre = list(movies_collection.aggregate(genre_pipeline))
    if popular_genre:
        print(f"most popular genre: {popular_genre[0]['_id']} ({popular_genre[0]['count']} movies)")
    
    # find movies with highest ratings
    top_movies = movies_collection.find({"average_rating": {"$gte": 4.5}}).limit(5)
    print("top rated movies (rating >= 4.5):")
    for movie in top_movies:
        print(f"  {movie['title']} - {movie['average_rating']:.2f}")

def main():
    """main function to set up mongodb database"""
    print("mongodb database setup for movie ratings")
    print("=" * 40)
    
    # connect to mongodb
    client = connect_to_mongodb()
    if not client:
        return
    
    try:
        # create database and collections
        db, users_collection, movies_collection, ratings_collection = create_database_and_collections(client)
        
        # load csv data
        df = load_csv_data()
        if df is None:
            return
        
        # create document models
        user_docs = create_user_documents(df)
        movie_docs = create_movie_documents(df)
        rating_docs = create_rating_documents(df)
        
        # insert documents into mongodb
        insert_documents_to_mongodb(users_collection, movies_collection, ratings_collection,
                                   user_docs, movie_docs, rating_docs)
        
        # create indexes for performance
        create_indexes(users_collection, movies_collection, ratings_collection)
        
        # demonstrate queries
        demonstrate_queries(users_collection, movies_collection, ratings_collection)
        
        print("\nmongodb database setup completed successfully")
        print("connection string: mongodb://localhost:27017")
        print("database: movie_ratings_db")
        print("collections: users, movies, ratings")
        
    finally:
        # close connection
        client.close()
        print("mongodb connection closed")

if __name__ == "__main__":
    main()
