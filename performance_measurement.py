import time
import psutil
import os
from pymongo import MongoClient
import statistics

def connect_to_mongodb():
    """connect to local mongodb instance"""
    try:
        client = MongoClient('mongodb://localhost:27017')
        client.admin.command('ping')
        print("connected to mongodb successfully")
        return client
    except Exception as e:
        print(f"failed to connect to mongodb: {e}")
        return None

def measure_query_performance(collection, query, query_name, iterations=10):
    """measure query execution time"""
    times = []
    
    for i in range(iterations):
        start_time = time.time()
        result = list(collection.find(query))
        end_time = time.time()
        execution_time = end_time - start_time
        times.append(execution_time)
    
    avg_time = statistics.mean(times)
    min_time = min(times)
    max_time = max(times)
    std_dev = statistics.stdev(times) if len(times) > 1 else 0
    
    print(f"{query_name}:")
    print(f"  average time: {avg_time:.4f} seconds")
    print(f"  min time: {min_time:.4f} seconds")
    print(f"  max time: {max_time:.4f} seconds")
    print(f"  std deviation: {std_dev:.4f} seconds")
    print(f"  results count: {len(result)}")
    print()
    
    return {
        'query_name': query_name,
        'avg_time': avg_time,
        'min_time': min_time,
        'max_time': max_time,
        'std_dev': std_dev,
        'result_count': len(result)
    }

def measure_aggregation_performance(collection, pipeline, query_name, iterations=10):
    """measure aggregation pipeline performance"""
    times = []
    
    for i in range(iterations):
        start_time = time.time()
        result = list(collection.aggregate(pipeline))
        end_time = time.time()
        execution_time = end_time - start_time
        times.append(execution_time)
    
    avg_time = statistics.mean(times)
    min_time = min(times)
    max_time = max(times)
    std_dev = statistics.stdev(times) if len(times) > 1 else 0
    
    print(f"{query_name}:")
    print(f"  average time: {avg_time:.4f} seconds")
    print(f"  min time: {min_time:.4f} seconds")
    print(f"  max time: {max_time:.4f} seconds")
    print(f"  std deviation: {std_dev:.4f} seconds")
    print(f"  results count: {len(result)}")
    print()
    
    return {
        'query_name': query_name,
        'avg_time': avg_time,
        'min_time': min_time,
        'max_time': max_time,
        'std_dev': std_dev,
        'result_count': len(result)
    }

def measure_count_performance(collection, query, query_name, iterations=10):
    """measure count query performance"""
    times = []
    
    for i in range(iterations):
        start_time = time.time()
        result = collection.count_documents(query)
        end_time = time.time()
        execution_time = end_time - start_time
        times.append(execution_time)
    
    avg_time = statistics.mean(times)
    min_time = min(times)
    max_time = max(times)
    std_dev = statistics.stdev(times) if len(times) > 1 else 0
    
    print(f"{query_name}:")
    print(f"  average time: {avg_time:.4f} seconds")
    print(f"  min time: {min_time:.4f} seconds")
    print(f"  max time: {max_time:.4f} seconds")
    print(f"  std deviation: {std_dev:.4f} seconds")
    print(f"  results count: {result}")
    print()
    
    return {
        'query_name': query_name,
        'avg_time': avg_time,
        'min_time': min_time,
        'max_time': max_time,
        'std_dev': std_dev,
        'result_count': result
    }

def get_system_resources():
    """get current system resource usage"""
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    
    print("system resource usage:")
    print(f"  cpu usage: {cpu_percent}%")
    print(f"  memory usage: {memory.percent}% ({memory.used / (1024**3):.2f} GB / {memory.total / (1024**3):.2f} GB)")
    print(f"  disk usage: {disk.percent}% ({disk.used / (1024**3):.2f} GB / {disk.total / (1024**3):.2f} GB)")
    print()

def get_database_stats(db):
    """get mongodb database statistics"""
    stats = db.command("dbStats")
    
    print("database statistics:")
    print(f"  collections: {stats['collections']}")
    print(f"  objects: {stats['objects']}")
    print(f"  data size: {stats['dataSize'] / (1024**2):.2f} MB")
    print(f"  storage size: {stats['storageSize'] / (1024**2):.2f} MB")
    print(f"  indexes: {stats['indexes']}")
    print(f"  index size: {stats['indexSize'] / (1024**2):.2f} MB")
    print()

def get_collection_stats(collection):
    """get collection statistics"""
    stats = collection.database.command("collStats", collection.name)
    
    print(f"{collection.name} collection statistics:")
    print(f"  documents: {stats['count']}")
    print(f"  size: {stats['size'] / (1024**2):.2f} MB")
    print(f"  storage size: {stats['storageSize'] / (1024**2):.2f} MB")
    print(f"  indexes: {stats['nindexes']}")
    print(f"  total index size: {stats['totalIndexSize'] / (1024**2):.2f} MB")
    print()

def measure_index_effectiveness(collection, query, query_name):
    """measure query performance with and without indexes"""
    print(f"measuring index effectiveness for: {query_name}")
    
    # get current indexes
    current_indexes = list(collection.list_indexes())
    print(f"current indexes: {len(current_indexes)}")
    
    # measure with indexes
    print("testing with indexes...")
    with_index_time = measure_query_performance(collection, query, f"{query_name} (with indexes)", 5)
    
    # drop all indexes except _id
    for index in current_indexes:
        if index['name'] != '_id_':
            try:
                collection.drop_index(index['name'])
            except:
                pass
    
    # measure without indexes
    print("testing without indexes...")
    without_index_time = measure_query_performance(collection, query, f"{query_name} (without indexes)", 5)
    
    # recreate indexes
    for index in current_indexes:
        if index['name'] != '_id_':
            try:
                collection.create_index(index['key'], **{k: v for k, v in index.items() if k not in ['key', 'name', 'v']})
            except:
                pass
    
    # calculate improvement
    improvement = ((without_index_time['avg_time'] - with_index_time['avg_time']) / without_index_time['avg_time']) * 100
    
    print(f"index effectiveness:")
    print(f"  with indexes: {with_index_time['avg_time']:.4f} seconds")
    print(f"  without indexes: {without_index_time['avg_time']:.4f} seconds")
    print(f"  improvement: {improvement:.2f}%")
    print()

def main():
    """main performance measurement function"""
    print("mongodb performance measurement")
    print("=" * 40)
    
    # connect to mongodb
    client = connect_to_mongodb()
    if not client:
        return
    
    try:
        # get database and collections
        db = client['movie_ratings_db']
        users_collection = db['users']
        movies_collection = db['movies']
        ratings_collection = db['ratings']
        
        # get system resources
        get_system_resources()
        
        # get database statistics
        get_database_stats(db)
        
        # get collection statistics
        get_collection_stats(users_collection)
        get_collection_stats(movies_collection)
        get_collection_stats(ratings_collection)
        
        print("query performance measurements:")
        print("=" * 30)
        
        # measure basic queries
        measure_count_performance(users_collection, {}, "count all users")
        measure_count_performance(movies_collection, {}, "count all movies")
        measure_count_performance(ratings_collection, {}, "count all ratings")
        
        # measure filter queries
        measure_query_performance(users_collection, {"average_rating": {"$gte": 4.0}}, "find high-rated users")
        measure_query_performance(movies_collection, {"genre": "Drama"}, "find drama movies")
        measure_query_performance(ratings_collection, {"rating": 5}, "find 5-star ratings")
        
        # measure aggregation queries
        genre_pipeline = [
            {"$group": {"_id": "$genre", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5}
        ]
        measure_aggregation_performance(movies_collection, genre_pipeline, "top 5 genres by count")
        
        rating_pipeline = [
            {"$group": {"_id": "$rating", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        measure_aggregation_performance(ratings_collection, rating_pipeline, "rating distribution")
        
        # measure index effectiveness
        measure_index_effectiveness(users_collection, {"average_rating": {"$gte": 4.0}}, "high-rated users query")
        measure_index_effectiveness(movies_collection, {"genre": "Drama"}, "drama movies query")
        
        print("performance measurement completed")
        
    finally:
        client.close()
        print("mongodb connection closed")

if __name__ == "__main__":
    main()
