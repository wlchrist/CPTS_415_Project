import pandas as pd
import random
import os

def process_imdb_data():
    """
    Process IMDb Title Basics.tsv to extract movies to add to ratings.csv
    """

    print("Reading IMDb Title Basics.tsv")
    
    # around 10,000 movies per chunk
    chunk_size = 10000
    movie_chunks = []
    
    for chunk in pd.read_csv("IMDb Title Basics.tsv", sep='\t', chunksize=chunk_size):
        # only get movies
        movies_chunk = chunk[chunk['titleType'] == 'movie']
        movie_chunks.append(movies_chunk)
        print(f"Processed chunk with {len(movies_chunk)} movies...")
    

    print("Combining movie chunks")
    movies_df = pd.concat(movie_chunks, ignore_index=True)
    
    print(f"Total movies found: {len(movies_df)}")
    
    existing_df = pd.read_csv("ratings.csv")
    
    existing_movie_ids = set(existing_df['movieID'].tolist()) # add movie ids
    existing_user_ids = set(existing_df['userID'].tolist())
    
    num_movies_to_add = min(5000, len(movies_df))
    sampled_movies = movies_df.sample(n=num_movies_to_add, random_state=42)
    
    print(f"Adding ratings for {num_movies_to_add} movies...")
    
    new_rows = []
    movie_id_counter = max(existing_movie_ids) + 1 if existing_movie_ids else 1000
    user_id_counter = max(existing_user_ids) + 1 if existing_user_ids else 1000
    
    for _, movie in sampled_movies.iterrows():
        title = movie['primaryTitle']
        genres = movie['genres']
        
        if pd.isna(genres) or genres == '\\N': # take in first genre or unknown as placeholder genre
            genre = 'Unknown'
        else:
            genre = genres.split(',')[0].strip()
        
        num_ratings = random.randint(100, 300)  # fake ratings until we figure out how to get real ratings
        
        for _ in range(num_ratings):
            user_id = user_id_counter
            user_id_counter += 1
            
            rating_weights = [1, 2, 3, 4, 5]  # generate rating between 1 - 5
            user_rating = random.choices(range(1, 6), weights=rating_weights)[0]
            
            new_rows.append({
                'userID': int(user_id),
                'movieID': int(movie_id_counter),
                'title': str(title),
                'genre': str(genre),
                'rating': int(user_rating)
            })
        
        movie_id_counter += 1
    
    new_df = pd.DataFrame(new_rows)
    
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    
    combined_df = combined_df.sample(frac=1, random_state=42).reset_index(drop=True)
    
    # Save the exanded CSV with exact same format as original
    combined_df.to_csv("ratings.csv", index=False, sep=',', lineterminator='\n')
    
    file_size = os.path.getsize("ratings.csv")
    print(f"file size: {file_size / (1024*1024):.2f} MB")
    
    print(f"\nDataset Statistics:")
    print(f"Total rows: {len(combined_df)}")
    print(f"Unique users: {combined_df['userID'].nunique()}")
    print(f"Unique movies: {combined_df['movieID'].nunique()}")
    print(f"Unique genres: {combined_df['genre'].nunique()}")
    print(f"Average rating: {combined_df['rating'].mean():.2f}")
    
    return combined_df

if __name__ == "__main__":
    try:
        expanded_data = process_imdb_data()
        print(f"\nmake sure dataset is good::")
        print(expanded_data.tail(10))
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
