import pandas as pd
from reduction import reduce
from cleansing import cleanse
from transformation import transform
import json

def parse(file, max_users=None, max_movies=None):
    dataset = pd.read_csv(file)
    dataset = reduce(dataset, max_users, max_movies)
    dataset = cleanse(dataset)
    documents  = transform(dataset)
    
    return documents

if __name__ == "__main__":
    file =  "ratings.csv"
    docs = parse(file, max_users=100, max_movies=500)

    with open("movie_documents.json", "w") as f:
        json.dump(docs, f, indent=2)
    print(docs)
