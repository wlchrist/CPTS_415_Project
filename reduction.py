import pandas as pd

def reduce(dataset,  max_users=None, max_movies=None):
    if max_users:
        dataset =  dataset[dataset['userID'].isin(dataset['userID'].unique()[:max_users])]
    if max_movies:
        dataset = dataset[dataset['movieID'].isin(dataset['movieID'].unique()[:max_movies])]
    return dataset

if __name__ == "__main__":
    data = pd.read_csv("ratings.csv")
    reduced = reduce (data, max_users=100, max_movies=500)
    print(reduced.head())
