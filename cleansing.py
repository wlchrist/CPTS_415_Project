import pandas as pd

def cleanse(dataset):
    dataset = dataset.dropna(subset=['userID', 'movieID', 'title', 'genre', 'rating'])
    dataset = dataset[dataset['rating'].between(0, 5)]
    dataset['genre'] = dataset['genre'].str.strip()
    dataset['rating'] = dataset['rating'].astype(int)
    
    return dataset


if __name__ == "__main__":
    data = pd.read_csv("ratings.csv")
    cleansed = cleanse(data)
    print(cleansed.head())