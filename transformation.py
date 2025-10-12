import pandas as pd

def transform(dataset):
    docs =  []
    for user_id, group in dataset.groupby('userID'):
        ratings = []
        for _, row in group.iterrows():
            ratings.append({
                "movieID": int(row['movieID']),
                "title": row['title'],
                "genre": row['genre'],
                "rating": float(row['rating'])
            })
        docs.append({
            "userID": int(user_id), 
            "ratings": ratings
        })
    return docs


if __name__ == "__main__":
    data = pd.read_csv("ratings.csv")
    from cleansing import cleanse
    data = cleanse (data)
    docs = transform(data)
    print(docs[:2])
