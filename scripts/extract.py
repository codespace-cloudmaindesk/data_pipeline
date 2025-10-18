import pandas as pd
from pathlib import Path

def extract_data(path:str) -> pd.DataFrame:
    print("Extract data from CSV...")

    df = pd.read_csv(path)
    df = df.dropna(subset=["country", "points", "price"])
    df["price"] = df["price"].astype(float)
    df["points"] = df["points"].astype(float)

    return df

if __name__ == "__main__":
    df = extract_data("../data/raw/wineratings.csv")
    print(df.head())