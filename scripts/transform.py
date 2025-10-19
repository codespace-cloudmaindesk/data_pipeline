import pandas as pd

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    print("Cleaning and transforming data...")
    df["price_to_rating"] = df["points"] / df["price"]
    summary = (
        df.groupby(["country", "variety"])
        .agg(
            avg_points=("points", "mean"),
            avg_price=("price", "mean"),
            best_value=("price_to_rating", "max"),
            reviews=("title", "count"),
        )
        .reset_index()
    )
    return summary

if __name__ == "__main__":
    raw_df = pd.read_csv("../data/raw/wineratings.csv")
    transformed = transform_data(raw_df)
    transformed.to_csv("../data/processed/wine_summary.csv", index=False)
    print("Transformation complete")