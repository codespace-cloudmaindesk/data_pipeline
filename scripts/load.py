import duckdb
import pandas as pd
from pathlib import Path

def load_to_duckdb(df: pd.DataFrame, db_path="warehouse/wine.duckdb"):
    print("Loading data into DuckDB...")
    Path("warehouse").mkdir(exist_ok=True)
    conn = duckdb.connect(db_path)
    conn.execute("CREATE TABLE IF NOT EXISTS wine_summary AS SELECT * FROM df")
    conn.close()
    print("Data loaded successfully")

if __name__ == "__main__":
    df = pd.read_csv("../data/processed/wine_summary.csv")
    load_to_duckdb(df)
