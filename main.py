from prefect import flow, task

from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_to_duckdb

@flow(name="Wine Ratings ETL")
def etl_pipeline():
    df_raw = extract_data("data/raw/wineratings.csv")
    df_transformed = transform_data(df_raw)
    load_to_duckdb(df_transformed)

if __name__ == "__main__":
    etl_pipeline()
