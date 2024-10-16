import polars as pl

FILE_PATH = "/opt/datasets/1brc/measurements.txt"

def start():
    df = pl.scan_csv(FILE_PATH, has_header=False, separator=";", new_columns=["city", "temperature"])
        .group_by("city")
        .agg(pl.min("temperature"), pl.mean("temperature"), pl.max("temperature"))
        .sort("city")
        .collect(streaming=True)

if __name__ == '__main__':
    start()
