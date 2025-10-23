from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame


def find_all_taxis() -> DataFrame:
    print("samples.nyctaxi.trips")


def main():
    find_all_taxis()


if __name__ == "__main__":
    main()
