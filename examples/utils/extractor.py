from pyspark.sql import SparkSession

from etl_framework.config.config import Config


def extract_from_file(spark: SparkSession, config: Config, table_name: str):
    df = spark.read.parquet(f'{config.input_location_path}/{table_name}.parquet')
    return df
