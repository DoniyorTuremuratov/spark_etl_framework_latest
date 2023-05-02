from pyspark.sql import SparkSession


def create_spark_session(spark_config, app_name="demo_app_name"):
    spark_builder = SparkSession.builder.appName(app_name)

    for k, v in spark_config.items():
        spark_builder.config(k, v)

    spark_session = spark_builder.getOrCreate()
    return spark_session
