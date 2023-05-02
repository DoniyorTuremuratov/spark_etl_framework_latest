from pyspark.sql import SparkSession, DataFrame

from etl_framework.config.config import Config
from etl_framework.job.etl_job import ETLJob
from etl_framework.operation.etl_operation import initializer, transformer, extractor, loader
from etl_framework.operation.operation_result import SqlToViewResult, TempViewResult, SqlToDataframeResult
from examples.utils.extractor import extract_from_file


@initializer
def initialize_job_params():
    yield 'table_name', 'top_movies'


@extractor
def extract_movies(spark: SparkSession, config: Config):
    table_name = 'movies'
    yield TempViewResult(view_name=table_name, dataframe=extract_from_file(spark, config, table_name))


@extractor
def extract_ratings(spark: SparkSession, config: Config):
    table_name = 'ratings'
    yield TempViewResult(view_name=table_name, dataframe=extract_from_file(spark, config, table_name))


@transformer
def transform_aggregated_movies():
    transform_query = """
        SELECT
            movieId,
            round(avg(rating), 2) AS avg_rating,
            count(rating) AS ratings_count
        FROM ratings
        GROUP BY movieId
    """
    yield SqlToViewResult(view_name='aggregated_movies', sql_script=transform_query)


@transformer
def transform_top_movies(table_name: str):
    transform_query = """
        SELECT
            m.title,
            a.avg_rating,
            a.ratings_count
        FROM aggregated_movies a
        LEFT JOIN movies m ON a.movieId = m.movieId
        WHERE a.ratings_count >= 100
        ORDER BY a.avg_rating DESC
    """
    yield SqlToDataframeResult(variable_name=table_name, sql_script=transform_query)


@loader
def load_final_df(config: Config, table_name: str, top_movies: DataFrame, mode: str = 'append'):
    top_movies.write.mode(mode).parquet(f"{config.output_location_path}/{table_name}")


class DemoJob(ETLJob):

    def get_operations(self):
        return [
            initialize_job_params,
            extract_movies,
            extract_ratings,
            transform_aggregated_movies,
            transform_top_movies,
            load_final_df,
        ]
