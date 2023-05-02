from dataclasses import dataclass

from pyspark.sql import DataFrame


@dataclass
class DataFrameResult:
    variable_name: str
    dataframe: DataFrame


@dataclass
class TempViewResult:
    view_name: str
    dataframe: DataFrame


@dataclass
class SqlToViewResult:
    view_name: str
    sql_script: str


@dataclass
class SqlToDataframeResult:
    variable_name: str
    sql_script: str
