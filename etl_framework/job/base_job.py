import inspect
import os
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession

from etl_framework.config.config import Config


class JobContext:
    def __init__(self, spark: SparkSession, config: Config, job_name: str, actual_date: str, **kwargs):
        self.spark = spark
        self.config = config
        self.actual_date = actual_date
        self.job_name = job_name
        self._context_kwargs = {**kwargs}

    def __getitem__(self, item):
        return self._context_kwargs.get(item)

    def __setitem__(self, key, value):
        self._context_kwargs[key] = value

    def __contains__(self, item):
        return item in self._context_kwargs


class AbstractJob(ABC):

    @staticmethod
    def is_right_job(claz: type, job_name: str) -> bool:
        filename = os.path.basename(inspect.getfile(claz))[:-3]
        return job_name.lower() == filename

    @abstractmethod
    def run(self, spark: SparkSession, config: Config, job_name: str, actual_date: str, **kwargs):
        pass
