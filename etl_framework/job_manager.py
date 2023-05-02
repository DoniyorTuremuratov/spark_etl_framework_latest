import inspect
from abc import ABC, abstractmethod

from etl_framework.config.config import Config
from etl_framework.job.base_job import AbstractJob
from etl_framework.utils.logger_utils import get_logger
from etl_framework.utils.spark import create_spark_session
from etl_framework.utils.validation import is_str, is_iso_date_str, is_list_of_strings

log = get_logger()


class JobManager(ABC):
    MANDATORY_PARAMETERS = {
        "env": is_str,
        "actual_date": is_iso_date_str,
        "job_names": is_list_of_strings
    }

    def __init__(self):
        self._additional_params = {}

    def set_extra_param(self, key, value):
        self._additional_params[key] = value

    @staticmethod
    def check_mandatory_parameters(parameters):
        for parameter, validation_func in JobManager.MANDATORY_PARAMETERS.items():
            if parameter not in parameters:
                raise AttributeError(f"Parameter with name {parameter} not found")
            elif not validation_func(parameters[parameter]):
                raise ValueError(f"Invalid value is given to {parameter}: {parameters[parameter]}")

    @staticmethod
    def get_config() -> Config:
        return Config()

    @staticmethod
    def create_spark_session(parameters):
        spark_config = parameters.get('spark_config', {})
        log.info(f"Spark parameters: {spark_config}")
        return create_spark_session(spark_config=spark_config)

    @abstractmethod
    def get_jobs_package(self):
        pass

    def prepare_job_parameters(self, parameters, job_name):
        config = self.get_config()
        job_parameters = {
            "spark": JobManager.create_spark_session(parameters),
            "config": config,
            "actual_date": parameters["actual_date"],
            "job_name": job_name
        }
        for k, v in self._additional_params.items():
            job_parameters[k] = v
        log.info(f"The following parameters are provided to {job_name}: {list(job_parameters.keys())}")
        return job_parameters

    def get_job(self, job_name) -> type:
        for name, claz in inspect.getmembers(self.get_jobs_package()):
            if inspect.isclass(claz) and not inspect.isabstract(claz) and issubclass(claz, AbstractJob):
                if claz.is_right_job(claz=claz, job_name=job_name):
                    return claz
        raise ValueError(f"No candidate job found for '{job_name}'")

    def run(self, global_params: dict):
        self.check_mandatory_parameters(global_params)

        for parameter, value in global_params.items():
            log.info(f"Param {parameter}: {value}")

        for job_name in global_params['job_names']:
            job_parameters = self.prepare_job_parameters(global_params, job_name)
            job_class = self.get_job(job_name)
            if job_class():
                job_class().run(**job_parameters)
