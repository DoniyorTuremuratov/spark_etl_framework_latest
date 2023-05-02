import inspect
import types
from abc import abstractmethod

from pyspark.sql import SparkSession

from etl_framework.config.config import Config
from etl_framework.job.base_job import AbstractJob, JobContext
from etl_framework.operation.etl_operation import ETLOperation
from etl_framework.operation.operation_result import DataFrameResult, TempViewResult, SqlToViewResult, \
    SqlToDataframeResult
from etl_framework.utils.logger_utils import get_logger

log = get_logger()


class ETLJob(AbstractJob):

    def __init__(self):
        self.context = None
        self._operations = None

    def set_context(self, job_context: JobContext):
        self.context = job_context

    @property
    def operations(self):
        if self._operations is None:
            operations = self.get_operations()
            if not operations:
                raise ValueError("No operations found, please return operations on 'get_operations' method")
            else:
                self._operations = operations
                return self._operations
        else:
            return self._operations

    @abstractmethod
    def get_operations(self):
        pass

    def create_operation_parameters(self, operation):
        inspection = inspect.getfullargspec(operation.function)
        args = inspection.args
        defaults = {}
        if inspection.defaults:
            defaults = {arg: default_value for arg, default_value in zip(reversed(args), reversed(inspection.defaults))}
        parameters = {}
        for arg in args:
            if arg == "context":
                parameters[arg] = self.context
            elif arg in self.context:
                parameters[arg] = self.context[arg]
            elif arg in defaults:
                parameters[arg] = defaults[arg]
            elif hasattr(self.context, arg):
                parameters[arg] = getattr(self.context, arg)
            else:
                return f"No parameter '{arg}' found on context and no default is given", None
        return None, parameters

    def add_operation_result_to_context(self, operation_result, operation):
        function_name = operation.__name__
        if operation_result is not None:
            if isinstance(operation_result, types.GeneratorType):
                for element in operation_result:
                    if type(element) == DataFrameResult:
                        log.info(f"Setting dataframe: {element.dataframe}")
                        self.context[element.variable_name] = element.dataframe
                    elif type(element) == TempViewResult:
                        element.dataframe.createOrReplaceTempView(element.view_name)
                        log.info(f"Registering view: {element.view_name}")
                    elif type(element) == SqlToViewResult:
                        log.info(f"Executing sql at '{function_name}' and registering as {element.view_name}")
                        log.info(element.sql_script)
                        self.context.spark.sql(element.sql_script).createOrReplaceTempView(element.view_name)
                    elif type(element) == SqlToDataframeResult:
                        log.info(f"Executing sql at '{function_name}'")
                        log.info(element.sql_script)
                        self.context[element.variable_name] = self.context.spark.sql(element.sql_script)
                    elif type(element) is tuple and len(element) == 2:
                        name, value = element
                        self.context[name] = value
                        log.info(f"Setting context variable {name}: {value}")
            else:
                log.warning("Operation result is not generator, try to use 'yield' instead of 'return'")

    def execute(self, operations):
        for operation in operations:
            func_name = operation.function.__name__
            err, parameters = self.create_operation_parameters(operation)
            if err:
                raise ValueError(f"Error on function {func_name}: {err}")
            log.info(f"Parameters set for function: {func_name}: {parameters}")
            operation.context = self.context
            result = operation.__call__(**parameters)
            self.add_operation_result_to_context(result, operation.function)

    def validate_operations(self):
        for operation in self.operations:
            if not isinstance(operation, ETLOperation):
                return "No valid operations are given"

    def run(self, spark: SparkSession, config: Config, job_name: str, actual_date: str, **kwargs):
        log.info("Starting application...")
        self.set_context(JobContext(spark, config, job_name, actual_date, **kwargs))
        err = self.validate_operations()
        if err:
            raise TypeError(err)
        self.execute(self.operations)
