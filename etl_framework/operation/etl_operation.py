from etl_framework.utils.logger_utils import get_logger

log = get_logger()


class ETLOperation:

    def __init__(self, function, operation_type: str):
        self.function = function
        self.operation_type = operation_type

    def __call__(self, *args, **kwargs):
        log.info(f"Calling {self.operation_type} function '{self.function.__name__}'")
        return self.function(*args, **kwargs)


def initializer(function):
    operation_type = 'initializer'
    return ETLOperation(function, operation_type)


def extractor(function):
    operation_type = 'extractor'
    return ETLOperation(function, operation_type)


def transformer(function):
    operation_type = 'transformer'
    return ETLOperation(function, operation_type)


def loader(function):
    operation_type = 'loader'
    return ETLOperation(function, operation_type)
