import logging
from logging.config import dictConfig


def get_logger():
    logging_config = {
        'version': 1,
        'formatters': {
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            }
        },
        'handlers': {
            'default': {
                'level': logging.DEBUG,
                'class': 'logging.StreamHandler',
                'formatter': 'standard'
            }
        },
        'root': {
            'handlers': [
                'default'
            ],
            'level': logging.DEBUG
        }
    }
    dictConfig(logging_config)

    logger = logging.getLogger()
    return logger
