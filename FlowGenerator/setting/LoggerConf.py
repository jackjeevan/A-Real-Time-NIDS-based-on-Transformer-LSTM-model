# from email.policy import default
# import logging.config

MY_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default_formatter': {
            'format': '%(asctime)s %(name)s %(levelname)s %(message)s',
            'datefmt': '%d-%b-%y %H:%M:%S'
        },
    },
    'handlers': {
        'stream_handler': {
            'class': 'logging.StreamHandler',
            'formatter': 'default_formatter',
        },
        'file_handler': {
            'class': 'logging.FileHandler',
            'formatter': 'default_formatter',
            'filename':'mlridin.log'
        }
    },
    'loggers': {
        '': {
            'handlers': ['stream_handler','file_handler'],
            'level': 'INFO',
            'propagate': True
        }
    }
}

# logging.config.dictConfig(MY_LOGGING_CONFIG)
# logger = logging.getLogger('mylogger')
# logger.info('info log')