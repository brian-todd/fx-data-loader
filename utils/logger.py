'''
Logging package configuration.
'''

import logging


def logger(fname=None, level=0):
    '''
    Get preset logger.

    Arguments
    ---------

    fname: str
        Path to logging file.

    level: int or logging.*
        Minimum log level.

    Returns
    -------

    LOG: logging.log
        Configured logger.
    '''

    logging.basicConfig(
        level=level,
        datefmt='%Y-%m-%d %H:%M:%S',
        format='[%(asctime)s:%(levelname)s:%(module)s:%(lineno)d] %(message)s',
        filename=fname
    )

    return logging.getLogger()
