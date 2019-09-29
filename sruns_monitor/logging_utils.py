# -*- coding: utf-8 -*-

###
# Nathaniel Watson
# nathanielwatson@stanfordhealthcare.org
# 2019-05-31
###

import logging
from logging.handlers import RotatingFileHandler
import os

import sruns_monitor as srm


FORMATTER = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s\t%(message)s')

def add_file_handler(logger, log_dir, level, tag):
    """
    Adds a ``logging.handlers.RotatingFileHandler`` handler to the specified ``logging`` instance
    that will log the messages it receives at the specified error level or greater.  The log file
    will be named as outlined in ``get_logfile_name``. The RotatingFileHandler is set to have
    a max size of 1MB.

    Args:
        logger: The `logging.Logger` instance to add the `RotatingFileHandler` to.
        log_dir: `str`. Directory in which to create the log file.
        level:  `int`. A logging level (i.e. given by one of the constants `logging.DEBUG`,
            `logging.INFO`, `logging.WARNING`, `logging.ERROR`, `logging.CRITICAL`).
        tag: `str`. A tag name to add to at the end of the log file name for clarity on the
            log file's purpose.
    """
    filename = get_logfile_name(log_dir=log_dir,tag=tag)
    logger.info("Creating log file {}".format(os.path.abspath(filename)))
    handler = RotatingFileHandler(filename=filename, mode="a", maxBytes=1000000000, backupCount=1)
    handler.setLevel(level)
    handler.setFormatter(FORMATTER)
    logger.addHandler(handler)

def get_logfile_name(log_dir, tag):
    """
    Creates a log file name that will reside in the directory specified by `log_dir`.  The file
    path will be '$log_dir/log_$TAG.txt', where $TAG is the value of the 'tag' parameter.

    Args:
        log_dir: `str`. Directory in which to create the log file.
        tag: `str`. A tag name to add to at the end of the log file name for clarity on the
            log file's purpose.
    """
    filename = "log_" + tag + ".txt"
    filename = os.path.join(log_dir, filename)
    return filename
