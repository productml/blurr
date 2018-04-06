import logging
from functools import wraps

import os
import sys
import threading

# Don't use this directly. Use _get_logger() instead.
_logger = None
_logger_lock = threading.Lock()


def _get_logger() -> logging.Logger:
    global _logger

    # Use double-checked locking to avoid taking lock unnecessarily.
    if _logger:
        return _logger

    _logger_lock.acquire()

    try:
        if _logger:
            return _logger

        # Scope the blurr logger to not conflict with users' loggers.
        logger = logging.getLogger('blurr')

        # Don't further configure the blurr logger if the root logger is
        # already configured. This prevents double logging in those cases.
        if not logging.getLogger().handlers:
            # Determine whether we are in an interactive environment
            _interactive = False
            try:
                # This is only defined in interactive shells.
                if sys.ps1:
                    _interactive = True
            except AttributeError:
                # Even now, we may be in an interactive shell with `python -i`.
                _interactive = sys.flags.interactive

            # If we are in an interactive environment (like Jupyter), set loglevel
            # to INFO and pipe the output to stdout.
            if _interactive:
                logger.setLevel(logging.INFO)
                _logging_target = sys.stdout
            else:
                _logging_target = sys.stderr

            # Add the output handler.
            _handler = logging.StreamHandler(_logging_target)
            _handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT, None))
            logger.addHandler(_handler)

        _logger = logger
        return _logger

    finally:
        _logger_lock.release()


def logging_decorator(logging_func):
    def wrapper(*args, **kwargs):
        logging_func(*args, **kwargs)

    return wrapper


@logging_decorator
def debug(msg, *args, **kwargs):
    _get_logger().debug(msg, *args, **kwargs)


@logging_decorator
def error(msg, *args, **kwargs):
    _get_logger().error(msg, *args, **kwargs)


@logging_decorator
def exception(msg, *args, **kwargs):
    _get_logger().exception(msg, *args, **kwargs)


@logging_decorator
def fatal(msg, *args, **kwargs):
    _get_logger().fatal(msg, *args, **kwargs)


@logging_decorator
def info(msg, *args, **kwargs):
    _get_logger().info(msg, *args, **kwargs)


@logging_decorator
def warning(msg, *args, **kwargs):
    _get_logger().warning(msg, *args, **kwargs)
