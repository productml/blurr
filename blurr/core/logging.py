import logging

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
            interactive = False
            try:
                # This is only defined in interactive shells.
                if sys.ps1:
                    interactive = True
            except AttributeError:
                # Even now, we may be in an interactive shell with `python -i`.
                interactive = sys.flags.interactive

            # If we are in an interactive environment (like Jupyter), set loglevel
            # to INFO and pipe the output to stdout.
            if interactive:
                logger.setLevel(logging.INFO)
                _logging_target = sys.stdout
            else:
                _logging_target = sys.stderr

            # Add the output handler.
            handler = logging.StreamHandler(_logging_target)
            handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT, None))
            logger.addHandler(handler)

        _logger = logger
        return _logger

    finally:
        _logger_lock.release()


def debug(msg, *args, **kwargs):
    _get_logger().debug(msg, *args, **kwargs)


def error(msg, *args, **kwargs):
    _get_logger().error(msg, *args, **kwargs)


def exception(msg, *args, **kwargs):
    _get_logger().exception(msg, *args, **kwargs)


def fatal(msg, *args, **kwargs):
    _get_logger().fatal(msg, *args, **kwargs)


def info(msg, *args, **kwargs):
    _get_logger().info(msg, *args, **kwargs)


def warning(msg, *args, **kwargs):
    _get_logger().warning(msg, *args, **kwargs)


def is_debug_enabled():
    _get_logger().isEnabledFor(logging.DEBUG)
