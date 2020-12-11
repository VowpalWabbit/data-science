import logging
import time

from threading import Lock


class ConsoleLogger:
    def __init__(self, level: str = 'INFO'):
        self.level = logging.getLevelName(level)
        self.lock = Lock()

    def debug(self, message: str, prefix: str = None):
        if self.level <= logging.DEBUG:
            self.__trace__(message, prefix)

    def info(self, message: str, prefix: str = None):
        if self.level <= logging.INFO:
            self.__trace__(message, prefix)

    def warning(self, message: str, prefix: str = None):
        if self.level <= logging.WARNING:
            self.__trace__(message, prefix)

    def error(self, message: str, prefix: str = None):
        if self.level <= logging.ERROR:
            self.__trace__(message, prefix)

    def critical(self, message: str, prefix: str = None):
        if self.level <= logging.CRITICAL:
            self.__trace__(message, prefix)

    def __trace__(self, message: str, prefix: str):
        prefix = f'{prefix}[{time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(time.time()))}] '
        self.lock.acquire()
        print(prefix + message)
        self.lock.release()


def debug(logger, message: str):
    if logger:
        logger.debug(message)


def info(logger, message: str):
    if logger:
        logger.info(message)


def warning(logger, message: str):
    if logger:
        logger.warning(message)


def error(logger, message: str):
    if logger:
        logger.error(message)


def critical(logger, message: str):
    if logger:
        logger.critical(message)
