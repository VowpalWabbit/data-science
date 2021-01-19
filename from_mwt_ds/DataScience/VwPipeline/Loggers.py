import logging
import time
import os

from threading import Lock


class _LoggerCore:
    def __init__(self, impl, level, tag):
        self.level = level
        self.impl = impl
        self.tag = tag

    def debug(self, message: str):
        if self.level <= logging.DEBUG:
            self._trace(message)

    def info(self, message: str):
        if self.level <= logging.INFO:
            self._trace(message)

    def warning(self, message: str):
        if self.level <= logging.WARNING:
            self._trace(message)

    def error(self, message: str):
        if self.level <= logging.ERROR:
            self._trace(message)

    def critical(self, message: str):
        if self.level <= logging.CRITICAL:
            self._trace(message)

    def _trace(self, message: str):
        prefix = f'[{self.tag or ""}][{time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(time.time()))}]'
        self.impl.trace(f'{prefix} {message}')


class ConsoleLoggerImpl:
    def __init__(self):
        self.lock = Lock()

    def trace(self, message: str):
        self.lock.acquire()
        print(message)
        self.lock.release()


class FileLoggerSafe:
    def __init__(self, path):
        self.lock = Lock()
        self.path = path

    def trace(self, message: str):
        self.lock.acquire()
        with open(self.path, 'a') as f:
            f.write(f'{message}\n')
        self.lock.release()


class FileLoggerUnsafe:
    def __init__(self, path):
        self.path = path

    def trace(self, message: str):
        with open(self.path, 'a') as f:
            f.write(f'{message}\n')


class ConsoleLogger(_LoggerCore):
    def __init__(self, level: str = 'INFO', tag=None, impl=ConsoleLoggerImpl()):
        self.level_str = level
        super().__init__(impl, logging.getLevelName(self.level_str), tag)

    def __getitem__(self, key):
        return ConsoleLogger(self.level_str, key, self.impl)
    
    def trace(self, message: str):
        self.impl.trace(message)


class FileLogger(_LoggerCore):
    def __init__(self, path=None, level: str = 'INFO', tag=None, impl=None):
        self.level_str = level
        if not impl:
            impl = FileLoggerSafe(path)
        super().__init__(impl, logging.getLevelName(self.level_str), tag)

    def __getitem__(self, key):
        return FileLogger(path=None, level=self.level_str, tag=key, impl=self.impl)
    
    def trace(self, message: str):
        self.impl.trace(message)


class MultiFileLogger(_LoggerCore):
    def __init__(self, folder=None, level: str = 'INFO', tag=None):
        self.level_str = level
        self.folder = folder
        os.makedirs(folder, exist_ok=True)
        impl = FileLoggerUnsafe(os.path.join(folder, f'{tag or "default"}.txt'))
        super().__init__(impl, logging.getLevelName(self.level_str), None)

    def __getitem__(self, key):
        return MultiFileLogger(folder=self.folder, level=self.LevelStr, tag=key)
    
    def trace(self, message: str):
        self.impl.trace(message)


class MultiLoggers:
    def __init__(self, loggers: list):
        self.loggers = loggers

    def debug(self, message: str):
        for logger in self.loggers:
            logger.debug(message)

    def info(self, message: str):
        for logger in self.loggers:
            logger.info(message)

    def warning(self, message: str):
        for logger in self.loggers:
            logger.warning(message)

    def error(self, message: str):
        for logger in self.loggers:
            logger.error(message)

    def critical(self, message: str):
        for logger in self.loggers:
            logger.critical(message)

    def __getitem__(self, key):
        return MultiLoggers([logger[key] for logger in self.loggers])
