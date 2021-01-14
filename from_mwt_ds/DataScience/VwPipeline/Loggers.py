import logging
import time
import os

from threading import Lock

class __LoggerCore__:
    def __init__(self, impl, level, tag):
        self.level = level
        self.impl = impl
        self.tag = tag

    def debug(self, message: str):
        if self.level <= logging.DEBUG:
            self.__trace__(message)

    def info(self, message: str):
        if self.level <= logging.INFO:
            self.__trace__(message)

    def warning(self, message: str):
        if self.level <= logging.WARNING:
            self.__trace__(message)

    def error(self, message: str):
        if self.level <= logging.ERROR:
            self.__trace__(message)

    def critical(self, message: str):
        if self.level <= logging.CRITICAL:
            self.__trace__(message)

    def __trace__(self, message: str):
        prefix = f'[{self.tag or ""}][{time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(time.time()))}]'
        self.impl.trace(f'{prefix} {message}')


class __Loggers__:
    def __init__(self, loggers: list):
        self.Loggers = loggers

    def debug(self, message: str):
        for l in self.Loggers:
            l.debug(message)

    def info(self, message: str):
        for l in self.Loggers:
            l.info(message)

    def warning(self, message: str):
        for l in self.Loggers:
            l.warning(message)

    def error(self, message: str):
        for l in self.Loggers:
            l.error(message)

    def critical(self, message: str, job=None):
        for l in self.Loggers:
            l.critical(message)

    def __getitem__(self, key):
        return __Loggers__([l[key] for l in self.Loggers])


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

class ConsoleLogger(__LoggerCore__):
    def __init__(self, level: str = 'INFO', tag=None, impl=ConsoleLoggerImpl()):
        self.LevelStr = level 
        super().__init__(impl, logging.getLevelName(self.LevelStr), tag)

    def __getitem__(self, key):
        return ConsoleLogger(self.LevelStr, key, self.impl)
    
    def trace(self, message: str):
        self.impl.trace(message)

class FileLogger(__LoggerCore__):
    def __init__(self, path=None, level: str = 'INFO', tag=None, impl=None):
        self.LevelStr = level
        if not impl:
            impl = FileLoggerSafe(path)
        super().__init__(impl, logging.getLevelName(self.LevelStr), tag)

    def __getitem__(self, key):
        return FileLogger(path=None, level=self.LevelStr, tag=key, impl=self.impl)
    
    def trace(self, message: str):
        self.impl.trace(message)

class MultiFileLogger(__LoggerCore__):
    def __init__(self, folder=None, level: str = 'INFO', tag=None):
        self.LevelStr = level
        self.Folder = folder
        os.makedirs(folder, exist_ok=True)
        impl = FileLoggerUnsafe(os.path.join(folder, f'{tag or "default"}.txt'))
        super().__init__(impl, logging.getLevelName(self.LevelStr), None)

    def __getitem__(self, key):
        return MultiFileLogger(folder=self.Folder, level=self.LevelStr, tag=key)
    
    def trace(self, message: str):
        self.impl.trace(message)
