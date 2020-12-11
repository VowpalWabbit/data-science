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

class EmptyLogger:
    def __init__(self):
        pass

    def debug(self, message: str, prefix: str = None):
        pass

    def info(self, message: str, prefix: str = None):
        pass

    def warning(self, message: str, prefix: str = None):
        pass

    def error(self, message: str, prefix: str = None):
        pass

    def critical(self, message: str, prefix: str = None):
        pass

class WidgetHandler:
    class Info:
        def __init__(self):
            self
            
    def __init__(self):
        self.Logger = None
        self.Jobs = {}
        self.Total = 0
        self.Done = 0
        self.TimePerJob = 0

    def start(self, inputs, opts_in):
        pass

    def on_job_start(self, job):
        pass

    def on_job_finish(self, job):
        pass

    def on_task_start(self, job):
        pass

    def on_task_finish(self, job):
        pass 

class EmptyHandler:
    def __init__(self):
        pass

    def start(self, inputs, opts_in):
        pass

    def on_job_start(self, job):
        pass

    def on_job_finish(self, job):
        pass

    def on_task_start(self, job):
        pass

    def on_task_finish(self, job):
        pass 
