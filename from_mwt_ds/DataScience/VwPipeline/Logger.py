import logging
import time

from threading import Lock

class ConsoleLogger:
    def __init__(self, level: str = 'INFO'):
        self.level = logging.getLevelName(level)
        self.lock = Lock()

    def debug(self, message: str, job=None):
        if self.level <= logging.DEBUG:
            self.__trace__(message, job)

    def info(self, message: str, job=None):
        if self.level <= logging.INFO:
            self.__trace__(message, job)

    def warning(self, message: str, job=None):
        if self.level <= logging.WARNING:
            self.__trace__(message, job)

    def error(self, message: str, job=None):
        if self.level <= logging.ERROR:
            self.__trace__(message, job)

    def critical(self, message: str, job=None):
        if self.level <= logging.CRITICAL:
            self.__trace__(message, job)

    def __trace__(self, message: str, job: str):
        prefix = f'[{job or "-"}][{time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(time.time()))}] '
        self.lock.acquire()
        print(prefix + message)
        self.lock.release()

class __Logger__:
    def __init__(self, loggers: list):
        self.Loggers = loggers

    def debug(self, message: str, job=None):
        for l in self.Loggers:
            l.debug(message, job)

    def info(self, message: str, job=None):
        for l in self.Loggers:
            l.info(message, job)

    def warning(self, message: str,  job=None):
        for l in self.Loggers:
            l.warning(message, job)

    def error(self, message: str, job=None):
        for l in self.Loggers:
            l.error(message, job)

    def critical(self, message: str, job=None):
        for l in self.Loggers:
            l.critical(message, job)

class WidgetHandler:      
    def __init__(self, leave=False):
        self.Total = None
        self.Total = 0
        self.Tasks = 0
        self.Done = 0
        self.TimePerJob = 0
        self.Leave = leave

    def on_start(self, inputs, opts_in):
        from tqdm import tqdm_notebook as tqdm
        self.Jobs = {}
        self.Tasks = len(inputs)
        self.Total = tqdm(range(len(opts_in)), desc='Total', leave=self.Leave)

    def on_finish(self):
        self.Total.close()    

    def on_job_start(self, job):
        from tqdm import tqdm_notebook as tqdm
        self.Jobs[job.Name] = tqdm(range(self.Tasks), desc=job.Name, leave=self.Leave)

    def on_job_finish(self, job):
        self.Jobs[job.Name].close()
        self.Jobs.pop(job.Name)
        self.Total.update(1)
        self.Total.refresh()

    def on_task_start(self, job, task):
        pass

    def on_task_finish(self, job, task):
        self.Jobs[job.Name].update(1)
        self.Jobs[job.Name].refresh()

class __Handler__:
    def __init__(self, handlers):
        self.Handlers = handlers

    def on_start(self, inputs, opts_in):
        for h in self.Handlers:
            h.on_start(inputs, opts_in)

    def on_finish(self):
        for h in self.Handlers:
            h.on_finish()

    def on_job_start(self, job):
        for h in self.Handlers:
            h.on_job_start(job)

    def on_job_finish(self, job):
        for h in self.Handlers:
            h.on_job_finish(job)

    def on_task_start(self, job, task):
        for h in self.Handlers:
            h.on_task_start(job, task)

    def on_task_finish(self, job, task):
        for h in self.Handlers:
            h.on_task_finish(job, task)
