import logging
import time

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
        prefix = f'[{self.tag}][{time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(time.time()))}]'
        self.impl.trace(f'{prefix} {message}')


class ConsoleLoggerImpl:
    def __init__(self):
        self.lock = Lock()

    def trace(self, message: str):
        self.lock.acquire()
        print(message)
        self.lock.release()

class ConsoleLogger(__LoggerCore__):
    def __init__(self, level: str = 'INFO', tag=None, impl=ConsoleLoggerImpl()):
        self.LevelStr = level 
        super().__init__(impl, logging.getLevelName(self.LevelStr), tag)

    def __getitem__(self, key):
        return ConsoleLogger(self.LevelStr, key, self.impl)
    
    def trace(self, message: str):
        self.impl.trace(message)

#class FileLogger:
#    def __init__(self, path, thread_safe=True, level: str = 'INFO'):
#        super().__init__(level)
#        self.impl = open(path, 'w')
#        self.lock = Lock()

#    def __trace__(self, message: str, job: str):
#        prefix = f'[{job or "-"}][{time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(time.time()))}] '
#        if self.thread_safe:
#            self.lock.acquire()
#            print(prefix + message)
#            self.lock.release()
#        else:
#            print(prefix + message)

class __Logger__:
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
        return __Logger__([l[key] for l in self.Loggers])

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
