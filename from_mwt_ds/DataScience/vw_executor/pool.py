import multiprocessing
from multiprocessing.pool import ThreadPool


def _execute(task_input):
    return task_input[0](*task_input[1])


class SeqPool:
    def __init__(self):
        self.procs = 1
        
    def map(self, task, inputs):
        result = []
        for i in inputs:
            result.append(task(*i))
        return result


class MultiThreadPool:
    def __init__(self, procs=multiprocessing.cpu_count()):
        self.procs = procs

    def map(self, task, inputs):
        args = [(task, i) for i in inputs]
        with ThreadPool(processes=self.procs) as p:
            return p.map(_execute, args)
