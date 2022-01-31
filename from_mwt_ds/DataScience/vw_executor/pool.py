import multiprocessing
from multiprocessing.pool import ThreadPool

from typing import Callable, List, Any
from abc import ABC, abstractmethod


def _execute(task_input):
    return task_input[0](*task_input[1])


class Pool(ABC):
    procs: int

    def __init__(self, procs):
        self.procs = procs

    @abstractmethod
    def map(self, task: Callable, inputs: List[Any]) -> Any:
        ...


class SeqPool(Pool):
    def __init__(self):
        super().__init__(1)
        
    def map(self, task: Callable, inputs: List[Any]) -> Any:
        result = []

        for i in inputs:
            result.append(task(*i))
        return result


class MultiThreadPool(Pool):
    def __init__(self, procs: int = multiprocessing.cpu_count()):
        super().__init__(procs)

    def map(self, task: Callable, inputs: List[Any]) -> Any:
        args = [(task, i) for i in inputs]
        with ThreadPool(processes=self.procs) as p:
            return p.map(_execute, args)
