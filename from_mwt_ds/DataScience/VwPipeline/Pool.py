import multiprocessing
from multiprocessing.pool import ThreadPool


def _execute(task_index_input):
    return task_index_input[1], task_index_input[0](*task_index_input[2])


class SeqPool:
    def __init__(self):
        self.Procs = 1
        
    def map(self, task, inputs):
        result = []
        for i in inputs:
            result.append(task(*i))
        return result


class MultiThreadPool:
    def __init__(self, procs=multiprocessing.cpu_count()):
        self.Procs = procs

    def map(self, task, inputs):
        p = ThreadPool(processes=self.Procs)
        args = [(task, index, i) for index, i in enumerate(inputs)]
        result = p.imap_unordered(_execute, args)
        p.close()
        p.join()
        outputs = [r[1] for r in sorted(result, key=lambda item: item[0])]
        return outputs
