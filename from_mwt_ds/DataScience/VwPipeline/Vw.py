import subprocess
import json
import os
import pandas as pd
import enum

from VwPipeline.Pool import SeqPool, MultiThreadPool
from VwPipeline import VwOpts
from VwPipeline import Logger

import multiprocessing


def __safe_to_float__(num: str, default):
    try:
        return float(num)
    except (ValueError, TypeError):
        return default


# Helper function to extract example counters and metrics from VW output.
# Counter lines are preceeded by a single line containing the text:
#   loss     last          counter         weight    label  predict features
# and followed by a blank line
# Metric lines have the following form:
# metric_name = metric_value
def __extract_metrics__(out_lines):
    average_loss_dict = {}
    since_last_dict = {}
    metrics = {}
    try:
        record = False
        for line in out_lines:
            line = line.strip()
            if record:
                if line == '':
                    record = False
                else:
                    counter_line = line.split()
                    count, average_loss, since_last = counter_line[2], counter_line[0], counter_line[1]
                    average_loss_dict[count] = average_loss
                    since_last_dict[count] = since_last
            elif line.startswith('loss'):
                fields = line.split()
                if fields[0] == 'loss' and fields[1] == 'last' and fields[2] == 'counter':
                    record = True
            elif '=' in line:
                key_value = [p.strip() for p in line.split('=')]
                metrics[key_value[0]] = key_value[1]
    finally:
        return average_loss_dict, since_last_dict, metrics


def __parse_vw_output__(lines):
    average_loss, since_last, metrics = __extract_metrics__(lines)
    loss = None
    if 'average loss' in metrics:
        # Include the final loss as the primary metric
        loss = __safe_to_float__(metrics['average loss'], None)
    return {'loss_per_example': average_loss, 'since_last': since_last, 'metrics': metrics}, loss


def __metrics_table__(metrics, name):
    return pd.DataFrame([{'n': int(k), name: float(metrics[name][k])}
                         for k in metrics[name]]).set_index('n')


def metrics_table(metrics):
    return pd.concat([__metrics_table__(m, 'loss_per_example').join(__metrics_table__(m, 'since_last')).assign(file=i)
                      for i, m in enumerate(metrics)]).reset_index().set_index(['file', 'n'])


def final_metrics_table(metrics):
    return [m['metrics'] for m in metrics]


def __save__(txt, path):
    with open(path, 'w') as f:
        f.write(txt)


def __load__(path):
    with open(path, 'r') as f:
        return f.read()


class ExecutionStatus(enum.Enum):
    NotStarted = 1
    Running = 2
    Success = 3
    Failed = 4


class Task:
    def __init__(self, job, file, folder, model, model_folder = '', norun=False):
        self.Job = job
        self.File = file
        self.Folder = folder
        self.Logger = self.Job.Logger
        self.Status = ExecutionStatus.NotStarted
        self.Model = model
        self.ModelFolder = model_folder
        self.NoRun = norun
        self.Status = ExecutionStatus.NotStarted
        self.Loss = None
        self.Populated = {}
        self.Args = self.__prepare_args__(self.Job.Cache)

    def __prepare_args__(self, cache):
        opts = self.Job.OptsIn.copy()
        opts[self.Job.InputMode] = self.File
        
        file_full = os.path.join(self.Folder, self.File)

        salt = os.path.getsize(file_full)
        if self.Model:
            opts['-i'] = self.Model

        self.PopulatedRelative = {o: cache.get_rel_path(opts, o, salt) for o in self.Job.OptsOut}
        self.Populated = {o: cache.get_path(opts, o, salt, self.Logger) for o in self.Job.OptsOut}

        self.StdOutPath = cache.get_path(opts, salt, self.Logger)

        if self.Model:
            opts['-i'] = os.path.join(self.ModelFolder, self.Model)
            
        opts[self.Job.InputMode] = file_full
        opts = dict(opts, **self.Populated)
        return VwOpts.to_string(opts)

    def __run__(self):
        command = f'{self.Job.Path} {self.Args}'
        self.Logger.debug(f'Executing: {command}')
        process = subprocess.Popen(
            command.split(),
            universal_newlines=True,
            encoding='utf-8',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        error = process.communicate()[1]
        return error

    def run(self, reset):
        result_files = list(self.Populated.values()) + [self.StdOutPath]
        not_exist = next((p for p in result_files if not os.path.exists(p)), None)

        if reset or not_exist:
            if not_exist:
                self.Logger.debug(f'{not_exist} had not been found.')
            if self.NoRun:
                raise Exception('Result is not found, and execution is deprecated')

            result = self.__run__()
            __save__(result, self.StdOutPath)
        else:
            self.Logger.debug(f'Result of vw execution is found: {self.Args}')
        self.Result, self.Loss = __parse_vw_output__(self.stdout())
        self.Status = ExecutionStatus.Success if self.Loss else ExecutionStatus.Failed

    def stdout(self):
        return open(self.StdOutPath, 'r').readlines()

class Job:
    def __init__(self, path, cache, opts_in, opts_out, input_mode, handler, logger):
        self.Path = path
        self.Cache = cache
        self.Logger = logger
        self.OptsIn = opts_in
        self.Name = VwOpts.to_string({k: opts_in[k] for k in opts_in.keys() - {'#base'}})
        self.OptsOut = opts_out
        self.InputMode = input_mode
        self.Failed = None
        self.Handler = handler
        self.Status = ExecutionStatus.NotStarted
        self.Loss = None
        self.Populated = {o: [] for o in self.OptsOut}
        self.Metrics = []
        self.Tasks = []

    def run(self, reset):
        self.Handler.on_job_start(self)
        self.Status = ExecutionStatus.Running
        for t in self.Tasks:
            self.Handler.on_task_start(self, t)
            t.run(reset)
            self.Handler.on_task_finish(self, t)
            if t.Status == ExecutionStatus.Failed:
                self.Failed = t
                break
            for p in t.Populated:
                self.Populated[p].append(t.Populated[p])
            self.Metrics.append(t.Result)

        self.Status = self.Failed.Status if self.Failed else ExecutionStatus.Success
        self.Loss = self.Tasks[-1].Loss if len(self.Tasks) > 0 and self.Status == ExecutionStatus.Success else None
        self.Handler.on_job_finish(self)
        return self


class TestJob(Job):
    def __init__(self, path, cache, files, input_dir, opts_in, opts_out, input_mode, norun, handler, logger):
        super().__init__(path, cache, opts_in, opts_out, input_mode, handler, logger)
        for f in files:
            self.Tasks.append(Task(self, f, input_dir, None, cache.Path, norun))


class TrainJob(Job):
    def __init__(self, path, cache, files, input_dir, opts_in, opts_out, input_mode, norun, handler, logger):
        if '-f' not in opts_out:
            opts_out.append('-f')
        super().__init__(path, cache, opts_in, opts_out, input_mode, handler, logger)
        for i, f in enumerate(files):
            model = None if i == 0 else self.Tasks[i - 1].PopulatedRelative['-f']
            self.Tasks.append(Task(self, f, input_dir, model, cache.Path, norun))


class Vw:
    def __init__(self, path, cache, procs=multiprocessing.cpu_count(), norun=False, reset=False, handlers=[], loggers=[]):
        self.Path = path
        self.Cache = cache
        self.Logger = Logger.__Logger__(loggers)
        self.Pool = SeqPool() if procs == 1 else MultiThreadPool(procs)
        self.NoRun = norun
        self.Handler = Logger.__Handler__(handlers)
        self.Reset = reset

    def __with__(self, path=None, cache=None, procs=None, norun=None, reset=None, handler=None):
        return Vw(path or self.Path, cache or self.Cache, procs or self.Pool.Procs, 
            norun or self.NoRun, reset or self.Reset, handler or self.Handler)

    def __run_impl__(self, inputs, opts_in, opts_out, input_mode, input_dir, job_type):
        job = job_type(self.Path, self.Cache, inputs, input_dir, opts_in, opts_out, input_mode, self.NoRun, self.Handler, self.Logger)
        return job.run(self.Reset)

    def __run_on_dict__(self, inputs, opts_in, opts_out, input_mode, input_dir, job_type):
        if not isinstance(inputs, list):
            inputs = [inputs]
        self.Handler.on_start(inputs, opts_in)
        if isinstance(opts_in, list):
            args = [(inputs, point, opts_out, input_mode, input_dir, job_type) for point in opts_in]
            result = self.Pool.map(self.__run_impl__, args)
        else:
            result = self.__run_impl__(inputs, opts_in, opts_out, input_mode, input_dir, job_type)
        self.Handler.on_finish()
        return result        

    def __run__(self, inputs, opts_in, opts_out, input_mode, input_dir, job_type):
        if isinstance(opts_in, pd.DataFrame):
            opts_in = list(opts_in.loc[:, ~opts_in.columns.str.startswith('!')].to_dict('index').values())
            result = self.__run_on_dict__(inputs, opts_in, opts_out, input_mode, input_dir, job_type)
            result_pd = []
            for r in result:
                loss = r.Loss if r.Failed==None else None
                metrics = metrics_table(r.Metrics) if r.Metrics else None
                final_metrics = final_metrics_table(r.Metrics) if r.Metrics else None
                results = {'!Loss': loss, '!Populated': r.Populated,
                           '!Metrics': metrics,
                           '!FinalMetrics': final_metrics,
                           '!Job': r}
                result_pd.append(dict(r.OptsIn, **results))
            return pd.DataFrame(result_pd)
        else:
            return self.__run_on_dict__(inputs, opts_in, opts_out, input_mode, input_dir, job_type)

    def cache(self, inputs, opts, input_dir = ''):
        if isinstance(opts, list):
            cache_opts = [{'#cmd': o_dedup} for o_dedup in set([VwOpts.to_cache_cmd(o) for o in opts])]
        else:
            cache_opts = {'#cmd': VwOpts.to_cache_cmd(opts)}
        return self.__run__(inputs, cache_opts, ['--cache_file'], '-d', input_dir, TestJob)

    def train(self, inputs, opts_in, opts_out=[], input_mode='-d', input_dir=''):
        return self.__run__(inputs, opts_in, opts_out, input_mode, input_dir, TrainJob)

    def test(self, inputs, opts_in, opts_out=[], input_mode='-d', input_dir=''):
        return self.__run__(inputs, opts_in, opts_out, input_mode, input_dir, TestJob)
