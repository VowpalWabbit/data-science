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
    return average_loss_dict, since_last_dict, metrics


def __parse_vw_output__(txt):
    average_loss, since_last, metrics = __extract_metrics__(txt.split('\n'))
    loss = None
    if 'average loss' in metrics:
        # Include the final loss as the primary metric
        loss = __safe_to_float__(metrics['average loss'], None)

    success = loss is not None
    return {'loss_per_example': average_loss, 'since_last': since_last, 'metrics': metrics, 'loss': loss}, success


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


class VwResult:
    def __init__(self, count):
        self.Loss = None
        self.Populated = []
        self.Metrics = []
        self.Status = ExecutionStatus.NotStarted


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
        self.Result = {}
        self.Args = self.__prepare_args__(self.Job.Cache)

    def __prepare_args__(self, cache):
        salt = None
        opts = self.Job.OptsIn.copy()
        opts[self.Job.InputMode] = self.File
        if self.Model:
            opts['-i'] = self.Model

        self.PopulatedRelative = {o: cache.get_rel_path(opts, o, salt) for o in self.Job.OptsOut}
        self.Populated = {o: cache.get_path(opts, o, salt) for o in self.Job.OptsOut}

        self.MetricsPath = cache.get_path(opts, salt)

        if self.Model:
            opts['-i'] = os.path.join(self.ModelFolder, self.Model)
            
        opts[self.Job.InputMode] = os.path.join(self.Folder, self.File)
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
        result_files = list(self.Populated.values()) + [self.MetricsPath]
        not_exist = next((p for p in result_files if not os.path.exists(p)), None)

        if reset or not_exist:
            if not_exist:
                self.Logger.debug(f'{not_exist} had not been found.')
            if self.NoRun:
                raise Exception('Result is not found, and execution is deprecated')

            result = self.__run__()
            __save__(result, self.MetricsPath)
        else:
            self.Logger.debug(f'Result of vw execution is found: {self.Args}')
        raw_result = __load__(self.MetricsPath)
        self.Logger.debug(raw_result)
        self.Result, success = __parse_vw_output__(raw_result)
        self.Status = ExecutionStatus.Success if success else ExecutionStatus.Failed
#        if not success:
#            Logger.critical(self.Logger, f'ERROR: {json.dumps(opts)}')
#            Logger.critical(self.Logger, raw_result)
#            raise Exception('Unsuccesful vw execution')
#        return parsed, populated



    def stdout(self):
        return open(self.MetricsPath, 'r').readlines()

class Job:
    def __init__(self, path, cache, opts_in, opts_out, input_mode, handler):
        self.Path = path
        self.Cache = cache
        self.Logger = cache.Logger
        self.OptsIn = opts_in
        self.OptsOut = opts_out
        self.InputMode = input_mode
        self.Failed = None
        self.Handler = handler

    def run(self, reset):
        self.Handler.on_job_start(self)
        self.Result.Status = ExecutionStatus.Running
        for index, t in enumerate(self.Tasks):
            self.Handler.on_task_start(self)
            t.run(reset)
            self.Handler.on_task_finish(self)
            if t.Status == ExecutionStatus.Failed:
                self.Result.Status = ExecutionStatus.Failed
                self.Handler.on_job_finish(self)
                self.Failed = t
                return self
            self.Result.Populated.append(t.Populated)
            self.Result.Metrics.append(t.Result)
            self.Result.Loss = t.Result['loss']
        self.Result.Status = ExecutionStatus.Success
        self.Handler.on_job_finish(self)
        return self


class TestJob(Job):
    def __init__(self, path, cache, files, input_dir, opts_in, opts_out, input_mode, norun, handler):
        super().__init__(path, cache, opts_in, opts_out, input_mode, handler)
        self.Tasks = []
        self.Name = VwOpts.to_string({k: opts_in[k] for k in opts_in.keys() - {'#base'}})
        for f in files:
            self.Tasks.append(Task(self, f, input_dir, None, cache.Path, norun))
        self.Result = VwResult(len(files))


class TrainJob(Job):
    def __init__(self, path, cache, files, input_dir, opts_in, opts_out, input_mode, norun, handler):
        super().__init__(path, cache, opts_in, opts_out, input_mode, handler)
        self.Tasks = []
        if '-f' not in opts_out:
            opts_out.append('-f')
        self.Name = VwOpts.to_string({k: opts_in[k] for k in opts_in.keys() - {'#base'}})
        for i, f in enumerate(files):
            model = None if i == 0 else self.Tasks[i - 1].PopulatedRelative['-f']
            self.Tasks.append(Task(self, f, input_dir, model, cache.Path, norun))
        self.Result = VwResult(len(files))


class Vw:
    def __init__(self, path, cache, procs=multiprocessing.cpu_count(), norun=False, reset=False, handler=Logger.EmptyHandler()):
        self.Path = path
        self.Cache = cache
        self.Logger = self.Cache.Logger
        self.Pool = SeqPool() if procs == 1 else MultiThreadPool(procs)
        self.NoRun = norun
        self.Handler = handler
        self.Reset = reset

    def __with__(self, path=None, cache=None, procs=None, norun=None, reset=None, handler=None):
        return Vw(path or self.Path, cache or self.Cache, procs or self.Pool.Procs, 
            norun or self.NoRun, reset or self.Reset, handler or self.Handler)

    def __run_impl__(self, inputs, opts_in, opts_out, input_mode, input_dir, job_type):
        job = job_type(self.Path, self.Cache, inputs, input_dir, opts_in, opts_out, input_mode, self.NoRun, self.Handler)
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
                loss = r.Result.Loss if r.Failed==None else None
                populated = r.Result.Populated if r.Result else None
                metrics = metrics_table(r.Result.Metrics) if r.Result.Metrics else None
                final_metrics = final_metrics_table(r.Result.Metrics) if r.Result.Metrics else None
                results = {'!Loss': loss, '!Populated': populated,
                           '!Metrics': metrics,
                           '!FinalMetrics': final_metrics,
                           '!Job': r}
                result_pd.append(dict(r.OptsIn, **results))
            return pd.DataFrame(result_pd)
        else:
            return [r.Result for r in self.__run_on_dict__(inputs, opts_in, opts_out, input_mode, input_dir, job_type)]

    def cache(self, inputs, opts, input_dir = ''):
        return self.__run__(inputs, {'#cmd': VwOpts.to_cache_cmd(opts)}, ['--cache_file'], '-d', input_dir, TestJob)

    def train(self, inputs, opts_in, opts_out=[], input_mode='-d', input_dir=''):
        return self.__run__(inputs, opts_in, opts_out, input_mode, input_dir, TrainJob)

    def test(self, inputs, opts_in, opts_out=[], input_mode='-d', input_dir=''):
        return self.__run__(inputs, opts_in, opts_out, input_mode, input_dir, TestJob)
