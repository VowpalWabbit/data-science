import subprocess
import os
import pandas as pd
import enum

from vw_executor.pool import SeqPool, MultiThreadPool
from vw_executor import vw_opts
from vw_executor.loggers import MultiLoggers
from vw_executor.handlers import Handlers
from vw_executor.vw_cache import VwCache
from vw_executor.handlers import WidgetHandler

import multiprocessing


def _safe_to_float(num: str, default):
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
def _extract_metrics(out_lines):
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


def _parse_vw_output(lines):
    average_loss, since_last, metrics = _extract_metrics(lines)
    loss = None
    if 'average loss' in metrics:
        # Include the final loss as the primary metric
        loss = _safe_to_float(metrics['average loss'], None)
    return {'loss_per_example': average_loss, 'since_last': since_last, 'metrics': metrics}, loss


def _metrics_table(metrics, name):
    return pd.DataFrame({'n': [int(k) for k in metrics[name]],
                        name: [float(metrics[name][k]) for k in metrics[name]]
                        }).set_index('n')


def metrics_table(metrics):
    return pd.concat([_metrics_table(m, 'loss_per_example').join(_metrics_table(m, 'since_last')).assign(file=i)
                      for i, m in enumerate(metrics)]).reset_index().set_index(['file', 'n'])


def final_metrics_table(metrics):
    return [m['metrics'] for m in metrics]


def _save(txt, path):
    with open(path, 'w') as f:
        f.write(txt)


def _load(path):
    with open(path, 'r') as f:
        return f.read()


class ExecutionStatus(enum.Enum):
    NotStarted = 1
    Running = 2
    Success = 3
    Failed = 4


class Task:
    def __init__(self, job, logger, input_file, input_folder, model_file, model_folder='', no_run=False):
        self._job = job
        self.input_file = input_file
        self.input_folder = input_folder
        self._logger = logger
        self.status = ExecutionStatus.NotStarted
        self.model_file = model_file
        self.model_folder = model_folder
        self._no_run = no_run
        self.loss = None
        self.args = self._prepare_args(self._job.cache)
        self.metrics = {}

    def _prepare_args(self, cache):
        opts = self._job.opts.copy()
        opts[self._job.input_mode] = self.input_file

        input_full = os.path.join(self.input_folder, self.input_file)

        salt = os.path.getsize(input_full)
        if self.model_file:
            opts['-i'] = self.model_file

        self.outputs_relative = {o: cache.get_rel_path(opts, o, salt) for o in self._job.outputs.keys()}
        self.outputs = {o: cache.get_path(opts, o, salt, self._logger) for o in self._job.outputs.keys()}

        self.stdout_path = cache.get_path(opts, None, salt, self._logger)

        if self.model_file:
            opts['-i'] = os.path.join(self.model_folder, self.model_file)

        opts[self._job.input_mode] = input_full
        opts = dict(opts, **self.outputs)
        return vw_opts.to_string(opts)

    def _run(self):
        command = f'{self._job.vw_path} {self.args}'
        self._logger.debug(f'Executing: {command}')
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
        result_files = list(self.outputs.values()) + [self.stdout_path]
        not_exist = next((p for p in result_files if not os.path.exists(p)), None)

        if reset or not_exist:
            if not_exist:
                self._logger.debug(f'{not_exist} had not been found.')
            if self._no_run:
                raise Exception('Result is not found, and execution is deprecated')

            result = self._run()
            _save(result, self.stdout_path)
        else:
            self._logger.debug(f'Result of vw execution is found: {self.args}')
        self.metrics, self.loss = _parse_vw_output(self.stdout())
        self.status = ExecutionStatus.Success if self.loss is not None else ExecutionStatus.Failed

    def stdout(self):
        return open(self.stdout_path, 'r').readlines()


class Job:
    def __init__(self, vw_path, cache, opts, outputs, input_mode, handler, logger):
        self.vw_path = vw_path
        self.cache = cache
        self.opts = opts
        self.name = vw_opts.to_string({k: opts[k] for k in opts.keys() - {'#base'}})
        self._logger = logger[self.name]
        self.input_mode = input_mode
        self.failed = None
        self._handler = handler
        self.status = ExecutionStatus.NotStarted
        self.loss = None
        self.outputs = {o: [] for o in outputs}
        self.metrics = []
        self.tasks = []

    def run(self, reset):
        self._handler.on_job_start(self)
        self._logger.debug('Starting job...')
        self.status = ExecutionStatus.Running
        for i, t in enumerate(self.tasks):
            self._logger.debug(f'Starting task {i}...')
            self._handler.on_task_start(self, i)
            t.run(reset)
            self._handler.on_task_finish(self, i)
            self._logger.debug(f'Task {i} is finished: {t.status}')
            if t.status == ExecutionStatus.Failed:
                self.failed = t
                break
            for p in t.outputs:
                self.outputs[p].append(t.outputs[p])
            self.metrics.append(t.metrics)

        self.status = self.failed.status if self.failed is not None else ExecutionStatus.Success
        self._logger.debug(f'Job is finished: {self.status}')
        self.loss = self.tasks[-1].loss if len(self.tasks) > 0 and self.status == ExecutionStatus.Success else None
        self._handler.on_job_finish(self)
        return self

    def to_dict(self):
        loss = self.loss if self.failed is None else None
        metrics = metrics_table(self.metrics) if self.metrics else None
        final_metrics = final_metrics_table(self.metrics) if self.metrics else None
        return dict(self.opts, **{'!Loss': loss,
                '!Outputs': self.outputs,
                '!Metrics': metrics,
                '!FinalMetrics': final_metrics,
                '!Job': self})

class TestJob(Job):
    def __init__(self, vw_path, cache, files, input_dir, opts, outputs, input_mode, no_run, handler, logger):
        super().__init__(vw_path, cache, opts, outputs, input_mode, handler, logger)
        for f in files:
            self.tasks.append(Task(self, self._logger, f, input_dir, None, cache.path, no_run))


class TrainJob(Job):
    def __init__(self, vw_path, cache, files, input_dir, opts, outputs, input_mode, no_run, handler, logger):
        if '-f' not in outputs:
            outputs.append('-f')
        super().__init__(vw_path, cache, opts, outputs, input_mode, handler, logger)
        for i, f in enumerate(files):
            model = None if i == 0 else self.tasks[i - 1].outputs_relative['-f']
            self.tasks.append(Task(self, self._logger, f, input_dir, model, cache.path, no_run))


class Vw:
    def __init__(self, path, cache_path,
        procs=max(1, multiprocessing.cpu_count() // 2),
        no_run=False,
        reset=False,
        handlers=[WidgetHandler()],
        loggers=None):
        self.path = path
        self._cache = VwCache(cache_path)
        self.logger = MultiLoggers(loggers or [])
        self.pool = SeqPool() if procs == 1 else MultiThreadPool(procs)
        self.no_run = no_run
        self.handler = Handlers(handlers or [])
        self.reset = reset

    def _with(self, path=None, cache_path=None, procs=None, no_run=None, reset=None, handlers=None, loggers=None):
        return Vw(path or self.path, cache_path or self._cache.path, procs or self.pool.procs,
                  no_run if no_run is not None else self.no_run,
                  reset if reset is not None else self.reset, handlers or self.handler.handlers,
                  loggers or self.logger.loggers)

    def _run_impl(self, inputs, opts, outputs, input_mode, input_dir, job_type):
        job = job_type(self.path, self._cache, inputs, input_dir, opts, outputs, input_mode, self.no_run,
                       self.handler, self.logger)
        return job.run(self.reset)

    def _run_on_dict(self, inputs, opts, outputs, input_mode, input_dir, job_type):
        if not isinstance(inputs, list):
            inputs = [inputs]
        self.handler.on_start(inputs, opts)
        if isinstance(opts, list):
            args = [(inputs, point, outputs, input_mode, input_dir, job_type) for point in opts]
            result = self.pool.map(self._run_impl, args)
        else:
            result = self._run_impl(inputs, opts, outputs, input_mode, input_dir, job_type)
        self.handler.on_finish(result)
        return result

    def _run(self, inputs, opts, outputs, input_mode, input_dir, job_type):
        if isinstance(opts, pd.DataFrame):
            opts = list(opts.loc[:, ~opts.columns.str.startswith('!')].to_dict('index').values())
            result = self._run_on_dict(inputs, opts, outputs, input_mode, input_dir, job_type)
            result_pd = []
            for t in result:
                result_pd.append(t.to_dict())
            return pd.DataFrame(result_pd)
        else:
            return self._run_on_dict(inputs, opts, outputs, input_mode, input_dir, job_type)

    def cache(self, inputs, opts, input_dir=''):
        if isinstance(opts, list):
            cache_opts = [{'#cmd': o_dedup} for o_dedup in set([vw_opts.to_cache_cmd(o) for o in opts])]
        else:
            cache_opts = {'#cmd': vw_opts.to_cache_cmd(opts)}
        return self._run(inputs, cache_opts, ['--cache_file'], '-d', input_dir, TestJob)

    def train(self, inputs, opts, outputs=None, input_mode='-d', input_dir=''):
        return self._run(inputs, opts, outputs or [], input_mode, input_dir, TrainJob)

    def test(self, inputs, opts, outputs=None, input_mode='-d', input_dir=''):
        return self._run(inputs, opts, outputs or [], input_mode, input_dir, TestJob)
