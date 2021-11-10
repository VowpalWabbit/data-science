import enum
import multiprocessing
from pathlib import Path
import subprocess
import time

import pandas as pd

from vw_executor.pool import SeqPool, MultiThreadPool
from vw_executor.loggers import _MultiLoggers
from vw_executor.handlers import _Handlers
from vw_executor.vw_cache import VwCache
from vw_executor.handlers import ProgressBars
from vw_executor.vw_opts import VwOpts, InteractiveGrid


def _safe_to_float(num: str, default):
    try:
        return float(num)
    except (ValueError, TypeError):
        return default


def _to(value: str, types: list):
    for t in types:
        try:
            return t(value)
        except (ValueError, TypeError):
            ...
    return value


# Helper function to extract example counters and metrics from VW output.
# Counter lines are preceded by a single line containing the text:
#   loss     last          counter         weight    label  predict features
# and followed by a blank line
# Metric lines have the following form:
# metric_name = metric_value

def _parse_loss(loss_str):
    if loss_str.strip()[-1] == 'h':
        loss_str = loss_str.strip()[:-1]
    return _safe_to_float(loss_str, None)


def _extract_metrics(out_lines):
    loss_table = {'i': [], 'loss': [], 'since_last': []}
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
                    try:
                        count, average_loss, since_last = counter_line[2], counter_line[0], counter_line[1]
                        average_loss_f = float(average_loss)
                        since_last_f = float(since_last)
                        loss_table['i'].append(count)
                        loss_table['loss'].append(average_loss_f)
                        loss_table['since_last'].append(since_last_f)
                    except (ValueError, TypeError):
                        ...  # todo: handle
            elif line.startswith('loss'):
                fields = line.split()
                if fields[0] == 'loss' and fields[1] == 'last' and fields[2] == 'counter':
                    record = True
            elif '=' in line:
                key_value = [p.strip() for p in line.split('=')]
                if key_value[0] == 'average loss':
                    metrics[key_value[0]] = _parse_loss(key_value[1])
                else:
                    metrics[key_value[0]] = _to(key_value[1], [int, float])
    finally:
        return pd.DataFrame(loss_table).set_index('i'), metrics


def _save(txt, path):
    with open(path, 'w') as f:
        if isinstance(txt, str):
            f.write(txt)
        else:
            f.writelines(txt)


class ExecutionStatus(enum.Enum):
    NotStarted = 1
    Running = 2
    Success = 3
    Failed = 4


class Output:
    def __init__(self, path):
        self.path = path
        self._processed = False
        self._loss = None
        self._loss_table = None
        self._metrics = None

    def _process(self):
        self._processed = True
        self._loss_table, self._metrics = _extract_metrics(self.raw)
        if 'average loss' in self._metrics:
            self._loss = self._metrics['average loss']

    @property
    def raw(self):
        with open(self.path, 'r') as f:
            return f.readlines()

    @property
    def loss(self):
        if not self._processed:
            self._process()
        return self._loss

    @property
    def loss_table(self):
        if not self._processed:
            self._process()
        return self._loss_table

    @property
    def metrics(self):
        if not self._processed:
            self._process()
        return self._metrics


class Task:
    def __init__(self, job, logger, input_file, input_folder, model_file, model_folder='', no_run=False):
        self._job = job
        self._logger = logger
        self.input_file = input_file
        self.input_folder = input_folder
        self.status = ExecutionStatus.NotStarted
        self.model_file = model_file
        self.model_folder = model_folder
        self._no_run = no_run
        self.args = self._prepare_args(self._job._cache)
        self.start_time = None
        self.end_time = None
        self.stdout = None

    def _prepare_args(self, cache):
        opts = self._job.opts.copy()
        opts[self._job.input_mode] = self.input_file

        input_full = Path(self.input_folder).joinpath(self.input_file)

        salt = Path(input_full).stat().st_size
        if self.model_file:
            opts['-i'] = self.model_file

        self.outputs_relative = {o: cache.get_path(opts, o, salt) for o in self._job.outputs.keys()}
        self.outputs = {o: Path(cache.path).joinpath(p) for o, p in self.outputs_relative.items()}

        self.stdout_path = Path(cache.path).joinpath(cache.get_path(opts, None, salt, self._logger))

        if self.model_file:
            opts['-i'] = Path(self.model_folder).joinpath(self.model_file)

        opts[self._job.input_mode] = input_full
        opts = VwOpts(dict(opts, **self.outputs))
        return str(opts)

    def _execute(self):
        self._logger.debug(f'Executing: {self.args}')
        return self._job._vw.run(self.args)

    def _run(self, reset):
        result_files = list(self.outputs.values()) + [self.stdout_path]
        not_exist = next((p for p in result_files if not Path(p).exists()), None)
        self.start_time = time.time()
        if reset or not_exist:
            if not_exist:
                self._logger.debug(f'{not_exist} had not been found.')
            if self._no_run:
                raise Exception('Result is not found, and execution is deprecated')

            result = self._execute()
            _save(result, self.stdout_path)
        else:
            self._logger.debug(f'Result of vw execution is found: {self.args}')
        self.end_time = time.time()
        self.stdout = Output(self.stdout_path)
        self.status = ExecutionStatus.Success if self.stdout.loss is not None else ExecutionStatus.Failed

    def reset_stdout(self):
        Path(self.stdout_path).unlink()

    @property
    def loss(self):
        return self.stdout.loss if self.stdout else None

    @property
    def loss_table(self):
        return self.stdout.loss_table if self.stdout else None

    @property
    def metrics(self):
        return self.stdout.metrics if self.stdout else None

    @property
    def runtime_s(self):
        return self.end_time - self.start_time if self.end_time else None


class Job:
    def __init__(self, vw, cache, opts, outputs, input_mode, handler, logger):
        self._vw = vw
        self._cache = cache
        self.opts = opts
        self.name = str(VwOpts({k: opts[k] for k in VwOpts(opts).keys() - {'#base'}}))
        self._logger = logger[self.name]
        self.input_mode = input_mode
        self.failed = None
        self._handler = handler
        self.status = ExecutionStatus.NotStarted
        self.outputs = {o: [] for o in outputs}
        self._tasks = []

    def _run(self, reset):
        self._handler.on_job_start(self)
        self._logger.info('Starting job...')
        self.status = ExecutionStatus.Running
        for i, t in enumerate(self._tasks):
            self._logger.info(f'Starting task {i}...     File name: {t.input_file}')
            self._handler.on_task_start(self, i)
            t._run(reset)
            self._handler.on_task_finish(self, i)
            self._logger.info(f'Task {i} is finished: {t.status}')
            if t.status == ExecutionStatus.Failed:
                self.failed = t
                break
            for p in t.outputs:
                self.outputs[p].append(t.outputs[p])

        self.status = self.failed.status if self.failed is not None else ExecutionStatus.Success
        self._logger.info(f'Job is finished: {self.status}')
        self._handler.on_job_finish(self)
        return self

    def __getitem__(self, i):
        return self._tasks[i]

    def __len__(self):
        return len(self._tasks)

    @property
    def loss(self):
        return self[-1].loss

    @property
    def loss_table(self):
        return pd.concat([t.stdout.loss_table.assign(file=i)
                          for i, t in enumerate(self._tasks)]).reset_index().set_index(['file', 'i'])

    def to_dict(self):
        return dict(self.opts, **{'!Loss': self.loss, '!Status': self.status.name, '!Job': self})

    @property
    def runtime_s(self):
        return self[-1].end_time - self[0].start_time if self[-1].end_time else None

    @property
    def metrics(self):
        return pd.DataFrame([t.metrics for t in self._tasks])


class TestJob(Job):
    def __init__(self, vw, cache, files, input_dir, opts, outputs, input_mode, no_run, handler, logger):
        super().__init__(vw, cache, opts, outputs, input_mode, handler, logger)
        for f in files:
            self._tasks.append(Task(self, self._logger, f, input_dir, None, cache.path, no_run))


class TrainJob(Job):
    def __init__(self, vw, cache, files, input_dir, opts, outputs, input_mode, no_run, handler, logger):
        if '-f' not in outputs:
            outputs.append('-f')
        super().__init__(vw, cache, opts, outputs, input_mode, handler, logger)
        for i, f in enumerate(files):
            model = None if i == 0 else self._tasks[i - 1].outputs_relative['-f']
            self._tasks.append(Task(self, self._logger, f, input_dir, model, cache.path, no_run))


def _assert_path_is_supported(path):
    if ' -' in str(path):
        raise ValueError(f'Paths that are containing " -" as substring are not supported: {path}')
    return path


class _VwBin:
    def __init__(self, path):
        self.path = path

    def run(self, args):
        command = f'{self.path} {args}'
        process = subprocess.Popen(
            command.split(),
            universal_newlines=True,
            encoding='utf-8',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        error = process.communicate()[1]
        return error


def _run_pyvw(args):
    from vowpalwabbit import pyvw
    with pyvw.vw(args, enable_logging=True) as execution:
        return execution.get_log()


class _VwPy:
    def __init__(self):
        self.path = None

    def run(self, args):
        from multiprocessing import Pool
        with Pool(1) as p:
            return p.apply(_run_pyvw, [args])


class Vw:
    def __init__(self, cache_path,
                 path=None,
                 procs=max(1, multiprocessing.cpu_count() // 2),
                 no_run=False,
                 reset=False,
                 handlers=[ProgressBars()],
                 loggers=None):
        self._cache = VwCache(_assert_path_is_supported(cache_path))
        self._vw = _VwBin(path) if path is not None else _VwPy()
        self.logger = _MultiLoggers(loggers or [])
        self.pool = SeqPool() if procs == 1 else MultiThreadPool(procs)
        self.no_run = no_run
        self.handler = _Handlers(handlers or [])
        self.reset = reset
        self.last_job = None

    def _with(self, path=None, cache_path=None, procs=None, no_run=None, reset=None, handlers=None, loggers=None):
        return Vw(cache_path or self._cache.path,
                  path or self._vw.path,
                  procs or self.pool.procs,
                  no_run if no_run is not None else self.no_run,
                  reset if reset is not None else self.reset,
                  handlers if handlers is not None else self.handler.handlers,
                  loggers if loggers is not None else self.logger.loggers)

    def _run_impl(self, inputs, opts, outputs, input_mode, input_dir, job_type):
        job = job_type(self._vw, self._cache, inputs, input_dir, VwOpts(opts), outputs, input_mode, self.no_run,
                       self.handler, self.logger)
        return job._run(self.reset)

    def _run_on_dict(self, inputs, opts, outputs, input_mode, input_dir, job_type):
        if not isinstance(inputs, list):
            inputs = [inputs]
        inputs = [_assert_path_is_supported(i) for i in inputs]
        if isinstance(opts, list):
            self.handler.on_start(inputs, opts)
            args = [(inputs, point, outputs, input_mode, input_dir, job_type) for point in opts]
            result = self.pool.map(self._run_impl, args)
        else:
            self.handler.on_start(inputs, [opts])
            result = self._run_impl(inputs, opts, outputs, input_mode, input_dir, job_type)
        self.handler.on_finish(result)
        return result

    def _run(self, inputs, opts, outputs, input_mode, input_dir, job_type):
        if isinstance(opts, pd.DataFrame):
            opts = opts.loc[:, ~opts.columns.str.startswith('!')].to_dict('records')
            result = self._run_on_dict(inputs, opts, outputs, input_mode, input_dir, job_type)
            result_pd = []
            for t in result:
                result_pd.append(t.to_dict())
            return pd.DataFrame(result_pd)
        else:
            return self._run_on_dict(inputs, opts, outputs, input_mode, input_dir, job_type)

    def cache(self, inputs, opts, input_dir=''):
        if isinstance(opts, pd.DataFrame):
            opts = opts.loc[:, ~opts.columns.str.startswith('!')].to_dict('records')
            cache_opts = [o_dedup for o_dedup in {VwOpts(o).to_cache_cmd() for o in opts}]
            result = self._run_on_dict(inputs, cache_opts, [], '-d', input_dir, TestJob)
            result_pd = []
            for t in result:
                result_pd.append(t.to_dict())
            return pd.DataFrame(result_pd)
        elif isinstance(opts, list):
            cache_opts = [o_dedup for o_dedup in {VwOpts(o).to_cache_cmd() for o in opts}]
        else:
            cache_opts = VwOpts(opts).to_cache_cmd()
        return self._run(inputs, cache_opts, ['--cache_file'], '-d', input_dir, TestJob)

    def train(self, inputs, opts, outputs=None, input_mode='-d', input_dir=''):
        if isinstance(opts, InteractiveGrid):
            return self._interact(inputs, opts, outputs or [], input_mode, input_dir, TrainJob)
        return self._run(inputs, opts, outputs or [], input_mode, input_dir, TrainJob)

    def test(self, inputs, opts, outputs=None, input_mode='-d', input_dir=''):
        if isinstance(opts, InteractiveGrid):
            return self._interact(inputs, opts, outputs or [], input_mode, input_dir, TestJob)
        return self._run(inputs, opts, outputs or [], input_mode, input_dir, TestJob)

    def _interact(self, inputs, opts, outputs, input_mode, input_dir, job_type):
        from ipywidgets import interactive, VBox, Layout, GridBox
        from IPython.display import display
        import matplotlib.pyplot as plt

        def _run_and_plot(**opts):
            self.last_job = self._with(handlers=[])._run(
                inputs, locals()['opts'], outputs, input_mode, input_dir, job_type)
            ax.clear()
            fig.suptitle('Loss')
            self.last_job.loss_table['loss'].plot(ax=ax)
            fig.canvas.draw()

        fig, ax = plt.subplots(dpi=100, figsize=[9, 4])
        widget = interactive(_run_and_plot, **opts)
        columns = 4
        rows = (len(widget.children) - 1) // columns + 1
        layout = Layout(
            grid_template_rows=' '.join(['auto'] * rows), grid_template_columns=' '.join(['auto'] * columns))
        control_elements = GridBox(children=widget.children[:-1], layout=layout)
        plot = widget.children[-1]
        display(VBox([control_elements, plot]))
