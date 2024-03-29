import enum
import multiprocessing
from pathlib import Path
import subprocess
import time
import os

import pandas as pd

from vw_executor.artifacts import Output, Predictions, Model8, Model9, Model
from vw_executor.pool import SeqPool, MultiThreadPool, Pool
from vw_executor.loggers import MultiLogger, ILogger
from vw_executor.vw_cache import VwCache
from vw_executor.handlers import MultiHandler, HandlerBase, ProgressBars
from vw_executor.vw_opts import VwOpts, InteractiveGrid, VwOptsLike, GridLike

from typing import Callable, Iterable, Optional, Union, Dict, Any, Type, List, Generator
from itertools import chain
from abc import ABC, abstractmethod


def _save(txt: Union[str, Iterable[str]], path: Path) -> None:
    with open(path, 'w') as f:
        if isinstance(txt, str):
            f.write(txt)
        else:
            f.writelines(map(lambda l: f'{l}\n', txt))


class ExecutionStatus(enum.Enum):
    NotStarted = 1
    Running = 2
    Success = 3
    Failed = 4


class _VwCore(ABC):
    path: Optional[Path]

    def __init__(self, path: Optional[Path]):
        self.path = path

    @abstractmethod
    def run(self, args: str) -> Union[str, List[str]]:
        ...


class _VwBin(_VwCore):
    def __init__(self, path: Path):
        super().__init__(path)

    def run(self, args: str, out_path: Path) -> str:
        command = f'{self.path} {args}'
        stdout_file = open(out_path.parent / (out_path.name + '.out.txt'), 'w')
        stderr_temp = out_path.parent / (out_path.name + '.pending')
        stderr_file = open(stderr_temp, 'w')

        process = subprocess.Popen(
            command.split(),
            universal_newlines=True,
            encoding='utf-8',
            stdout=stdout_file,
            stderr=stderr_file
        )

        returncode = process.wait()
        stdout_file.close()
        stderr_file.close()

        os.replace(stderr_temp, out_path)

        return []


def _run_pyvw(args: str, filename=None) -> Iterable[str]:
    import traceback

    from vowpalwabbit import pyvw
    try:
        execution = pyvw.Workspace(args, enable_logging=True)
    except Exception as e:
        if filename is not None:
            stderr_temp = filename.parent / (filename.name + '.pending')
            with stderr_temp.open('w') as f:
                f.write(str(e)+"\n\n")
                traceback.print_exc(file=f)
            return []
        else:
            raise e
    execution.finish()
    result = [l.rstrip() for l in execution.get_log()]
    if filename is not None:
        stderr_temp = filename.parent / (filename.name + '.pending')
        _save(result, stderr_temp)
        os.replace(stderr_temp, filename)
        return []
    else:
        return result


class _VwPy(_VwCore):
    def __init__(self):
        super().__init__(None)

    def run(self, args: str, filename=None) -> Iterable[str]:
        from multiprocessing import Pool
        with Pool(1) as p:
            return p.apply(_run_pyvw, [args], {"filename": filename})

def symlink(source:Path, link_name:Path):
    import os

    if os.name == "nt":
        def symlink_ms(source, link_name):
            from subprocess import call
            returncode = call(['cmd', '/C','mklink', link_name, source], shell=False, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
            if returncode != 0:
                raise Exception(f"Make sure developer mode is enabled. Failed to create symlink {link_name} -> {source}.")
        return symlink_ms(source, link_name)
    else:
        return os.symlink(source, link_name)

def create_symlink_if_exists(source:Path, link_name):
    if link_name.exists():
        raise ValueError('Trying to overwrite existing symlink. Please consider changing the destination folder.')

    if source.exists():
        symlink(source, link_name)

class Task:
    job: 'Job'
    _logger: MultiLogger
    _no_run: bool
    input_file: Path
    input_folder: Path
    status: ExecutionStatus
    model_file: Optional[Path]
    model_folder: Path
    args: str
    start_time: Optional[float]
    end_time: Optional[float]
    stdout: Output
    outputs_relative: Dict[str, Path]
    outputs:  Dict[str, Path]

    def __init__(self,
                 job: 'Job',
                 logger: MultiLogger,
                 input_file: Path,
                 input_folder: Path,
                 model_file: Optional[Path],
                 model_folder: Path,
                 order_position: Optional[int] = 0,
                 no_run: bool = False):
        self.job = job
        self._logger = logger
        self.input_file = input_file
        self.input_folder = input_folder
        self.status = ExecutionStatus.NotStarted
        self.model_file = model_file
        self.model_folder = model_folder
        self._order_position = order_position
        self._no_run = no_run
        self.args = str(self._prepare_args(self.job.cache))
        self.start_time = None
        self.end_time = None
    
    def create_human_readeable_symlink(
        self,
        base_dir: Optional[Union[str, Path]] = None,
        translate_output: Dict[str, str] = {"-p": "predictions.txt", "-f": "final_regressor.vwmodel",
        "--extra_metrics": "extra_metrics.json", "--invert_hash": "invert_hash.txt", "--readable_model": "readable_model.txt"},
        create_symlink: Callable = create_symlink_if_exists) -> Path:
        if self.input_file.parent == self.input_file:
            raise ValueError("Input files cannot be on the root folder")

        def remove_argdash(arg: str):
            if len(arg) > 1 and arg[0] == "-" and arg[1] == "-":
                return arg[2:]
            elif arg[0] == "-":
                return arg[1:]
            else:
                return arg
        
        argdirname = ".".join([remove_argdash(arg) for arg in self.job.name.split()])

        if not base_dir:
            from datetime import datetime
            base_dir = Path.cwd() / "_results" / datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        else:
            base_dir = Path(base_dir)
        
        input_file_dir = self.input_file.parent.absolute()
        input_file_name = str(self._order_position) + "th_file"

        stderr_temp = self.stdout.path.parent / (self.stdout.path.name + '.pending')

        if self.status == ExecutionStatus.Failed:
            task_dir = base_dir / "ERRORS" / argdirname / input_file_dir.name / input_file_name
        else:
            task_dir = base_dir / argdirname / input_file_dir.name / input_file_name
        task_dir.mkdir(parents=True, exist_ok=True)

        create_symlink(self.stdout.path.absolute(), task_dir / "stdout.txt")
        create_symlink(self.input_file.absolute(), task_dir / self.input_file.name)

        stdout_file = self.stdout.path.parent / (self.stdout.path.name + '.out.txt')
        create_symlink(stderr_temp.absolute(), task_dir / "ERROR_stdout.txt")

        if stderr_temp.exists():
            create_symlink(stdout_file.absolute(), task_dir / "more_ERROR_stdout.txt")
        else:
            create_symlink(stdout_file.absolute(), task_dir / "more_stdout.txt")

        for output_arg, filename in self.outputs.items():
            create_symlink(filename.absolute(), task_dir / translate_output[output_arg])

        if self.model_file:
            create_symlink(self.model_folder.joinpath(self.model_file).absolute(), task_dir / "input_regressor.vwmodel")
        
        vw_opts = self._prepare_args(self.job.cache)
        for o in self.outputs.keys():
            vw_opts[o] = translate_output[o] + ".repro"

        cmd_repro_file = task_dir / "cmd_repro.txt"
        with open(cmd_repro_file, "w") as f:
            f.write(f"cwd: {str(Path.cwd().absolute())}\n")
            f.write(f"vw args: {str(vw_opts)}\n")
        return task_dir

    def _prepare_args(self, cache: VwCache) -> VwOpts:
        opts = self.job.opts.copy()
        opts[self.job.input_mode] = self.input_file

        input_full = self.input_folder.joinpath(self.input_file)

        salt = input_full.stat().st_size
        if self.model_file:
            opts['-i'] = self.model_file

        self.outputs_relative = {o: cache.get_path(opts, self._logger, o, salt) for o in self.job.outputs.keys()}
        self.outputs = {o: cache.path.joinpath(p) for o, p in self.outputs_relative.items()}

        self.stdout = Output(cache.path.joinpath(cache.get_path(opts, self._logger, None, salt)))

        if self.model_file:
            opts['-i'] = self.model_folder.joinpath(self.model_file)

        opts[self.job.input_mode] = input_full
        opts = VwOpts(dict(opts, **self.outputs))
        return opts

    def _execute(self) -> Union[str, Iterable[str]]:
        self._logger.debug(f'Executing: {self.args}')
        return self.job.core.run(self.args, self.stdout.path)

    def run(self, reset: bool) -> None:
        self.status = ExecutionStatus.Running
        result_files = list(self.outputs.values()) + [self.stdout.path]
        not_exist = next((p for p in result_files if not p.exists()), None)
        self.start_time = time.time()
        if reset or not_exist:
            if not_exist:
                self._logger.debug(f'{not_exist} had not been found.')
            if self._no_run:
                raise Exception('Result is not found, and execution is deprecated')

            try:
                result = self._execute()
                assert result == []
                self.status = ExecutionStatus.Success if self.stdout.loss is not None else ExecutionStatus.Failed
            except:
                self.status = ExecutionStatus.Failed
                raise
            finally:
                self.end_time = time.time()
        else:
            self._logger.debug(f'Result of vw execution is found: {self.args}')
            self.end_time = time.time()
            self.status = ExecutionStatus.Success if self.stdout.loss is not None else ExecutionStatus.Failed

    def reset_stdout(self) -> None:
        self.stdout.path.unlink()

    def _get_artifact(self, key: str, artifact_type: Type) -> Any:
        return artifact_type(self.outputs[key])

    def predictions(self, key: str, problem = None) -> Union[Predictions, Generator[Dict, None, None]]:
        if not problem:
            return self._get_artifact(key, Predictions)
        events = problem.fget(self._get_artifact(key, Predictions))
        return map(lambda i_d: dict(i_d[1], **{'i': i_d[0]}), enumerate(events))

    def model8(self, key: str) -> Model8:
        return self._get_artifact(key, Model8)

    def model9(self, key: str) -> Model9:
        return self._get_artifact(key, Model9)

    def model(self, key: str) -> Model:
        return self._get_artifact(key, Model)     

    @property
    def loss(self) -> Optional[float]:
        return self.stdout.loss if self.status == ExecutionStatus.Success else None

    @property
    def loss_table(self) -> Optional[pd.DataFrame]:
        return self.stdout.loss_table if self.status == ExecutionStatus.Success else None

    @property
    def metrics(self) -> Optional[Dict[str, Any]]:
        return self.stdout.metrics if self.status == ExecutionStatus.Success else None

    @property
    def runtime_s(self) -> Optional[float]:
        return self.end_time - self.start_time if self.end_time else None


class Job:
    core: _VwCore
    cache: VwCache
    _logger: MultiLogger
    _handler: HandlerBase
    _tasks: List[Task]
    opts: VwOpts
    name: str
    input_mode: str
    failed: Optional[Task]
    status: ExecutionStatus
    outputs: Dict[str, List[Path]]

    def __init__(self,
                 vw: _VwCore,
                 cache: VwCache,
                 opts: VwOpts,
                 outputs: List[str],
                 input_mode: str,
                 handler: HandlerBase,
                 logger: MultiLogger):
        self.core = vw
        self.cache = cache
        self.opts = opts
        self.name = str(VwOpts({k: opts[k] for k in self.opts.keys() - {'#base'}}))
        self._logger = logger[self.name]
        self.input_mode = input_mode
        self.failed = None
        self._handler = handler
        self.status = ExecutionStatus.NotStarted
        self.outputs = {o: [] for o in outputs}
        self._tasks = []

    def run(self, reset: bool) -> 'Job':
        self._handler.on_job_start(self)
        self._logger.info('Starting job...')
        self.status = ExecutionStatus.Running
        for i, t in enumerate(self._tasks):
            self._logger.info(f'Starting task {i}...     File name: {t.input_file}')
            self._handler.on_task_start(self, i)
            try:
                t.run(reset)
            finally:
                self._handler.on_task_finish(self, i)
                self._logger.info(f'Task {i} is finished: {t.status}')
                for p in t.outputs:
                    self.outputs[p].append(t.outputs[p])
                if t.status == ExecutionStatus.Failed:
                    self.failed = t
                    break

        self.status = self.failed.status if self.failed is not None else ExecutionStatus.Success
        self._logger.info(f'Job is finished: {self.status}')
        self._handler.on_job_finish(self)
        return self

    def __getitem__(self, i) -> Task:
        return self._tasks[i]

    def __len__(self) -> int:
        return len(self._tasks)

    @property
    def loss(self) -> Optional[float]:
        return self[-1].loss

    @property
    def loss_table(self) -> pd.DataFrame:
        return pd.concat([t.stdout.loss_table.assign(file=i)
                          for i, t in enumerate(self._tasks)]).reset_index().set_index(['file', 'i'])

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.opts, **{'!Loss': self.loss, '!Status': self.status.name, '!Job': self})

    @property
    def runtime_s(self) -> Optional[float]:
        return self[-1].end_time - self[0].start_time if self[-1].end_time else None

    @property
    def metrics(self) -> pd.DataFrame:
        return pd.DataFrame([t.metrics for t in self._tasks])

    def predictions(self, key: str, problem) -> Generator[Dict, None, None]:
        if not problem:
            raise ValueError('Problem should be defined for job')
        for i, task in enumerate(self):
            for event in task.predictions(key, problem):
                yield dict(event, **{'file': i})


class TestJob(Job):
    def __init__(self,
                 vw: Union[_VwPy, _VwBin],
                 cache: VwCache,
                 files: List[Path],
                 input_dir: Path,
                 opts: VwOpts,
                 outputs: List[str],
                 input_mode: str,
                 no_run: bool,
                 handler: HandlerBase,
                 logger: MultiLogger):
        super().__init__(vw, cache, opts, outputs, input_mode, handler, logger)
        for i, f in enumerate(files):
            self._tasks.append(Task(self, self._logger, f, input_dir, None, cache.path, order_position=i, no_run=no_run))


class TrainJob(Job):
    def __init__(self,
                 vw: _VwCore,
                 cache: VwCache,
                 files: List[Path],
                 input_dir: Path,
                 opts: VwOpts,
                 outputs: List[str],
                 input_mode: str,
                 no_run: bool,
                 handler: HandlerBase,
                 logger: MultiLogger):
        if '-f' not in outputs:
            outputs.append('-f')
        super().__init__(vw, cache, opts, outputs, input_mode, handler, logger)
        for i, f in enumerate(files):
            model = None if i == 0 else self._tasks[i - 1].outputs_relative['-f']
            self._tasks.append(Task(self, self._logger, f, input_dir, model, cache.path, order_position=i, no_run=no_run))


def _assert_path_is_supported(path: Union[str, Path]) -> Path:
    if ' -' in str(path):
        raise ValueError(f'Paths that are containing " -" as substring are not supported: {path}')
    return Path(path)


class Vw:
    _cache: VwCache
    _vw: _VwCore
    logger: MultiLogger
    pool: Pool
    no_run: bool
    handler: HandlerBase
    reset: bool
    last_job: Optional[Job]

    def __init__(self,
                 cache_path: Union[str, Path],
                 path: Optional[Union[str, Path]] = None,
                 procs: int = max(1, multiprocessing.cpu_count() // 2),
                 no_run: bool = False,
                 reset: bool = False,
                 handler: Optional[HandlerBase] = ProgressBars(),
                 logger: Optional[ILogger] = None):
        self._cache = VwCache(_assert_path_is_supported(cache_path))
        self._vw = _VwBin(path) if path is not None else _VwPy()
        self.logger = logger or MultiLogger([])
        self.pool = SeqPool() if procs == 1 else MultiThreadPool(procs)
        self.no_run = no_run
        self.handler = handler or MultiHandler([])
        self.reset = reset
        self.last_job = None

    def _with(self,
              cache_path: Optional[Union[str, Path]] = None,
              path: Optional[Union[str, Path]] = None,
              procs: Optional[int] = None,
              no_run: Optional[bool] = None,
              reset: Optional[bool] = None,
              handler: Optional[HandlerBase] = None,
              logger: Optional[ILogger] = None) -> 'Vw':
        return Vw(cache_path or self._cache.path,
                  path or self._vw.path,
                  procs or self.pool.procs,
                  no_run if no_run is not None else self.no_run,
                  reset if reset is not None else self.reset,
                  handler or self.handler,
                  logger or self.logger)

    def _run_impl(self,
                  inputs: List[Path],
                  opts: VwOptsLike,
                  outputs: List[str],
                  input_mode: str,
                  input_dir: Union[Path, str],
                  job_type: Type) -> Job:
        job = job_type(self._vw, self._cache, inputs, Path(input_dir), VwOpts(opts), outputs, input_mode, self.no_run,
                       self.handler, self.logger)
        return job.run(self.reset)

    def _run_on_dict(self,
                     inputs: Union[str, Path, List[Union[Path, str]]],
                     opts: Union[VwOptsLike, GridLike],
                     outputs: List[str],
                     input_mode: str,
                     input_dir: Union[str, Path],
                     job_type: Type) -> Union[Job, List[Job]]:
        if not isinstance(inputs, list):
            inputs = [inputs]
        inputs = [_assert_path_is_supported(i) for i in inputs]
        input_dir = Path(input_dir)
        if isinstance(opts, list):
            self.handler.on_start(inputs, opts)
            args = [(inputs, point, outputs, input_mode, input_dir, job_type) for point in opts]
            result = self.pool.map(self._run_impl, args)
        else:
            self.handler.on_start(inputs, [opts])
            result = self._run_impl(inputs, opts, outputs, input_mode, input_dir, job_type)
        self.handler.on_finish(result)
        return result

    def _run_on_dataframe(self,
                     inputs: Union[str, Path, List[Union[Path, str]]],
                     opts: pd.DataFrame,
                     outputs: List[str],
                     input_mode: str,
                     input_dir: Union[str, Path],
                     job_type: Type) -> pd.DataFrame:
        opts_dict = opts.loc[:, ~opts.columns.str.startswith('!')].to_dict('records')
        result = self._run_on_dict(inputs, opts_dict, outputs, input_mode, input_dir, job_type)
        return pd.DataFrame([t.to_dict() for t in result]).set_index(opts.index)                     

    def _run(self,
             inputs: Union[str, Path, List[Union[Path, str]]],
             opts: Union[pd.DataFrame, VwOptsLike, GridLike],
             outputs: List[str],
             input_mode: str,
             input_dir: Union[Path, str],
             job_type: Type) -> Union[Job, List[Job], pd.DataFrame]:
        if isinstance(opts, pd.DataFrame):
            return self._run_on_dataframe(inputs, opts, outputs, input_mode, input_dir, job_type)
        else:
            return self._run_on_dict(inputs, opts, outputs, input_mode, input_dir, job_type)

    def cache(self,
              inputs: Union[str, Path, List[Union[Path, str]]],
              opts: Union[pd.DataFrame, VwOptsLike, GridLike],
              input_dir: Union[Path, str] = '') -> Union[Job, List[Job], pd.DataFrame]:
        if isinstance(opts, pd.DataFrame):
            self.logger.warning('Index is ignored during caching on dataframe')
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

    def train(self,
              inputs:  Union[str, Path, List[Union[Path, str]]],
              opts: Union[pd.DataFrame, VwOptsLike, GridLike, InteractiveGrid],
              outputs: Optional[List[str]] = None,
              input_mode: str = '-d',
              input_dir: Union[Path, str] = '') -> Optional[Union[Job, List[Job], pd.DataFrame]]:
        if isinstance(opts, InteractiveGrid):
            return self._interact(inputs, opts, outputs or [], input_mode, input_dir, TrainJob)
        return self._run(inputs, opts, outputs or [], input_mode, input_dir, TrainJob)

    def test(self,
             inputs:  Union[str, Path, List[Union[Path, str]]],
             opts: Union[pd.DataFrame, VwOptsLike, GridLike, InteractiveGrid],
             outputs: Optional[List[str]] = None,
             input_mode: str = '-d',
             input_dir: Union[Path, str] = '') -> Optional[Union[Job, List[Job], pd.DataFrame]]:
        if isinstance(opts, InteractiveGrid):
            return self._interact(inputs, opts, outputs or [], input_mode, input_dir, TestJob)
        return self._run(inputs, opts, outputs or [], input_mode, input_dir, TestJob)

    def _interact(self,
                  inputs: Union[str, Path, List[Union[Path, str]]],
                  opts: InteractiveGrid,
                  outputs: Optional[List[str]],
                  input_mode: str,
                  input_dir: Union[Path, str],
                  job_type: Type) -> None:
        from ipywidgets import interactive, VBox, Layout, GridBox
        from IPython.display import display
        import matplotlib.pyplot as plt

        def _run_and_plot(**options):
            self.last_job = self._with(handler=None)._run(
                inputs, locals()['options'], outputs, input_mode, input_dir, job_type)
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
