import shutil
from pathlib import Path


class ProgressBars:      
    def __init__(self, leave=False, verbose=False):
        self.total = None
        self.tasks = 0
        self.leave = leave
        self.jobs = {}
        self.verbose = verbose

    def on_start(self, inputs, opts):
        from tqdm.notebook import tqdm
        self.jobs = {}
        self.tasks = len(inputs)
        self.total = tqdm(range(len(opts)), desc='Total', leave=self.leave)

    def on_finish(self, _result):
        self.total.close()

    def on_job_start(self, job):
        from tqdm.notebook import tqdm
        if self.verbose:
            self.jobs[job.name] = tqdm(range(self.tasks), desc=job.name, leave=self.leave)

    def on_job_finish(self, job):
        if self.verbose:
            self.jobs[job.name].close()
            self.jobs.pop(job.name)
        self.total.update(1)
        self.total.refresh()

    def on_task_start(self, job, task_idx):
        pass

    def on_task_finish(self, job, _task_idx):
        if self.verbose:
            self.jobs[job.name].update(1)
            self.jobs[job.name].refresh()


class AzureMLHandler:
    def __init__(self, context, folder=None):
        self.folder = Path(folder) if folder is not None else None
        if self.folder:
            self.folder.mkdir(parents=True, exist_ok=True)
        self.context = context

    def on_start(self, inputs, opts):
        pass

    def on_finish(self, result):
        best = result if not isinstance(result, list) else sorted(result, key=lambda x: x.loss)[0]
        for k, v in best.opts.items():
            if k != '#base':
                self.context.log(k, v)
        self.context.log('loss', best.loss)

    def on_job_start(self, job):
        pass

    def on_job_finish(self, job):
        pass

    def on_task_start(self, job, task_idx):
        pass

    def on_task_finish(self, job, task_idx):
        from vw_executor.vw import ExecutionStatus
        task = job.tasks[task_idx]
        if self.folder and task.stdout_path.exists():
            fname = f'{job.name}.{task_idx}.stdout.txt'
            shutil.copyfile(task.stdout_path, self.folder.joinpath(fname))
        if task.status == ExecutionStatus.Success:
            per_example = task.metrics['loss_per_example']
            since_last = task.metrics['since_last']
            metrics = task.metrics['metrics']

            for key, value in per_example.items():
                self.context.log_row('avg_loss_by_example', count=key, loss=value)

            for key, value in since_last.items():
                self.context.log("loss_by_example", value)

            for key, value in metrics.items():
                self.context.log(key, value)


class _Handlers:
    def __init__(self, handlers):
        self.handlers = handlers

    def on_start(self, inputs, opts):
        for h in self.handlers:
            h.on_start(inputs, opts)

    def on_finish(self, result):
        for h in self.handlers:
            h.on_finish(result)

    def on_job_start(self, job):
        for h in self.handlers:
            h.on_job_start(job)

    def on_job_finish(self, job):
        for h in self.handlers:
            h.on_job_finish(job)

    def on_task_start(self, job, task_idx):
        for h in self.handlers:
            h.on_task_start(job, task_idx)

    def on_task_finish(self, job, task_idx):
        for h in self.handlers:
            h.on_task_finish(job, task_idx)
