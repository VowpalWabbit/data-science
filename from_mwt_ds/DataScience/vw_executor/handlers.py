import shutil
from pathlib import Path


class HandlerBase:
    def on_start(self, inputs, opts): 
        ...

    def on_finish(self, result):
        ...

    def on_job_start(self, job):
        ...

    def on_job_finish(self, job):
        ...

    def on_task_start(self, job, task_idx):
        ...

    def on_task_finish(self, job, task_idx):
        ...


class ProgressBars(HandlerBase):      
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

    def on_task_finish(self, job, _task_idx):
        if self.verbose:
            self.jobs[job.name].update(1)
            self.jobs[job.name].refresh()


class ArtifactCopy(HandlerBase):
    def __init__(self, path, stdout_copy=True, outputs=None, reset=True):
        self.folder = Path(path)
        self.folder.mkdir(exist_ok=True, parents=True)
        self.stdout_copy = stdout_copy
        self.outputs = outputs or []
        self.reset = reset

    def _folder(self, job, output):
        return self.folder.joinpath(job.name).joinpath(output)

    def on_start(self, inputs, opts):
        if self.reset:
            shutil.rmtree(self.folder)

    def on_job_start(self, job):
        if self.stdout_copy:
            self._folder(job, 'stdout').mkdir(exist_ok=True, parents=True)
        for o in self.outputs:
            self._folder(job, o).mkdir(exist_ok=True, parents=True)

    def on_task_finish(self, job, task_idx):
        import shutil
        if self.stdout_copy:
            shutil.copyfile(job[task_idx].stdout.path, self._folder(job, 'stdout').joinpath(str(task_idx)))
        for o in self.outputs:
            shutil.copyfile(job[task_idx].outputs[o], self._folder(job, o).joinpath(str(task_idx)))


class AzureMLHandler(HandlerBase):
    def __init__(self, context):
        self.context = context

    def on_finish(self, result):
        best = result if not isinstance(result, list) else sorted(result, key=lambda x: x.loss)[0]
        for k, v in best.opts.items():
            if k != '#base':
                self.context.log(k, v)
        self.context.log('best_loss', best.loss)

    def on_task_finish(self, job, task_idx):
        from vw_executor.vw import ExecutionStatus
        task = job[task_idx]
        if task.status == ExecutionStatus.Success:
            for i, row in task.loss_table.iterrows():
                self.context.log(name='loss', value=row['loss'])
                self.context.log(name='since_last', value=row['since_last'])

            for key, value in task.metrics.items():
                self.context.log(key, value)


class MultiHandler:
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
