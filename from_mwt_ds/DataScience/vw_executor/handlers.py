import os
import shutil


class WidgetHandler:      
    def __init__(self, leave=False):
        self.total = None
        self.tasks = 0
        self.leave = leave
        self.jobs = {}

    def on_start(self, inputs, opts):
        from tqdm import tqdm_notebook as tqdm
        self.jobs = {}
        self.tasks = len(inputs)
        self.total = tqdm(range(len(opts)), desc='Total', leave=self.leave)

    def on_finish(self, _result):
        self.total.close()

    def on_job_start(self, job):
        from tqdm import tqdm_notebook as tqdm
        self.jobs[job.name] = tqdm(range(self.tasks), desc=job.name, leave=self.leave)

    def on_job_finish(self, job):
        self.jobs[job.name].close()
        self.jobs.pop(job.name)
        self.total.update(1)
        self.total.refresh()

    def on_task_start(self, job, task_idx):
        pass

    def on_task_finish(self, job, _task_idx):
        self.jobs[job.name].update(1)
        self.jobs[job.name].refresh()


class AzureMLHandler:
    def __init__(self, context, folder=None):
        self.folder = folder
        if self.folder:
            os.makedirs(self.folder, exist_ok=True)
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
        if self.folder and os.path.exists(task.stdout_path):
            fname = f'{job.name}.{task_idx}.stdout.txt'
            shutil.copyfile(task.stdout_path, os.path.join(self.folder, fname))
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


class Handlers:
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
