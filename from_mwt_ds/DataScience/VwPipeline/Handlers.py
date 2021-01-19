from VwPipeline.Vw import ExecutionStatus

import os
import shutil


class WidgetHandler:      
    def __init__(self, leave=False):
        self.Total = None
        self.Tasks = 0
        self.Done = 0
        self.TimePerJob = 0
        self.Leave = leave
        self.Jobs = {}

    def on_start(self, inputs, opts):
        from tqdm import tqdm_notebook as tqdm
        self.Jobs = {}
        self.Tasks = len(inputs)
        self.Total = tqdm(range(len(opts)), desc='Total', leave=self.Leave)

    def on_finish(self, _result):
        self.Total.close()    

    def on_job_start(self, job):
        from tqdm import tqdm_notebook as tqdm
        self.Jobs[job.name] = tqdm(range(self.Tasks), desc=job.name, leave=self.Leave)

    def on_job_finish(self, job):
        self.Jobs[job.name].close()
        self.Jobs.pop(job.name)
        self.Total.update(1)
        self.Total.refresh()

    def on_task_start(self, job, task_idx):
        pass

    def on_task_finish(self, job, _task_idx):
        self.Jobs[job.name].update(1)
        self.Jobs[job.name].refresh()


class AzureMLHandler:
    def __init__(self, context, folder=None):
        self.Folder = folder
        if self.Folder:
            os.makedirs(folder, exist_ok=True)
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
        task = job.Tasks[task_idx]
        if self.Folder and os.path.exists(task.stdout_path):
            fname = f'{job.name}.{task_idx}.stdout.txt'
            shutil.copyfile(task.stdout_path, os.path.join(self.Folder, fname))
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
        self.Handlers = handlers

    def on_start(self, inputs, opts):
        for h in self.Handlers:
            h.on_start(inputs, opts)

    def on_finish(self, result):
        for h in self.Handlers:
            h.on_finish(result)

    def on_job_start(self, job):
        for h in self.Handlers:
            h.on_job_start(job)

    def on_job_finish(self, job):
        for h in self.Handlers:
            h.on_job_finish(job)

    def on_task_start(self, job, task_idx):
        for h in self.Handlers:
            h.on_task_start(job, task_idx)

    def on_task_finish(self, job, task_idx):
        for h in self.Handlers:
            h.on_task_finish(job, task_idx)
