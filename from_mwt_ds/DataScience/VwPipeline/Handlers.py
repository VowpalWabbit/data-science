class WidgetHandler:      
    def __init__(self, leave=False):
        self.Total = None
        self.Total = 0
        self.Tasks = 0
        self.Done = 0
        self.TimePerJob = 0
        self.Leave = leave

    def on_start(self, inputs, opts_in):
        from tqdm import tqdm_notebook as tqdm
        self.Jobs = {}
        self.Tasks = len(inputs)
        self.Total = tqdm(range(len(opts_in)), desc='Total', leave=self.Leave)

    def on_finish(self):
        self.Total.close()    

    def on_job_start(self, job):
        from tqdm import tqdm_notebook as tqdm
        self.Jobs[job.Name] = tqdm(range(self.Tasks), desc=job.Name, leave=self.Leave)

    def on_job_finish(self, job):
        self.Jobs[job.Name].close()
        self.Jobs.pop(job.Name)
        self.Total.update(1)
        self.Total.refresh()

    def on_task_start(self, job, task):
        pass

    def on_task_finish(self, job, task):
        self.Jobs[job.Name].update(1)
        self.Jobs[job.Name].refresh()

class __Handlers__:
    def __init__(self, handlers):
        self.Handlers = handlers

    def on_start(self, inputs, opts_in):
        for h in self.Handlers:
            h.on_start(inputs, opts_in)

    def on_finish(self):
        for h in self.Handlers:
            h.on_finish()

    def on_job_start(self, job):
        for h in self.Handlers:
            h.on_job_start(job)

    def on_job_finish(self, job):
        for h in self.Handlers:
            h.on_job_finish(job)

    def on_task_start(self, job, task):
        for h in self.Handlers:
            h.on_task_start(job, task)

    def on_task_finish(self, job, task):
        for h in self.Handlers:
            h.on_task_finish(job, task)
