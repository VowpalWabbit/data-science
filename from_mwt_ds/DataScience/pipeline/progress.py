class tqdm_progress:      
    def __init__(self):
        self.impl = None

    def on_start(self, steps):
        from tqdm import tqdm_notebook as tqdm
        self.impl = tqdm(range(steps), desc='Progress', leave=False)

    def on_finish(self):
        self.impl.close()    

    def on_step(self):
        self.impl.update(1)
        self.impl.refresh()

class dummy_progress:      
    def __init__(self):
        ...

    def on_start(self, steps):
        ...

    def on_finish(self):
        ...

    def on_step(self):
        ...

