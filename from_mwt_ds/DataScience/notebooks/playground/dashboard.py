import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import inspect


class Dashboard:
    def __init__(self, required_outputs, visualizers, figsize=(12, 4)):
        def _is_train_visualizer(visualizer):
            return 'job' in set(inspect.signature(visualizer).parameters.keys())

        self.env_visualizers = []
        self.train_visualizers = []

        self.fig = plt.figure(figsize=figsize)
        plots_over_y = len(visualizers)
        plots_over_x = max([len(row) for row in visualizers])
        gs = gridspec.GridSpec(plots_over_y, 2 * plots_over_x)
        gs.update(wspace=0.5)
        for i in range(plots_over_y):
            cols = len(visualizers[i])
            offset = plots_over_x - cols
            for j in range(cols):
                v = visualizers[i][j]
                ax = plt.subplot(gs[i, j * 2 + offset: j * 2 + offset + 2], )
                if _is_train_visualizer(v):
                    self.train_visualizers.append((ax, v))
                else:
                    self.env_visualizers.append((ax, v))

        self.vw_outputs = required_outputs

    def reset(self):
        [ax_v[0].clear() for ax_v in self.env_visualizers]
        [ax_v[0].clear() for ax_v in self.train_visualizers]

    def after_simulation(self, examples):
        [ax_v[1](examples, ax=ax_v[0]) for ax_v in self.env_visualizers]

    def after_train(self, examples, job):
        [ax_v[1](examples, job, ax=ax_v[0]) for ax_v in self.train_visualizers]
        self.fig.canvas.draw_idle()