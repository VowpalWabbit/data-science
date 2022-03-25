from ipywidgets import interactive, VBox, Accordion, Layout, GridBox, fixed
from pathlib import Path
from vw_executor.vw import Vw
from functools import reduce
from playground.utils import get_simulation


def _collapse(*grids):
    from collections import OrderedDict
    result = reduce(lambda r, g: OrderedDict(r, **g), grids)
    separator = [len(g) for g in grids]
    return result, separator


def _split(collapsed, separator):
    result = []
    last = 0
    items = list(collapsed.items())
    for c in separator:
        result.append(dict(items[last:last + c]))
        last = last + c
    return tuple(result)


def _grid_layout(elements, columns=4):
    rows = (len(elements) - 1) // columns + 1
    layout = Layout(grid_template_rows=' '.join(['auto'] * rows), grid_template_columns=' '.join(['auto'] * columns))
    return GridBox(children=elements, layout=layout)


class VwPlayground:
    def __init__(self, simulation, visualization, vw_binary=None, cache_path='.cache'):
        self.data_folder = Path(cache_path).joinpath('datasets').joinpath(str(hash(simulation.__code__)))
        self.simulation = simulation
        self.examples = None
        self.visualization = visualization
        self.last_job = None
        self.vw = Vw(cache_path, vw_binary, handlers=[])

    def run(self, simulator_grid, vw_grid, columns=4):
        class State:
            def __init__(self, opts, examples, examples_path):
                self.opts = opts
                self.examples = examples
                self.examples_path = examples_path

            def update(self, opts, data_folder, simulation):
                if opts != self.opts:
                    self.opts = opts
                    self.examples, self.examples_path = get_simulation(data_folder, simulation, **opts)

        def _run_and_plot(separator, state, **options):
            sim_opts, train_opts = _split(options, separator)
            self.visualization.reset()
            state.update(sim_opts, self.data_folder, self.simulation)
            self.visualization.after_simulation(state.examples)
            self.last_job = self.vw.train(
                [state.examples_path], train_opts, self.visualization.vw_outputs)
            self.visualization.after_train(state.examples, self.last_job)

        collapsed, separator = _collapse(simulator_grid, vw_grid)
        state = State({}, None, None)
        widget = interactive(_run_and_plot, separator=fixed(separator), state=fixed(state), **collapsed)
        simulator_controls = _grid_layout(widget.children[:len(simulator_grid)], columns)
        vw_controls = _grid_layout(widget.children[len(simulator_grid):len(simulator_grid) + len(vw_grid)], columns)
        controls = Accordion(children=[simulator_controls, vw_controls])
        controls.set_title(0, 'Simulator args')
        controls.set_title(1, 'Vw args')
        output = widget.children[-1]
        display(VBox([controls, output]))
