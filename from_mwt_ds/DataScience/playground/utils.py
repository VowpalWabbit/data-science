from vw_executor.vw_opts import VwOpts
from pathlib import Path

import pandas as pd
import json


def save_examples(examples, path):
    with open(path, 'w') as f:
        for ex in examples:
            f.write(f'{json.dumps(ex, separators=(",", ":"))}\n')


def load_examples(path):
    with open(path) as f:
        for line in f:
            yield json.loads(line)


def get_simulation(folder, simulator, **kwargs):
    path = Path(folder).joinpath(f'{str(VwOpts(kwargs)).replace(" ", "-")}.json')
    if not path.exists():
        Path(folder).mkdir(parents=True, exist_ok=True)
        examples = list(simulator(**kwargs))
        save_examples(examples, path)
    else:
        examples = list(load_examples(path))
    return examples, path


def cb_df(examples):
    return pd.DataFrame([{
        'reward': -e['_label_cost'],
        'shared_good': e['c']['shared']['f'],
        'chosen': e['_labelIndex'],
        'prob': e['_label_probability']
    } for e in examples])
