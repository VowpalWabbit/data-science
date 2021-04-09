import json
import pandas as pd

from itertools import chain
from pathlib import Path



class FileSizeHasher:
    extension = 'size'
    
    def evaluate(self, path):
        return Path(path).stat().st_size

class FilesPipeline:
    hasher = FileSizeHasher()
    
    def _load_hash(self, path):
        hash_path = f'{path}.{self.hasher.extension}'
        if not Path(hash_path).exists():
            return None
        try:
            hash_value = int(open(hash_path, 'r').read())
        except:
            hash_value = None
        return hash_value
    
    def _is_in_sync(self, inp, output):
        input_hash = self.hasher.evaluate(inp)
        output_hash = self._load_hash(output)
        return input_hash and output_hash and input_hash == output_hash

    def _sync(self, inp, output):
        with open(f'{output}.{self.hasher.extension}','w') as f:
            f.write(str(self.hasher.evaluate(inp)))
    
    def __init__(self, hasher = None):
        self.hasher = hasher if hasher is not None else self.hasher
    
    def process(self, 
        files,
        processor,
        path_gen=None,
        process=False):
        path_gen = path_gen or (lambda f: f'{f}.{processor.__name__}') 
        result = []
        for path_in in files:
            print(f'Processing {path_in}...')
            path_out = path_gen(path_in)
            Path(path_out).parent.mkdir(parents=True, exist_ok=True)
            if process or not self._is_in_sync(path_in, path_out):
                with open(path_out, 'w') as fout:
                    with open(path_in) as fin:
                        fout.writelines(processor(fin))
                self._sync(path_in, path_out)
            if Path(path_out).exists():
                result.append(path_out)
        return result

def files_2_csvs(
    files,
    processor,
    path_gen=None,
    process=False):
    result = []
    for f in files:
        print(f'Processing {f}...')
        output = path_gen(f)
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        if process or not _is_in_sync(f, output):
            df = pd.DataFrame(processor(open(f)))
            if len(df) > 0:
                df.to_csv(output, index=False)
            _sync(f, output)
        if Path(output).exists():
            result.append(output)
    return result

def csvs_2_rows(files, processors=[]):
    if not processors:
        processors = [lambda d: d]
    for kv in chain.from_iterable(map(lambda f: pd.read_csv(f).iterrows(), files)):
        yield ChainMap(*[p(kv[1]) for p in processors])

def ndjsons_2_rows(files, processors=[]):
    if not processors:
        processors = [lambda d: d]
    for o in map(lambda l: json.loads(l), chain.from_iterable(map(lambda f: open(f), files))):
        yield o

from itertools import zip_longest

def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)

def _aggregate_batch(batch, agg_factories: dict):    # agg_factories: map from policy to map from name to agg factory
    aggs = {policy: {name: agg_factories[policy][name]() for name in agg_factories[policy]} for policy in agg_factories}
    for event in batch:
        if event:
            for policy in aggs:
                for name in aggs[policy]:
                    aggs[policy][name].add(event['r'], event['p'], event['b'][policy])
    result = {}
    for policy in aggs:
        for name in aggs[policy]:
            agg_result = aggs[policy][name].get()
            for metric in agg_result:
                result[(policy, name, metric)] = agg_result[metric]
    return result

def aggregate(predictions, agg_factories, window, rolling=False):
    if not rolling:
        if isinstance(window, int):
            for batch_id, batch in enumerate(grouper(predictions, window)):
                agg = _aggregate_batch(batch, agg_factories)
                agg['i'] = batch_id * window
                yield agg
        else:
            ...
    else:
        ...