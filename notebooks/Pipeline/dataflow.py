import json
import pandas as pd

from itertools import chain
from pathlib import Path

from Pipeline.progress import dummy_progress

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
    
    def lines_2_lines(self, 
        files,
        processor,
        path_gen=None,
        process=False,
        progress=dummy_progress()):
        path_gen = path_gen or (lambda f: f'{f}.{processor.__name__}') 
        result = []
        progress.on_start(len(files))
        for path_in in files:
            path_out = path_gen(path_in)
            Path(path_out).parent.mkdir(parents=True, exist_ok=True)
            if process or not self._is_in_sync(path_in, path_out):
                with open(path_out, 'w') as fout:
                    with open(path_in) as fin:
                        fout.writelines(processor(fin))
                self._sync(path_in, path_out)
            if Path(path_out).exists():
                result.append(path_out)
            progress.on_step()
        progress.on_finish() 
        return result

    def lines_2_csv(self,
        files,
        processor,
        path_gen=None,
        process=False,
        progress=dummy_progress()):
        path_gen = path_gen or (lambda f: f'{f}.{processor.__name__}') 
        result = []
        progress.on_start(len(files))
        for path_in in files:
            path_out = path_gen(path_in)
            Path(path_out).parent.mkdir(parents=True, exist_ok=True)
            if process or not self._is_in_sync(path_in, path_out):
                df = processor(open(path_in))
                df.to_csv(path_out, index=False)
                self._sync(path_in, path_out)
            if Path(path_out).exists():
                result.append(path_out)
            progress.on_step()
        progress.on_finish() 
        return result
