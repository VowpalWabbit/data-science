import json
import pandas as pd

from itertools import chain
from pathlib import Path

from Pipeline.progress import dummy_progress, tqdm_progress

class FileSizeHasher:
    extension = 'size'
    
    def evaluate(self, path):
        return Path(path).stat().st_size

class Execution:
    def __init__(self, reader, input, hasher, path_gen, processor, process):
        self.reader = reader
        self.input = input
        self.output = path_gen(input)
        self.hasher = hasher
        self.processor = processor
        self.process = process 

    def _is_in_sync(self):
        input_hash = self.hasher.evaluate(self.input)
        output_hash = self._load_hash(self.output)
        return input_hash and output_hash and input_hash == output_hash

    def _load_hash(self, path):
        hash_path = f'{path}.{self.hasher.extension}'
        if not Path(hash_path).exists():
            return None
        try:
            hash_value = int(open(hash_path, 'r').read())
        except:
            hash_value = None
        return hash_value

    def run(self):
        if self.process or not self._is_in_sync():
            Path(self.output).parent.mkdir(parents=True, exist_ok=True)
            return self.processor(self.reader(self.input))
        return None
            
    def close(self):
        return self.output if Path(self.output).exists() else None       

class Fileset:
    def __init__(self, files=[], reader=None, writer=None):
        self.files = files
        self.reader = reader
        self.writer = writer

    def init(self, executions, progress=tqdm_progress()):
        self.files = []
        progress.on_start(len(executions))
        for execution in executions:
            result = execution.run()
            if result is not None:
                self.writer(execution.output, result)
            self.files.append(execution.close())
            progress.on_step()
        progress.on_finish() 
        return self

    def process(self, processor, path_gen=None, process=False, hasher = FileSizeHasher()):
        path_gen = path_gen or (lambda f: f'{f}.{processor.__name__}') 
        result = []
        for path_in in self.files:
            result.append(Execution(self.reader, path_in, hasher, path_gen, processor, process))
        return result

class MultilineFiles(Fileset):
    @staticmethod
    def read(path):
        return open(path)

    @staticmethod
    def write(path, lines):
        with open(path, 'w') as f:
            f.writelines(lines)

    def __init__(self, files = []):
        super().__init__(files=files, reader=MultilineFiles.read, writer=MultilineFiles.write)  

class PickleFiles(Fileset):
    @staticmethod
    def read(path):
        return pd.read_pickle(path)

    @staticmethod
    def write(path, df):
        df.to_pickle(path)

    def __init__(self, files = []):
        super().__init__(files=files, reader=PickleFiles.read, writer=PickleFiles.write)  

    def open(self):
        return pd.concat([self.read(p) for p in self.files])

class CsvFiles(Fileset):
    @staticmethod
    def read(path):
        return pd.read_csv(path)

    @staticmethod
    def write(path, df):
        df.to_csv(path, index=False)

    def __init__(self, files = []):
        super().__init__(files=files, reader=CsvFiles.read, writer=CsvFiles.write) 

    def open(self):
        return pd.concat([self.read(p) for p in self.files])

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

    def ndjson_2_csv(self,
        files,
        processor,
        path_gen=None,
        process=False,
        progress=dummy_progress(),
        index=True,
        openers = None):
        path_gen = path_gen or (lambda f: f'{f}.{processor.__name__}') 
        result = []
        progress.on_start(len(files))
        for i, path_in in enumerate(files):
            opener = openers[i] if openers else lambda p: map(lambda l: json.loads(l), open(p))
            path_out = path_gen(path_in)
            Path(path_out).parent.mkdir(parents=True, exist_ok=True)
            if process or not self._is_in_sync(path_in, path_out):
                df = processor(opener(path_in))
                df.to_csv(path_out, index=index)
                self._sync(path_in, path_out)
            if Path(path_out).exists():
                result.append(path_out)
            progress.on_step()
        progress.on_finish() 
        return result

    def lines_2_df_pickle(self,
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
                df.to_pickle(path_out)
                self._sync(path_in, path_out)
            if Path(path_out).exists():
                result.append(path_out)
            progress.on_step()
        progress.on_finish() 
        return result

    def df_pickle_2_df_pickle(self,
        files,
        processor,
        path_gen=None,
        process=False,
        progress=dummy_progress(),
        index=True,
        openers = None):
        path_gen = path_gen or (lambda f: f'{f}.{processor.__name__}') 
        result = []
        progress.on_start(len(files))
        for i, path_in in enumerate(files):
            opener = openers[i] if openers else lambda p: pd.read_pickle(p)
            path_out = path_gen(path_in)
            Path(path_out).parent.mkdir(parents=True, exist_ok=True)
            if process or not self._is_in_sync(path_in, path_out):
                df = processor(opener(path_in))
                df.to_pickle(path_out)
                self._sync(path_in, path_out)
            if Path(path_out).exists():
                result.append(path_out)
            progress.on_step()
        progress.on_finish() 
        return result
