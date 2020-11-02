import sys
import subprocess
import re
import json
import os
import pandas as pd

from VwPipeline.Pool import SeqPool, MultiThreadPool
from VwPipeline import VwOpts
from VwPipeline import Logger

import multiprocessing

def __safe_to_float__(str, default):
    try:
        return float(str)
    except (ValueError, TypeError):
        return default

# Helper function to extract example counters and metrics from VW output.
# Counter lines are preceeded by a single line containing the text:
#   loss     last          counter         weight    label  predict features
# and followed by a blank line
# Metric lines have the following form:
# metric_name = metric_value
def __extract_metrics__(out_lines):
    average_loss_dict = {}
    since_last_dict = {}
    metrics = {}
    record = False
    for line in out_lines:
        line = line.strip()
        if record:
            if line == '':
                record = False
            else:
                counter_line = line.split()
                count, average_loss, since_last = counter_line[2], counter_line[0], counter_line[1]
                average_loss_dict[count] = average_loss
                since_last_dict[count] = since_last
        elif line.startswith('loss'):
                fields = line.split()
                if fields[0] == 'loss' and fields[1] == 'last' and fields[2] == 'counter':
                    record = True    
        elif '=' in line:
            key_value = [p.strip() for p in line.split('=')]
            metrics[key_value[0]] = key_value[1]
    return average_loss_dict, since_last_dict, metrics

def __parse_vw_output__(txt):
    average_loss, since_last, metrics = __extract_metrics__(txt.split('\n'))
    loss = None
    if 'average loss' in metrics:
        # Include the final loss as the primary metric
        loss = __safe_to_float__(metrics['average loss'], None)

    success = loss is not None
    return {'loss_per_example': average_loss, 'since_last': since_last, 'metrics': metrics, 'loss': loss}, success

def __metrics_table__(metrics, name):
    return pd.DataFrame([{'n': int(k), name: float(metrics[name][k])}
                         for k in metrics[name]]).set_index('n')

def metrics_table(metrics):
    return pd.concat([__metrics_table__(m, 'loss_per_example').join(__metrics_table__(m, 'since_last')).assign(file=i)
                      for i, m in enumerate(metrics)]).reset_index().set_index(['file', 'n'])

def final_metrics_table(metrics):
    return [m['metrics'] for m in metrics]


def __save__(txt, path):
    with open(path, 'w') as f:
        f.write(txt)

def __load__(path):
    with open(path, 'r') as f:
        return f.read()

class VwInput:
    @staticmethod
    def cache(opts, i):
        return {'--cache_file': i, **opts}

    @staticmethod
    def raw(opts, i):
        return {'-d': i, **opts}

class VwResult:
    def __init__(self, loss, populated, metrics):
        self.Loss = loss
        self.Populated = populated
        self.Metrics = metrics

class Vw:
    def __init__(self, path, cache, procs=multiprocessing.cpu_count(), reset=False, norun=False, dedup_on_size = False):
        self.Path = path
        self.Cache = cache
        self.Logger = self.Cache.Logger
        self.Pool = SeqPool() if procs == 1 else MultiThreadPool(procs)
        self.Reset = reset
        self.NoRun = norun
        self.DedupOnSize = dedup_on_size

    def __generate_command_line__(self, opts):
        return f'{self.Path} {VwOpts.to_string(opts)}'

    def __run__(self, opts: dict):
        command = self.__generate_command_line__(opts)
        Logger.debug(self.Logger, f'Executing: {command}')
        process = subprocess.Popen(
            command.split(),
            universal_newlines=True,
            encoding='utf-8',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        error = process.communicate()[1]
        return error

    def __get_salt__(self, path):
        return str(os.stat(path).st_size) if self.DedupOnSize else None

    def run(self, opts_in: dict, opts_out: list, salt = None):
        populated = {o: self.Cache.get_path(opts_in, o, salt) for o in opts_out}
        metrics_path = self.Cache.get_path(opts_in, salt)

        result_files = list(populated.values()) + [metrics_path]
        not_exist = next((p for p in result_files if not os.path.exists(p)), None)

        opts = dict(opts_in, **populated)

        if self.Reset or not_exist:
            if not_exist:
                Logger.debug(self.Logger, f'{not_exist} had not been found.')
            if self.NoRun:
                raise 'Result is not found, and execution is deprecated'  

            result = self.__run__(opts)
            __save__(result, metrics_path)                          
        else:
            Logger.debug(self.Logger, f'Result of vw execution is found: {VwOpts.to_string(opts)}')
        raw_result = __load__(metrics_path)
        Logger.debug(self.Logger, raw_result)        
        parsed, success = __parse_vw_output__(raw_result)
        if not success:
            Logger.critical(self.Logger, f'ERROR: {json.dumps(opts)}')
            Logger.critical(self.Logger, raw_result)
            raise Exception('Unsuccesful vw execution')
        return parsed, populated

    def __test__(self, inputs, opts_in, opts_out, input_mode):
        opts_populated = [None] * len(inputs)
        metrics = [None] * len(inputs)
        for index, inp in enumerate(inputs):
            Logger.info(self.Logger, f'Vw.Test: {inp}, opts_in: {json.dumps(opts_in)}, opts_out: {json.dumps(opts_out)}')
            current_opts = input_mode(opts_in, inp)
            salt = self.__get_salt__(inp)
            result, populated = self.run(current_opts, opts_out, salt)
            opts_populated[index] = populated
            metrics[index] = result
        return VwResult(result['loss'], opts_populated, metrics)

    def __test_on_dict__(self, inputs, opts_in, opts_out, input_mode=VwInput.raw):
        if not isinstance(inputs, list):
            inputs = [inputs]
        if isinstance(opts_in, list):
            args = [(inputs, point, opts_out, input_mode) for point in opts_in]
            return self.Pool.map(self.__test__, args)            
        return self.__test__(inputs, opts_in, opts_out, input_mode)

    def __train__(self, inputs, opts_in, opts_out, input_mode=VwInput.raw):
        if '-f' not in opts_out:
            opts_out.append('-f')
        opts_populated = [None] * len(inputs)
        metrics = [None] * len(inputs)
        for index, inp in enumerate(inputs):
            Logger.info(self.Logger, f'Vw.Train: {inp}, opts_in: {json.dumps(opts_in)}, opts_out: {json.dumps(opts_out)}')
            current_opts = input_mode(opts_in, inp)
            salt = self.__get_salt__(inp)
            if index > 0:
                current_opts['-i'] = opts_populated[index - 1]['-f']
            result, populated = self.run(current_opts, opts_out, salt)
            opts_populated[index] = populated
            metrics[index] = result
        return VwResult(result['loss'], opts_populated, metrics)     

    def __train_on_dict__(self, inputs, opts_in, opts_out=[], input_mode=VwInput.raw):
        if not isinstance(inputs, list):
            inputs = [inputs]
        if isinstance(opts_in, list):
            args = [(inputs, point, opts_out, input_mode) for point in opts_in]
            return self.Pool.map(self.__train__, args)            
        return self.__train__(inputs, opts_in, opts_out, input_mode)   

    def cache(self, inputs, opts):
        if not isinstance(inputs, list):
            inputs = [inputs]
        return self.test(inputs, {'#cmd': VwOpts.to_cache_cmd(opts)}, ['--cache_file'])

    def train(self, inputs, opts_in, opts_out=[], input_mode=VwInput.raw):
        if isinstance(opts_in, pd.DataFrame):
            opts_in = list(opts_in.loc[:, ~opts_in.columns.str.startswith('!')].to_dict('index').values())
            result = zip(opts_in, self.__train_on_dict__(inputs, opts_in, opts_out, input_mode))
            result_pd = []
            for r in result:
                results = {'!Loss': r[1].Loss, '!Populated': r[1].Populated, '!Metrics': metrics_table(r[1].Metrics), '!FinalMetrics': final_metrics_table(r[1].Metrics)}
                result_pd.append(dict(r[0], **results))
            return pd.DataFrame(result_pd)
        else:
            return self.__train_on_dict__(inputs, opts_in, opts_out, input_mode)

    def test(self, inputs, opts_in, opts_out=[], input_mode=VwInput.raw):
        if isinstance(opts_in, pd.DataFrame):
            opts_in = list(opts_in.loc[:, ~opts_in.columns.str.startswith('!')].to_dict('index').values())
            result = zip(opts_in, self.__test_on_dict__(inputs, opts_in, opts_out, input_mode))
            result_pd = []
            for r in result:
                results = {'!Loss': r[1].Loss, '!Populated': r[1].Populated, '!Metrics': metrics_table(r[1].Metrics), '!FinalMetrics': final_metrics_table(r[1].Metrics)}
                result_pd.append(dict(r[0], **results))
            return pd.DataFrame(result_pd)
        else:
            return self.__test_on_dict__(inputs, opts_in, opts_out, input_mode)
        
