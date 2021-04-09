import itertools
import json
import uuid
import pandas as pd
from collections import ChainMap

def _is_decision(line):
    return not line.startswith('{"RewardValue"')

def _get_or(obj, field, default):
    return default if field not in obj else obj[field]
            
class Processor:
    default_top_processor = lambda o: {
        'Timestamp': o['Timestamp'],
        '_skipLearn': _get_or(o, '_skipLearn', False),
        'pdrop': _get_or(o, 'pdrop', 0.0)}
    
    default_slot_processor = lambda o: {
        '_inc': _get_or(o, '_inc', [])}
    
    default_outcome_processor = lambda o: {
        '_label_cost': o['_label_cost'],
        '_id': o['_id'],
        '_a': o['_a'],
        '_p': o['_p']} 
    
    processors = {
        '/': [default_top_processor],
        'c': [],
        '_multi': [],
        '_slots': [default_slot_processor],
        '_outcomes': [default_outcome_processor]
    }

    filters = [_is_decision]

    def __init__(self, processors = None, filters = None):
        self.processors = processors if processors is not None else self.processors
        self.filters = filters if filters is not None else self.filters

    def _process_decision(self, line):
        parsed = json.loads(line)
        top = dict(ChainMap(*[p(parsed) for p in self.processors['/']]))
        shared = dict(ChainMap(*[p(parsed['c']) for p in self.processors['c']]))

        actions = [None] * len(parsed['c']['_multi'])
        for i, o in enumerate(parsed['c']['_multi']):
            actions[i] = dict(ChainMap(*[p(o) for p in self.processors['_multi']]))

        slots = [None] * len(parsed['c']['_slots'])
        for i, o in enumerate(parsed['c']['_slots']):
            slots[i] = dict(ChainMap(*[p(o) for p in self.processors['_slots']]))
        
        outcomes = [None] * len(parsed['_outcomes'])
        for i, o in enumerate(parsed['_outcomes']):
            outcomes[i] = dict(ChainMap(*[p(o) for p in self.processors['_outcomes']]))
        
        result = dict(ChainMap(top, {'c': dict(ChainMap(shared, {'_multi': actions, '_slots': slots})), '_outcomes': outcomes}))
        return json.dumps(result)

    def process(self, lines):
        for f in self.filters:
            lines = filter(lambda l: f(l), lines)
        if len(self.processors) > 1:
            return map(lambda l: f'{self._process_decision(l)}\n', lines) 
        return lines


class Predictor:   
    indexers = {
        't': lambda o: o['Timestamp']
    }
    
    baselines = {
        'random': lambda obj: [1 / len(o['_a']) for o in obj['_outcomes']],
        'baseline1_old': lambda obj: [o['_p'][0] * int(o['_a'][0] == i) for i, o in enumerate(obj['_outcomes'])]
    }

    def __init__(self, filters = None, indexers = None, baselines = None):
        self.filters = filters if filters is not None else self.filters
        self.indexers = indexers if indexers is not None else self.indexers
        self.baselines = baselines if baselines is not None else self.baselines

    def _decision_2_prediction(self, line):
        parsed = json.loads(line)
        result = {
            'i': {},
            'a': [],
            'r': [],
            'p': [],
            'n': 1 if 'pdrop' not in parsed else 1 / (1 - parsed['pdrop']),
            'b': {}
        }
        for i, o in enumerate(parsed['_outcomes']):
            result['a'].append(o['_a'][0])         
            result['p'].append(o['_p'][0])
            result['r'].append(-o['_label_cost'])

        for indexer_name, indexer_func in self.indexers.items():
            result['i'][indexer_name] = indexer_func(parsed)

        for baseline_name, baseline_func in self.baselines.items():
            result['b'][baseline_name] = baseline_func(parsed)

        return json.dumps(result)

    def predict(self, lines):
        for f in self.filters:
            lines = filter(lambda l: f(l), lines)
        return map(lambda l: f'{self._decision_2_prediction(l)}\n', lines) 

class VwPredictionsCcb:
    @staticmethod
    def line_2_slot(line):
        return {p.split(':')[0] : float(p.split(':')[1])  for p in line.split(',')}

    @staticmethod
    def lines_2_slots(lines):
        return map(VwPredictionsCcb.line_2_slot, filter(lambda l : not l.isspace(), lines))

    @staticmethod
    def files_2_slots(files):
        return itertools.chain.from_iterable(map(lambda f: VwPredictionsCcb.lines_2_slots(open(f)), files))