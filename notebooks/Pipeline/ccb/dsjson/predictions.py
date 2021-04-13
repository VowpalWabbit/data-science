import Pipeline.ccb.dsjson.filters as filters
import json

class Predictor: 
    filters = [filters.is_decision]

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