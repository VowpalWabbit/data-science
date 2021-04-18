import Pipeline.ccb.dsjson.filters as filters
import json
import pandas as pd

class Predictor: 
    filters = [filters.is_decision]
    
    baselines = {
        'random': lambda obj: [1 / len(o['_a']) for o in obj['_outcomes']],
        'baseline1_old': lambda obj: [o['_p'][0] * int(o['_a'][0] == i) for i, o in enumerate(obj['_outcomes'])]
    }

    def __init__(self, filters = None, baselines = None):
        self.filters = filters if filters is not None else self.filters
        self.baselines = baselines if baselines is not None else self.baselines

    def _decision_2_prediction(self, line):
        parsed = json.loads(line)
        result = {
            't': [],
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

        result['t'].append(o['Timestamp'])

        for baseline_name, baseline_func in self.baselines.items():
            result['b'][baseline_name] = baseline_func(parsed)

        return json.dumps(result)

    def _decision_2_prediction_df(self, line, result):
        parsed = json.loads(line)
        a = []
        p = []
        r = []
        for i, o in enumerate(parsed['_outcomes']):
            a.append(o['_a'][0])         
            p.append(o['_p'][0])
            r.append(-o['_label_cost'])
        result['a'].append(a)         
        result['p'].append(p)
        result['r'].append(r)

        result['t'].append(pd.to_datetime(parsed['Timestamp']))
        result['n'].append(1 if 'pdrop' not in parsed else 1 / (1 - parsed['pdrop']))

        for baseline_name, baseline_func in self.baselines.items():
            result[('b', baseline_name)].append(baseline_func(parsed))

    def predict(self, lines):
        for f in self.filters:
            lines = filter(lambda l: f(l), lines)
        return map(lambda l: f'{self._decision_2_prediction(l)}\n', lines) 

    def predict_df(self, lines):
        for f in self.filters:
            lines = filter(lambda l: f(l), lines)
        result = {'t': [], 'a': [], 'p': [], 'r': [], 'n': []}
        for baseline_name in self.baselines:
            result[('b', baseline_name)]=[] 
        for l in lines:
            self._decision_2_prediction_df(l, result)   
        return pd.DataFrame(result)