import pandas as pd

def _serialize_estimator_table(df):
    result = []
    columns = df.columns
    for i, l in df.iterrows():
        row = {}
        for c in columns:
            row[c] = l[c].save()
        result.append(row)
    result = pd.DataFrame(result)
    result.index = df.index
    return result

def evaluate(df):
    result = []
    from ast import literal_eval as make_tuple
    df.columns = [make_tuple(c) for c in df.columns]
    columns = df.columns
    for i, l in df.iterrows():
        row = {}
        for c in columns:
            est = l[c].get()
            for k,v in est.items():
                row[c + (k,)] = v
        result.append(row)
    result = pd.DataFrame(result)
    result.index = df.index
    return result

class Estimator:
    def __init__(self, factory, estimators, online_estimator = None):
        self.factory = factory
        self.estimators = estimators
        self.online_estimator = online_estimator

    
    def _estimate(self, prediction): # estimators: map from policy to list of estimator description
        result = {'t': pd.to_datetime(prediction['i']['t'])}
        if self.online_estimator:
            result[('online', self.online_estimator)] = self.factory(self.online_estimator)
            result[('online', self.online_estimator)].add(prediction['r'], prediction['p'], prediction['p'], prediction['n'])

        for p in self.estimators:
            for e in self.estimators[p]:
                result[(p,e)]=self.factory(e)

        for p in prediction['b']:
            for e in self.estimators[p]:
                result[(p,e)].add(prediction['r'], prediction['p'], prediction['b'][p], prediction['n'])
        
        return result

    def _estimate_df(self, prediction, baseline_columns): # estimators: map from policy to list of estimator description
        result = {'t': pd.to_datetime(prediction['t'])}
        if self.online_estimator:
            result[('online', self.online_estimator)] = self.factory(self.online_estimator)
            result[('online', self.online_estimator)].add(prediction['r'], prediction['p'], prediction['p'], prediction['n'])

        for p in baseline_columns:
            for e in self.estimators[p[1]]:
                result[p + (e,)]=self.factory(e)
                result[p + (e,)].add(prediction['r'], prediction['p'], prediction[p], prediction['n'])
        
        return result

    def preestimate(self, predictions, window):
        if isinstance(window, str):
            df = pd.DataFrame(map(lambda p: self._estimate(p), predictions)).set_index('t')
            df.columns = [str(c) for c in df.columns]
            return _serialize_estimator_table(df.resample(window).sum())
        else:
            raise Exception('not supported')

    def preestimate_df(self, predictions_df, window):
        if isinstance(window, str):
            baselines = [c for c in predictions_df.columns if isinstance(c, tuple) and c[0]=='b']
            result = pd.DataFrame(map(lambda i_p: self._estimate_df(i_p[1], baselines), predictions_df.iterrows())).set_index('t')
            result.columns = [str(c) for c in result.columns]
            result.resample(window).sum()
            return result
        else:
            raise Exception('not supported')

    def read_preestimate(self, path):
        from ast import literal_eval as make_tuple
        df = pd.read_csv(path, parse_dates=['t']).set_index('t')
        df.columns = [make_tuple(c) for c in df.columns]

        result = []
        columns = df.columns
        for i, l in df.iterrows():
            row = {}
            for c in columns:
                row[c] = self.factory(c[1], l[c])
            result.append(row)
        result = pd.DataFrame(result)
        result.index = df.index
        result.columns = [str(c) for c in result.columns]
        return result

