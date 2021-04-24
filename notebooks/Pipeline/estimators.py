import pandas as pd

class Estimator:
    def __init__(self, factory, estimators, online_estimator = None, window='5min'):
        self.factory = factory
        self.estimators = estimators
        self.online_estimator = online_estimator
        self.window = window

    def _estimate(self, prediction, baseline_columns): # estimators: map from policy to list of estimator description
        result = {'t': pd.to_datetime(prediction['t'])}
        if self.online_estimator:
            result[('online', self.online_estimator)] = self.factory(self.online_estimator)
            result[('online', self.online_estimator)].add(prediction['r'], prediction['p'], prediction['p'], prediction['n'])

        for p in baseline_columns:
            for e in self.estimators[p]:
                result[(p,) + (e,)]=self.factory(e)
                result[(p,) + (e,)].add(prediction['r'], prediction['p'], prediction[('b', p)], prediction['n'])
        
        return result

    def preestimate(self, predictions_df):
        if isinstance(self.window, str):
            baselines = [c[1] for c in predictions_df.columns if isinstance(c, tuple) and c[0]=='b']
            result = pd.DataFrame(map(lambda i_p: self._estimate(i_p[1], baselines), predictions_df.iterrows())).set_index('t')
            result.columns = [str(c) for c in result.columns]
            result.resample(self.window).sum()
            return result
        else:
            raise Exception('not supported')

