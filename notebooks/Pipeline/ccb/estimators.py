import Pipeline.cb.estimators as cb
from itertools import zip_longest
import pandas as pd

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

class cb_estimator:
    def __init__(self, impl, slots=[]):
        self._impl = impl
        self.slots = set(slots)

    @property
    def slots(self):
        return self.__slots

    @slots.setter
    def slots(self, slots):
        self.__slots = set(slots)
        self.__name = f'ccb|{self._impl.name()}|{",".join([str(s) for s in self.slots])}'

    def add(self, r, p_log, p, n = 1):
        slots = self.slots if any(self.slots) else range(len(r))
        for s in slots:
            self._impl.add(r[s], p_log[s], p[s], n)

    def __add__(self, other):
        if self.slots != other.slots or type(self) != type(other):
            raise Exception('Estimator type mismatch')

        return cb_estimator(self._impl + other._impl, self.slots, )

    def name(self):
        return self.__name

    def save(self):
        return self._impl.save()

    def load(self, o):
        self._impl.load(o)

    def get(self, *args, **kwargs):
        return self._impl.get(*args, **kwargs)   

def create(name, desc=None):
    parts = name.split('|')
    result = None
    if parts[0] != 'ccb':
        raise Exception(f'Unknown estimator: {name}') 
    slots = set([int(s) for s in parts[2].split(',')])
    if parts[1] == 'ips':
        result = cb_estimator(cb.ips(), slots)
    elif parts[1] == 'snips':
        result = cb_estimator(cb.snips(), slots)
    elif parts[1] == 'ips_snips':
        result = cb_estimator(cb.ips_snips(), slots)
    else:
        raise Exception(f'Unknown estimator: {name}')  
    if desc:
        result.load(desc)
    return result

def estimate(predictions, est_factories, window, rolling=False):
    if not rolling:
        if isinstance(window, int):
            for batch_id, batch in enumerate(grouper(predictions, window)):
                agg = _aggregate_batch(batch, est_factories)
                agg['i'] = batch_id * window
                yield agg
        else:
            raise Exception('not supported')
    else:
        raise Exception('not supported')
