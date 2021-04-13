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

class ips:
    slots = None
    def __init__(self, slots = None, r = 0, n = 0):
        self.slots = set(slots) if slots else None
        self.r = 0
        self.n = 0

    def add(self, r, p_log, p, n = 1):
        slots = self.slots if self.slots else range(len(r))
        for s in slots:
            self.r += n * r[s] * p[s] / p_log[s]
            self.n += n

    def __add__(self, other):
        if self.slots != other.slots:
            raise Exception('cannot aggregate ips over different set of slots')

        x = self.r + other.r
        y = self.n + other.n
        return Point(x, y)

    def get(self):
        return {'e': 0 if self.n == 0 else self.r / self.n}

class snips:
    slots = None
    def __init__(self, slots = None):
        self.slots = slots
        self.r = 0
        self.n = 0

    def add(self, r, p_log, p, n = 1):
        slots = self.slots if self.slots else range(len(r))
        for s in slots:
            self.r += n * r[s] * p[s] / p_log[s]
            self.n += n * p[s] / p_log[s]

    def get(self):
        return {'e': 0 if self.n == 0 else self.r / self.n}

def estimate(predictions, agg_factories, window, rolling=False):
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

