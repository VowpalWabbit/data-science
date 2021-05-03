import cb.estimators as cb
import pandas as pd

class cb_estimator_old:
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
        slots = self.slots if len(self.slots) > 0 else range(len(r))
        for s in slots:
            self._impl.add(r[s], p_log[s], p[s], n)

    def __add__(self, other):
        if self.slots != other.slots or type(self) != type(other):
            raise Exception('Estimator type mismatch')

        return cb_estimator_old(self._impl + other._impl, self.slots)

    def name(self):
        return self.__name

    def save(self):
        return self._impl.save()

    def load(self, o):
        self._impl.load(o)

    def get(self, *args, **kwargs):
        return self._impl.get(*args, **kwargs)

class cb_estimator:
    def __init__(self, impl):
        self._impl = impl
        self._slots = {}

    def add(self, r, p_log, p, n = 1):
        for s in range(len(r)):
            self._slots.setdefault(s, self._impl()).add(r[s], p_log[s], p[s], n)

    def __add__(self, other):
        result = cb_estimator(self._impl)
        (l, s) = (self, other) if len(self._slots) > len(other._slots) else (other, self)
        for k, v in l._slots.items():
            result._slots[k] = result._impl()
            result._slots[k] += v

        for k, v in s._slots.items():
            result._slots[k] += v

        return result

    def get(self, weights = None, slots = None, *args, **kwargs):
        if weights is not None:
            result = 0
            for s, e in self._slots.items():
                result += weights(s, e) * e.get(*args, **kwargs)
            return result
        if slots is not  None:
            if len(slots) == 0:
                slots = self._slots.keys()
            result = self._impl()
            for s in slots:
                result += self._slots[s]
            return result.get(*args, **kwargs) 
        raise 'Not Supported'
