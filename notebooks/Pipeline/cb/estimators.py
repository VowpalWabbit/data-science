import json

class ips_snips:
    def __init__(self, r = 0, n = 0, n_ips = 0):
        self.r = r
        self.n = n
        self.n_ips = n_ips

    def add(self, r, p_log, p, n = 1):
        self.r += n * r * p / p_log
        self.n += n
        self.n_ips += n * p / p_log

    def __add__(self, other):
        r = self.r + other.r
        n = self.n + other.n
        n_ips = self.n_ips + other.n_ips
        return ips_snips(r, n, n_ips)

    def __str__(self):
        return f'r: {self.r}, n: {self.n}, n_ips: {self.n_ips}'

    def name(self):
        return 'ips_snips'

    def get(self, type):
        if type == 'ips':
            return {'e': 0 if self.n == 0 else self.r / self.n}
        elif type == 'snips':
            return {'e': 0 if self.n == 0 else self.r / self.n_ips}

class ips:
    def __init__(self, r = 0, n = 0):
        self.r = r
        self.n = n

    def add(self, r, p_log, p, n = 1):
        self.r += n * r * p / p_log
        self.n += n

    def __add__(self, other):
        r = self.r + other.r
        n = self.n + other.n
        return ips(r, n)

    def __str__(self):
        return f'num: {self.r}, denum: {self.n}'

    def name(self):
        return 'ips'

    def save(self):
        return json.dumps({'r' : self.r, 'n': self.n})

    def load(self, desc):
        o = json.loads(desc)
        self.r = o['r']
        self.n = o['n']   

    def get(self):
        return {'e': 0 if self.n == 0 else self.r / self.n}

class snips:
    name='snips'

    def __init__(self, r = 0, n = 0):
        self.r = r
        self.n = n

    def add(self, r, p_log, p, n = 1):
        self.r += n * r * p / p_log
        self.n += n * p / p_log

    def name(self):
        return 'snips'

    def __add__(self, other):
        r = self.r + other.r
        n = self.n + other.n
        return snips(r, n)

    def __str__(self):
        return f'num: {self.r}, denum: {self.n}'

    def save(self):
        return json.dumps({'r' : self.r, 'n': self.n}).replace(',', '|')

    def load(self, desc):
        o = json.loads(desc.replace('|', ','))
        self.r = o['r']
        self.n = o['n']       

    def get(self):
        return {'e': 0 if self.n == 0 else self.r / self.n}

def create(name, desc = None):
    result = None
    if name == 'ips':
        result = ips()
    elif name == 'snips':
        result = snips()
    else:
        raise Exception('Unknown estimator')
    if desc:
        result.load(desc)
    return result