import scipy.stats
import math

class ips_snips_old:
    def __init__(self, r = 0, n = 0, n_ips = 0, r2 = 0):
        self.r = r
        self.n = n
        self.n_ips = n_ips
        self.r2 = r2

    def add(self, r, p_log, p, n = 1):
        num = r * p / p_log
        self.r += n * num
        self.n += n * int(p > 0)
        self.n_ips += n * p / p_log
        self.r2 += n * num**2

    def __add__(self, other):
        r = self.r + other.r
        n = self.n + other.n
        r2 = self.r2 + other.r2
        n_ips = self.n_ips + other.n_ips
        return ips_snips_old(r, n, n_ips, r2)

    def __str__(self):
        return f'r: {self.r}, n: {self.n}, n_ips: {self.n_ips}'

    def name(self):
        return 'ips_snips'

    def get_ips(self):
        return 0 if self.n == 0 else self.r / self.n

    def get_snips(self):
        return 0 if self.n == 0 else self.r / self.n_ips

    def get(self, type, alpha=0.05):
        if type == 'ips':
            return {'e': self.get_ips()}
        elif type == 'snips':
            return {'e': self.get_snips()}
        elif type == 'gaussian':
            ppf = scipy.stats.norm.ppf(1 - alpha/2)
            variance = (self.r2 - self.r**2 / self.n) / (self.n - 1)
            delta = ppf * math.sqrt(variance/self.n)


class ips_snips:
    def __init__(self, r = 0, n = 0, n_ips = 0, r2 = 0):
        self.r = r
        self.n = n
        self.n_ips = n_ips
        self.r2 = r2

    def add(self, r, p_log, p, n = 1):
        num = r * p / p_log
        self.r += n * num
        self.n += n * int(p > 0)
        self.n_ips += n * p / p_log
        self.r2 += n * num**2

    def __add__(self, other):
        r = self.r + other.r
        n = self.n + other.n
        r2 = self.r2 + other.r2
        n_ips = self.n_ips + other.n_ips
        return ips_snips(r, n, n_ips, r2)

    def get_ips(self):
        return 0 if self.n == 0 else self.r / self.n

    def get_snips(self):
        return 0 if self.n == 0 else self.r / self.n_ips

    def get(self, type):
        if type == 'ips':
            return self.get_ips()
        elif type == 'snips':
            return self.get_snips()
        raise Exception('Not supported')

    def get_interval(self, alpha=0.05):
        if self.n < 2:
            return 0, 0
        ppf = scipy.stats.norm.ppf(1 - alpha/2)
        variance = (self.r2 - self.r**2 / self.n) / (self.n - 1)
        delta = ppf * math.sqrt(variance/self.n)
        return self.get_ips() - delta, self.get_ips() + delta