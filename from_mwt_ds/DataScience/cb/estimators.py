class ips_snips:
    def __init__(self, r = 0, n = 0, n_ips = 0, r2 = 0):
        self.r = r
        self.n = n
        self.n_ips = n_ips

    def add(self, r, p_log, p, n = 1):  
        num = r * p / p_log
        self.r += n * num
        self.n += n
        self.n_ips += n * p / p_log

    def __add__(self, other):
        r = self.r + other.r
        n = self.n + other.n
        n_ips = self.n_ips + other.n_ips
        return ips_snips(r, n, n_ips)

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
