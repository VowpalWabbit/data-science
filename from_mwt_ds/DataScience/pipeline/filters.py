class UniformSampler:
    counter = 0
    
    def __init__(self, fraction):
        self.period = int(1/fraction)
        
    def do(self, line):
        self.counter = (self.counter + 1) % self.period
        return self.counter == 0