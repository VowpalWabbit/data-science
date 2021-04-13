class VwPredictionsCcb:
    @staticmethod
    def line_2_slot(line):
        return {p.split(':')[0] : float(p.split(':')[1])  for p in line.split(',')}

    @staticmethod
    def lines_2_slots(lines):
        return map(VwPredictionsCcb.line_2_slot, filter(lambda l : not l.isspace(), lines))

    @staticmethod
    def files_2_slots(files):
        return itertools.chain.from_iterable(map(lambda f: VwPredictionsCcb.lines_2_slots(open(f)), files))