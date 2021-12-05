import pandas as pd


def _safe_to_float(num: str, default):
    try:
        return float(num)
    except (ValueError, TypeError):
        return default


def _to(value: str, types: list):
    for t in types:
        try:
            return t(value)
        except (ValueError, TypeError):
            ...
    return value


# Helper function to extract example counters and metrics from VW output.
# Counter lines are preceded by a single line containing the text:
#   loss     last          counter         weight    label  predict features
# and followed by a blank line
# Metric lines have the following form:
# metric_name = metric_value

def _parse_loss(loss_str):
    if loss_str.strip()[-1] == 'h':
        loss_str = loss_str.strip()[:-1]
    return _safe_to_float(loss_str, None)


def _extract_metrics(out_lines):
    loss_table = {'i': [], 'loss': [], 'since_last': []}
    metrics = {}
    try:
        record = False
        for line in out_lines:
            line = line.strip()
            if record:
                if line == '':
                    record = False
                else:
                    counter_line = line.split()
                    try:
                        count, average_loss, since_last = counter_line[2], counter_line[0], counter_line[1]
                        average_loss_f = float(average_loss)
                        since_last_f = float(since_last)
                        loss_table['i'].append(count)
                        loss_table['loss'].append(average_loss_f)
                        loss_table['since_last'].append(since_last_f)
                    except (ValueError, TypeError):
                        ...  # todo: handle
            elif line.startswith('loss'):
                fields = line.split()
                if fields[0] == 'loss' and fields[1] == 'last' and fields[2] == 'counter':
                    record = True
            elif '=' in line:
                key_value = [p.strip() for p in line.split('=')]
                if key_value[0] == 'average loss':
                    metrics[key_value[0]] = _parse_loss(key_value[1])
                else:
                    metrics[key_value[0]] = _to(key_value[1], [int, float])
    finally:
        return pd.DataFrame(loss_table).set_index('i'), metrics


class Artifact:
    def __init__(self, path):
        self.path = path

    @property
    def raw(self):
        with open(self.path, 'r') as f:
            return f.readlines()


class Output(Artifact):
    def __init__(self, path):
        super().__init__(path)
        self._processed = False
        self._loss = None
        self._loss_table = None
        self._metrics = None

    def _process(self):
        self._processed = True
        self._loss_table, self._metrics = _extract_metrics(self.raw)
        if 'average loss' in self._metrics:
            self._loss = self._metrics['average loss']

    @property
    def loss(self):
        if not self._processed:
            self._process()
        return self._loss

    @property
    def loss_table(self):
        if not self._processed:
            self._process()
        return self._loss_table

    @property
    def metrics(self):
        if not self._processed:
            self._process()
        return self._metrics

class Predictions(Artifact):
    def __init__(self, path):
        super().__init__(path)

    @property
    def cb(self):
        result = []
        for i, l in enumerate(self.raw):
            l = l.strip()
            if len(l) == 0:
                continue
            result.append(dict({kv.split(':')[0]: float(kv.split(':')[1]) 
                for kv in l.split(',')}, **{'i': i}))
        return pd.DataFrame(result).set_index('i')

    @property
    def ccb(self):
        result = []
        session = 0
        slot = 0
        for l in self.raw:
            l = l.strip()
            if len(l) == 0:
                slot = 0
                session += 1
                continue
            result.append(dict({kv.split(':')[0]: float(kv.split(':')[1]) 
                for kv in l.split(',')}, **{'session': session, 'slot': slot}))
            slot += 1
        return pd.DataFrame(result).set_index(['session', 'slot'])

    @property
    def regression(self):
        with open(self.path) as f:
            return pd.DataFrame([{'i': i, 'y': float(l.strip())}for i, l in enumerate(f)]).set_index('i')
