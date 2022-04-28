import pandas as pd
from pathlib import Path
from typing import Optional, Tuple, Dict, Union, List, Any
import json


def _safe_to_float(num: str, default: Optional[float]) -> Optional[float]:
    try:
        return float(num)
    except (ValueError, TypeError):
        return default


def _to(value: str, types: list) -> Any:
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

def _parse_loss(loss_str: str) -> Optional[float]:
    if loss_str.strip()[-1] == 'h':
        loss_str = loss_str.strip()[:-1]
    return _safe_to_float(loss_str, None)


def _extract_metrics(out_lines) -> Tuple[pd.DataFrame, Dict[str, Optional[Union[str, int, float]]]]:
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
    path: Path

    def __init__(self, path: Union[str, Path]):
        self.path = Path(path)

    @property
    def raw(self) -> List[str]:
        with open(self.path, 'r') as f:
            return f.readlines()


class Output(Artifact):
    _processed: bool
    _loss: Optional[float]
    _loss_table: Optional[pd.DataFrame]
    _metrics: Optional[Dict[str, Any]]

    def __init__(self, path: Union[str, Path]):
        super().__init__(path)
        self._processed = False
        self._loss = None
        self._loss_table = None
        self._metrics = None

    def _process(self) -> None:
        self._processed = True
        self._loss_table, self._metrics = _extract_metrics(self.raw)
        if 'average loss' in self._metrics:
            self._loss = self._metrics['average loss']

    @property
    def loss(self) -> Optional[float]:
        if not self._processed:
            self._process()
        return self._loss

    @property
    def loss_table(self) -> Optional[pd.DataFrame]:
        if not self._processed:
            self._process()
        return self._loss_table

    @property
    def metrics(self) -> Optional[Dict[str, Any]]:
        if not self._processed:
            self._process()
        return self._metrics


class Predictions(Artifact):
    def __init__(self, path: Union[str, Path]):
        super().__init__(path)

    @property
    def cb(self) -> pd.DataFrame:
        result = []
        for line in self.raw:
            line = line.strip()
            if len(line) == 0:
                continue
            result.append(dict({kv.split(':')[0]: _safe_to_float(kv.split(':')[1], None)
                                for kv in line.split(',')}))
        result = pd.DataFrame(result)
        result.index.name = 'i'
        return result

    @property
    def ccb(self) -> pd.DataFrame:
        result = []
        session = 0
        slot = 0
        for line in self.raw:
            line = line.strip()
            if len(line) == 0:
                slot = 0
                session += 1
                continue
            result.append(dict({kv.split(':')[0]: _safe_to_float(kv.split(':')[1], None)
                                for kv in line.split(',')}, **{'session': session, 'slot': slot}))
            slot += 1
        return pd.DataFrame(result).set_index(['session', 'slot'])

    @property
    def scalar(self) -> pd.DataFrame:
        with open(self.path) as f:
            return pd.DataFrame([{'i': i, 'y': _safe_to_float(l.strip(), None)} for i, l in enumerate(f)]).set_index(
                'i')

    @property
    def cats(self) -> pd.DataFrame:
        result = {'action': [], 'prob': []}
        for line in self.raw:
            line = line.strip()
            if len(line) == 0:
                continue
            action, prob = line.split(',')
            result['action'].append(_safe_to_float(action, None))
            result['prob'].append(_safe_to_float(prob, None))
        result = pd.DataFrame(result)
        result.index.name = 'i'
        return result


class Model8(Artifact):
    def __init__(self, path: Union[str, Path]):
        super().__init__(path)

    @property
    def weights(self) -> pd.DataFrame:
        result = {'name': [], 'weight': []}
        weights = False
        for line in self.raw:
            if weights:
                parts = line.split(':')
                result['name'].append(parts[0])
                result['weight'].append(_safe_to_float(parts[-1], None))
            if line.strip() == ':0':
                weights = True
        return pd.DataFrame(result).set_index('name')


class Model9(Artifact):
    def __init__(self, path: Union[str, Path]):
        super().__init__(path)

    @property
    def weights(self) -> pd.DataFrame:
        result = {'name': [], 'weight': []}
        for line in reversed(self.raw):
            if ':' not in line:
                break

            parts = line.split(' ')[0].split(':')
            result['name'].append(parts[0])
            result['weight'].append(_safe_to_float(parts[-1], None))
        return pd.DataFrame(result).set_index('name')


class Model(Artifact):
    def __init__(self, path: Union[str, Path]):
        super().__init__(path)

    @property
    def weights(self) -> pd.DataFrame:
        def flatten_terms(terms):
            return "*".join([f"{term['namespace']}^{term['name']}" for term in terms]) if terms else None
        with open(self.path) as f:
            weight_rows = json.load(f)["weights"]
        return pd.DataFrame([dict({
            "name": flatten_terms(x.get("terms", None)),
            "index": x["index"],
            "value": x["value"]},
            **x.get("gd_extra_online_state", {})) for x in weight_rows])    
