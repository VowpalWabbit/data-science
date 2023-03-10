import pandas as pd
from pathlib import Path
from typing import Optional, Tuple, Dict, Union, List, Any, Generator, Dict
import json


def _safe_to_float(num: str, default: Optional[float]) -> Optional[float]:
    try:
        return float(num)
    except (ValueError, TypeError):
        return default

def _safe_to_int(num: str, default: Optional[int]) -> Optional[int]:
    try:
        return int(num)
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
    def cb(self) -> Generator[Dict, None, None]:
        with open(self.path) as f:
            for line in f:
                line = line.strip()
                if len(line) == 0:
                    continue
                yield {kv.split(':')[0]: _safe_to_float(kv.split(':')[1], None)
                                    for kv in line.split(',')}

    @property
    def ccb_slot(self) -> Generator[Dict, None, None]:
        session = 0
        slot = 0
        with open(self.path) as f:
            for line in f:
                line = line.strip()
                if len(line) == 0:
                    slot = 0
                    session += 1
                    continue
                yield dict({kv.split(':')[0]: _safe_to_float(kv.split(':')[1], None)
                                    for kv in line.split(',')}, **{'session': session, 'slot': slot})
                slot += 1

    @property
    def slates_slot(self) -> Generator[Dict, None, None]:
        return self.ccb_slot

    @property
    def scalar(self) -> Generator[Dict, None, None]:
        with open(self.path) as f:
            for line in f:
                yield {'y': _safe_to_float(line.strip(), None)}

    @property
    def cats(self) -> Generator[Dict, None, None]:
        with open(self.path) as f:
            for line in f:
                line = line.strip()
                if len(line) == 0:
                    continue
                action, prob = line.split(',')
                yield {'action': _safe_to_float(action, None), 'prob': _safe_to_float(prob, None)}

    @property
    def csoaa_ldf(self) -> Generator[Dict, None, None]:
        result = []
        index = 0
        label = 0
        i = 0
        for line in self.raw:
            line = line.strip()
            if len(line) == 0:
                yield {'index': index, 'label': label}
                index = 0
                label = 0
                i = 0
                continue
            else:
                value = _safe_to_int(line, 0)
                if value:
                    index = i
                    label = value
                i += 1      


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

    @staticmethod
    def parse_invert_hash(s: str, weight: Dict):
        def get_ns(s: str) -> str:
            split = s.split('^')
            return "^".join(split[:-1]), split[-1]

        interacted = s.split('*')
        
        if len(interacted) == 1:
            weight["ft_type"].append("feature")
            weight["ns1"].append(None)
            weight["ns2"].append(None)
            weight["ns3"].append(None)
        elif len(interacted) == 2:
            weight["ft_type"].append("quadratic")
            weight["ns1"].append(get_ns(interacted[0]))
            weight["ns2"].append(get_ns(interacted[1]))
            weight["ns3"].append(None)
        elif len(interacted) == 3:
            weight["ft_type"].append("cubic")
            weight["ns1"].append(get_ns(interacted[0]))
            weight["ns2"].append(get_ns(interacted[1]))
            weight["ns3"].append(get_ns(interacted[2]))

    @property
    def weights(self) -> pd.DataFrame:
        result = {'name': [], 'weight': [], 'ft_type': [], 'ns1': [], 'ns2': [], 'ns3': []}
        for line in reversed(self.raw):
            if ':' not in line:
                break

            parts = line.split(' ')[0].split(':')
            result['name'].append(parts[0])
            result['weight'].append(_safe_to_float(parts[-1], None))
            Model9.parse_invert_hash(parts[0], result)
        df = pd.DataFrame(result)
        df.set_index('name', inplace=True)
        df.sort_index(inplace=True)
        return df


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
