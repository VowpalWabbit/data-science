import hashlib
import pandas as pd

from typing import Dict, Union, Any, List, Iterable

VwOptsLike = Union[str, Dict[str, Any]]


class VwOpts(dict):
    def __init__(self, opts: VwOptsLike):
        if isinstance(opts, str):
            opts = {'#0': opts}
        super().__init__(opts)

    def __str__(self) -> str:
        not_none = {k: v for k, v in self.items() if v is not None and not pd.isnull(v)}
        return ' '.join(['{0} {1}'.format(str(key).strip(), str(value).strip()) if not key.startswith('#')
                        else str(value) for key, value in not_none.items()])

    def __eq__(self, other) -> bool:
        return self.hash() == other.hash()

    def __hash__(self) -> int:     
        return int(self.hash(), 16)

    def hash(self) -> str:
        items = [i.strip() for i in f' {str(self)}'.split(' -')]
        return hashlib.md5(' '.join(sorted(items)).strip().encode('utf-8')).hexdigest()   

    def to_cache_cmd(self) -> str:
        import argparse
        parser = argparse.ArgumentParser(add_help=False)

        parser.add_argument('-b', '--bit_precision', type=int)

        parser.add_argument('--ccb_explore_adf', action='store_true')
        parser.add_argument('--cb_explore_adf', action='store_true')
        parser.add_argument('--cb_adf', action='store_true')
        parser.add_argument('--slates', action='store_true')

        parser.add_argument('--json', action='store_true')
        parser.add_argument('--dsjson', action='store_true')
        parser.add_argument('--cats', action='store_true')

        parser.add_argument('--compressed', action='store_true')
        namespace, _ = parser.parse_known_args(str(self).split())
        result = ''
        if namespace.cb_adf:
            result = result + '--cb_adf '
        if namespace.cb_explore_adf:
            result = result + '--cb_explore_adf '
        if namespace.ccb_explore_adf:
            result = result + '--ccb_explore_adf '
        if namespace.slates:
            result = result + '--slates '
        if namespace.json:
            result = result + '--json '
        if namespace.dsjson:
            result = result + '--dsjson '
        if namespace.compressed:
            result = result + '--compressed '
        if namespace.bit_precision:
            result = result + f'-b {namespace.bit_precision} '
        if namespace.cats:
            result = result + f'--cats 1 --bandwidth 1 --min_value 0 --max_value 1 '

        return result.strip()


GridLike = List[VwOptsLike]


class Grid(list):
    def __init__(self, grid: Union[Iterable[VwOptsLike], Dict[str, Iterable[Any]]]):
        if isinstance(grid, dict):
            super().__init__(product(*[dimension(k, v) for k, v in grid.items()]))
        else:
            super().__init__({VwOpts(o) for o in grid})

    def __mul__(self, other: 'Grid') -> 'Grid':
        return Grid(product(self, other))

    def __add__(self, other: 'Grid') -> 'Grid':
        return Grid(list(self) + list(other))     


class InteractiveGrid(dict):
    def __init__(self, grid: Dict[str, Any]):
        if not isinstance(grid, dict):
            raise Exception('not supported')
        super().__init__(grid)

    def __mul__(self, other: Dict[str, Any]):
        return dict(self, **other)

    def __add__(self, other: Dict[str, Any]):
        raise Exception('not supported')  
       

def _dim_to_list(d: Union[pd.DataFrame, Iterable[VwOptsLike]]) -> GridLike:
    if isinstance(d, pd.DataFrame):
        return Grid(d.loc[:, ~d.columns.str.startswith('!')].to_dict('index').values())
    else:
        return Grid(d)


def product(*dimensions: Union[Iterable[VwOptsLike], pd.DataFrame]) -> Grid:
    import functools
    import itertools
    result = functools.reduce(
        lambda d1, d2: map(
            lambda t: dict(t[0], **t[1]),
            itertools.product(_dim_to_list(d1), _dim_to_list(d2))
        ), dimensions)
    return Grid(result)


def dimension(name: str, values: List[Any]) -> Grid:
    return Grid([{name: v} for v in values])
