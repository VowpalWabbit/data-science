# What is this?
Python helper for running vowpalwabbit on multiple command line arguments and have programmatic access to results.

# Getting started
## Initialize the wrapper
```
from vw_executor.vw import Vw
vw = Vw(
    'path to cache folder',
    'path to vw binary'         # Optional. If empty, pyvw is used
    )
```

## Run on single configuration
```
result = vw.train(INPUTS, CONFIGURATION)
```
where

INPUTS - list of paths to input files


### CONFIGURATION

Every configuration can be defined using one of the following ways:
1. string. I.e.
```
result = vw.train(['input1.txt'], '--cb_explore_adf --dsjson')
```
2. dictionary. In this case, for every key/value pair:

- If value is None/NaN the whole pair is skipped
- If key starts with '#', key is skipped
- Otherwise f'{key} {value}' is added to command line

For example,
```
vw.train(['input1.txt'], {'#base': '--cb_explore_adf --dsjson', '--epsilon': 0.2, '--learning_rate': None})
```
is running vw binary with the following command line:
```
--cb_explore_adf --dsjson --epsilon 0.2
```

## Run on multiple configurations
```
result = vw.train(INPUTS, CONFIGURATIONS)
```
CONFIGURATIONS is the list of CONFIGURATION here. It is recommended to use helper vw_opts.Grid class in order to create it.
Grid can be constructed using one of the following ways:
1. From Iterable of CONFIGURATION. For example,
```
from vw_executor.vw_opts import Grid
vw.train(['input1.txt'], Grid(['--cb_explore_adf --dsjson -l 0.1', '--cb_explore_adf --dsjson -l 0.1']))
```

2. From dictionary with string keys and iterable values.
    1. Grid({'-k': ['v1', 'v2']}) is equivalent to Grid(['-k v1', '-k v2'])
    2. Grid({'-k1': [v1, v2], '-k2': [v3, v4]}) is equivalent to Grid(['-k1 v1 k2 v3', '-k1 v1 k2 v4', '-k1 v2 k2 v3', '-k1 v2 k2 v4'])

Grid supports multiplication("and") and summation("or") operators.
For example,
```
Grid({
    '-k1': ['v1, v2']
}) * (Grid({'-k2': ['v3']}) + Grid({'-k3': ['v4']}))
```
is equivalent to
```
Grid(['-k1 v1 -k2 v3', '-k1 v1 -k3 v4', '-k1 v2 -k2 v3', '-k1 v2 -k3 v4'])
```
