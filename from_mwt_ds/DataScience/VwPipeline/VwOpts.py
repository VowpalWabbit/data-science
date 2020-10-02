def string_hash(cmd: str) -> str:
    items = [i.strip() for i in f' {cmd}'.split(' -')]
    items.sort()
    return ' '.join(items).strip()

def to_string(opts: dict) -> str:
    return ' '.join(['{0} {1}'.format(key, opts[key]) if not key.startswith('#')
        else str(opts[key]) for key in sorted(opts.keys())])

def product(*dimensions: list) -> list:
    import functools
    import itertools
    result = functools.reduce(
        lambda d1, d2: map(
            lambda tuple: dict(tuple[0], **tuple[1]),
            itertools.product(d1, d2)
        ), dimensions)
    return list(result)

def dimension(name: str, values: list) -> list:
    return list(map(lambda v: dict([(name, str(v))]), values))

def to_cache_cmd(opts: dict) -> str:
    import argparse
    parser = argparse.ArgumentParser(add_help=False)
    
    parser.add_argument('-b', '--bit_precision', type=int)
    
    parser.add_argument('--ccb_explore_adf', action='store_true')
    parser.add_argument('--cb_explore_adf', action='store_true')
    parser.add_argument('--cb_adf', action='store_true')
    parser.add_argument('--slates', action='store_true')
    
    parser.add_argument('--json', action='store_true')
    parser.add_argument('--dsjson', action='store_true')
    
    parser.add_argument('--compressed', action='store_true')

    namespace, _ = parser.parse_known_args(to_string(opts).split())
    result = ''
    if namespace.cb_adf: result = result + '--cb_adf '
    if namespace.cb_explore_adf: result = result + '--cb_explore_adf '
    if namespace.ccb_explore_adf: result = result + '--ccb_explore_adf '
    if namespace.slates: result = result + '--slates '
    if namespace.json: result = result + '--json '  
    if namespace.dsjson: result = result + '--dsjson '   
    if namespace.compressed: result = result + '--compressed '     
    if namespace.bit_precision: result = result + f'-b {namespace.bit_precision} '

    return result.strip() 
