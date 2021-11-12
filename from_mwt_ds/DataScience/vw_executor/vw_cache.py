from pathlib import Path

from vw_executor import loggers
from vw_executor.vw_opts import VwOpts


class VwCache:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)

    def _get_path(self, context: str, args_hash: str) -> str:
        folder = self.path.joinpath(context)
        folder.mkdir(parents=True, exist_ok=True)
        return Path(context).joinpath(args_hash)

    def get_path(self, opts: VwOpts, output: str = None, salt: str = None, logger=loggers.ConsoleLogger()) -> str:
        args_hash = VwOpts(dict(opts, **{'-#': salt})).hash()
        result = self._get_path(f'cache{output}', args_hash)
        logger.debug(f'Generating path for opts: {str(opts)}, output: {output}. Result: {result}')
        return result
