from pathlib import Path

from vw_executor.loggers import MultiLogger
from vw_executor.vw_opts import VwOpts

from typing import Optional


class VwCache:
    path: Path

    def __init__(self, path: Path):
        self.path = path
        self.path.mkdir(parents=True, exist_ok=True)

    def _get_path(self, context: str, args_hash: str) -> Path:
        folder = self.path.joinpath(context)
        folder.mkdir(parents=True, exist_ok=True)
        return Path(context).joinpath(args_hash)

    def get_path(self,
                 opts: VwOpts,
                 logger: MultiLogger,
                 output: Optional[str] = None,
                 salt: Optional[str] = None) -> Path:
        args_hash = VwOpts(dict(opts, **{'-#': salt})).hash()
        result = self._get_path(f'cache{output}', args_hash)
        logger.debug(f'Generating path for opts: {str(opts)}, output: {output}. Result: {result}')
        return result
