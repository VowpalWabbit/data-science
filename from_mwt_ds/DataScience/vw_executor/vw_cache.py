from pathlib import Path

from vw_executor.loggers import MultiLogger
from vw_executor.vw_opts import VwOptsLike, VwOpts

from typing import Optional, Union


class VwCache:
    path: Path

    def __init__(self, path: Union[str, Path], version):
        self.path = Path(path)
        self.impl = None
        if version == 1:
            self.impl = self._impl1
        elif version == 2:
            self.path = self.path.joinpath('2')
            self.impl = self._impl2
        self.path.mkdir(parents=True, exist_ok=True)

    def _impl1(self, context: str, args_hash: str) -> Path:
        folder = self.path.joinpath(context)
        folder.mkdir(parents=True, exist_ok=True)
        return Path(context).joinpath(args_hash)

    def _impl2(self, context: str, args_hash: str) -> Path:
        result = Path(context).joinpath(args_hash)
        self.path.joinpath(result).mkdir(parents=True, exist_ok=True)
        return result.joinpath('default')

    def get_path(self,
                 opts: VwOptsLike,
                 logger: MultiLogger,
                 output: Optional[str] = None,
                 salt: Optional[int] = None) -> Path:
        args_hash = VwOpts(dict(opts, **{'-#': salt})).hash()
        result = self.impl(f'cache{output}', args_hash)
        logger.debug(f'Generating path for opts: {str(opts)}, output: {output}. Result: {result}')
        return result

