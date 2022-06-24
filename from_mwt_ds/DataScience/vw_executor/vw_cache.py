from pathlib import Path

from vw_executor.loggers import MultiLogger
from vw_executor.vw_opts import VwOptsLike, VwOpts
from vw_executor.version import Version

from typing import Optional, Union

from abc import ABC, abstractmethod, abstractproperty

class VwCache(ABC):
    @abstractmethod
    def get_path(self,
                 opts: VwOptsLike,
                 logger: MultiLogger,
                 version: Version,
                 output: Optional[str] = None,
                 salt: Optional[int] = None):
        ...

class _VwCache1(VwCache):
    path: Path

    def __init__(self, path: Union[str, Path]):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)

    def _get_path(self, context: str, args_hash: str) -> Path:
        folder = self.path.joinpath(context)
        folder.mkdir(parents=True, exist_ok=True)
        return Path(context).joinpath(args_hash)

    def get_path(self,
                 opts: VwOptsLike,
                 logger: MultiLogger,
                 version: Version,
                 output: Optional[str] = None,
                 salt: Optional[int] = None) -> Path:
        args_hash = VwOpts(dict(opts, **{'-#': salt})).hash()
        result = self._get_path(f'cache{output}', args_hash)
        logger.debug(f'Generating path for opts: {str(opts)}, output: {output}. Result: {result}')
        return result

class _VwCache2(VwCache):
    path: Path

    def __init__(self, path: Union[str, Path]):
        self.path = Path(path).joinpath('2')
        self.path.mkdir(parents=True, exist_ok=True)

    def _get_path(self, context: str, args_hash: str, version: Version) -> Path:
        op_folder = Path(context).joinpath(args_hash)
        full_path = self.path.joinpath(op_folder)
        full_path.mkdir(parents=True, exist_ok=True)
        match = next(full_path.glob(version.pattern), None)
        if match:
            return match.relative_to(self.path)
        return op_folder.joinpath(str(version))

    def get_path(self,
                 opts: VwOptsLike,
                 logger: MultiLogger,
                 version: Version,
                 output: Optional[str] = None,
                 salt: Optional[int] = None) -> Path:
        args_hash = VwOpts(dict(opts, **{'-#': salt})).hash()
        result = self._get_path(f'cache{output}', args_hash, version)
        logger.debug(f'Generating path for opts: {str(opts)}, output: {output}. Result: {result}')
        return result

def create_cache(path: Union[str, Path], version: int):
    if version == 1:
        return _VwCache1(path)
    elif version == 2:
        return _VwCache2(path)
    return None
