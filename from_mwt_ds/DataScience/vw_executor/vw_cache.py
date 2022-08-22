from pathlib import Path

from vw_executor.loggers import MultiLogger
from vw_executor.vw_opts import VwOptsLike, VwOpts
from vw_executor.version import Version

from typing import Optional, Union

from abc import ABC, abstractmethod, abstractproperty

class VwCache(ABC):
    def __init__(self, path: Union[str, Path]):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)

    def _get_path(self, context: str, args_hash: str) -> Path:
        return Path(context).joinpath(args_hash)

    def get_path_for_hash(self,
                 opts: VwOptsLike,
                 logger: MultiLogger,
                 output: Optional[str] = None,
                 salt: Optional[int] = None) -> Path:
        args_hash = VwOpts(dict(opts, **{'-#': salt})).hash()
        result = self._get_path(f'cache{output}', args_hash)
        logger.debug(f'Generating path for opts: {str(opts)}, output: {output}. Result: {result}')
        return result

    @abstractmethod
    def get_path(self,
                 path_for_hash: Path,
                 version: Version,
                 logger: MultiLogger):
        ...

class _VwCache1(VwCache):
    path: Path

    def __init__(self, path: Union[str, Path]):
        super().__init__(path)

    def get_path(self, path_for_hash: Path, _: Version):
        full_path = self.path.joinpath(path_for_hash)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        return full_path



class _VwCache2(VwCache):
    path: Path

    def __init__(self, path: Union[str, Path]):
        super().__init__(Path(path).joinpath('v2'))      

    def get_path(self, path_for_hash: Path, version: Version):
        full_path = self.path.joinpath(path_for_hash)
        full_path.mkdir(parents=True, exist_ok=True)
        match = next(full_path.glob(version.pattern), None)
        return match or full_path.joinpath(str(version))

    def get_model_path(self, path_for_hash: Path, instance: str):

def create_cache(path: Union[str, Path], version: int):
    if version == 1:
        return _VwCache1(path)
    elif version == 2:
        return _VwCache2(path)
    return None
