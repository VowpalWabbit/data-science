from pathlib import Path

from vw_executor import loggers
from vw_executor.vw_opts import VwOpts


class VwCache:
    def __init__(self, path: str):
        self.path = path
        Path(self.path).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _file_name(string_hash: str) -> str:
        import hashlib
        return hashlib.md5(string_hash.encode('utf-8')).hexdigest()

    def _get_path(self, context: str, args_hash: str) -> str:
        folder_name = Path(self.path).joinpath(context)
        Path(folder_name).mkdir(parents=True, exist_ok=True)
        return Path(context).joinpath(VwCache._file_name(args_hash))

    def get_path(self, opts: VwOpts, output: str = None, salt: str = None, logger=loggers.ConsoleLogger()) -> str:
        args_hash = VwOpts(dict(opts, **{'-#': salt})).hash()
        result = self._get_path(f'cache{output}', args_hash)
        logger.debug(f'Generating path for opts: {str(opts)}, output: {output}. Result: {result}')
        return result
