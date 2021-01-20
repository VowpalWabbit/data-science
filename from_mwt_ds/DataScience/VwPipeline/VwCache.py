import os

from VwPipeline import Loggers


class VwCache:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(self.path, exist_ok=True)

    @staticmethod
    def _file_name(string_hash: str) -> str:
        import hashlib
        return hashlib.md5(string_hash.encode('utf-8')).hexdigest()

    def _get_path(self, context: str, args_hash: str) -> str:
        folder_name = os.path.join(self.path, context)
        os.makedirs(folder_name, exist_ok=True)
        return os.path.join(context, VwCache._file_name(args_hash))

    def get_rel_path(self, opts: dict, output: str = None, salt: str = None, logger=Loggers.ConsoleLogger()) -> str:
        from VwPipeline import VwOpts
        opts_str = VwOpts.to_string(opts)
        if salt:
            opts_str = opts_str + f' -# {salt}'
        args_hash = VwOpts.string_hash(opts_str)
        result = self._get_path(f'cache{output}', args_hash)
        logger.debug(f'Generating path for opts: {VwOpts.to_string(opts)}, output: {output}. Result: {result}')
        return result

    def get_path(self, opts: dict, output: str = None, salt: str = None, logger=Loggers.ConsoleLogger()) -> str:
        return os.path.join(self.path, self.get_rel_path(opts, output, salt, logger))
