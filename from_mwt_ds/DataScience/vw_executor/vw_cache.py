import os

from vw_executor import loggers


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

    def get_path(self, opts: dict, output: str = None, salt: str = None, logger=loggers.ConsoleLogger()) -> str:
        from vw_executor import vw_opts
        opts_str = vw_opts.to_string(opts)
        if salt:
            opts_str = opts_str + f' -# {salt}'
        args_hash = vw_opts.string_hash(opts_str)
        result = self._get_path(f'cache{output}', args_hash)
        logger.debug(f'Generating path for opts: {vw_opts.to_string(opts)}, output: {output}. Result: {result}')
        return result
