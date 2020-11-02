import os

from VwPipeline import Logger

class VwCache:
    def __init__(self, path: str, logger = None):
        self.Path = path
        self.Logger = logger
        os.makedirs(self.Path, exist_ok=True)

    @staticmethod
    def __file_name__(string_hash: str) -> str:
        import hashlib
        return hashlib.md5(string_hash.encode('utf-8')).hexdigest()

    def __get_path__(self, context: str, args_hash: str) -> str:
        folder_name = os.path.join(self.Path, context)
        os.makedirs(folder_name, exist_ok=True)
        return os.path.join(folder_name, VwCache.__file_name__(args_hash))

    def get_path(self, opts_in: dict, opt_out: str = None, salt: str = None) -> str:
        from VwPipeline import VwOpts
        opts = VwOpts.to_string(opts_in)
        if salt:
            opts = opts + f' -# {salt}'
        args_hash = VwOpts.string_hash(opts)
        result = self.__get_path__(f'cache{opt_out}', args_hash)
        Logger.debug(self.Logger, f'Generating path for opts_in: {VwOpts.to_string(opts_in)}, opt_out: {opt_out}. Result: {result}')
        return result
