from pathlib import Path

class Mapper:
    def __init__(self, src, dst):
        self._src = Path(src)
        self._dst = Path(dst)
        if not self._src.is_absolute() or not self._dst.is_absolute():
            raise Exception("Only absolute paths are supported")
            
    def __call__(self, path):
        p = Path(path)
        if not self._src in p.parents: 
            raise Exception(f'{path} is not from {self._src}')
        return self._dst.joinpath(p.relative_to(self._src))