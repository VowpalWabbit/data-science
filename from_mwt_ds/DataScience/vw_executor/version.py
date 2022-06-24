import re

def _extract_commit(s):
    result = re.search('.*\(git commit:\s+(\S+)\)', s)
    if result:
        return result.group(1)
    return None


class Version:
    def __init__(self, kind: str, version: str):
        self.kind = kind
        parts = [int(i) for i in version.split()[0].split('.')]
        assert len(parts) == 3
        self.major = parts[0]
        self.minor = parts[1]
        self.rev = parts[2]
        self.commit = _extract_commit(version)
        self.pattern = str(self)

    def __str__(self):
        return f'{self.kind}-{self.major}.{self.minor}.{self.rev}-{self.commit or ""}'