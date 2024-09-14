from io import BufferedIOBase
from urllib.parse import urljoin

class HttpInstance:
    def __init__(self, url, mode='r'):
        self.root_url = url
        self.mode = mode

    def serialize(self) -> Iterable[bytes]:
        raise Exception('Not implemented')

    def open(self, path, mode='r') -> BufferedIOBase:
        if mode not in [ 'r', 'rb' ]:
            raise Exception(f"Invalid mode: {mode}")

        return open(self._resolve(path), mode)

    def read(self, path, mode='r') -> str | bytes:
        ...

    def add(self, path : str, filename : str = None, data : bytes|str = None, fobj = None, checksum : str = None):
        ...

    def finalize(self):
        ...

    def commit(self):
        ...

    def __repr__(self):
        return f"<HttpInstance({self.instance_id}) @ {hex(id(self))}>"


class HttpArchive:
    def __init__(self, url):
        self.root_url = url

    def get(self, instance_id: str, mode : str = 'r') -> HttpInstance:
        url = urljoin(self.root_url, instance_id)

        return HttpInstance(url, mode=mode)

    def new(self, mode : str = 't') -> HttpInstance:
        raise Exception('Not implemented')
    
    def meta(self, instance_id: str) -> dict:
        return loads(self._resolve(instance_id).joinpath('_meta.json').read_text())

    def serialize(self, instance_id: str) -> Iterable[bytes]:
        raise Exception('Not implemented')

    def deserialize(self, data: Instance|bytes|Iterable[bytes] = None, instance_id: str = None, ):
        raise Exception('Not implemented')

    def open(self, instance_id: str, filename : str, mode='r') -> BufferedIOBase:
        ...

    def read(self, instance_id: str, filename : str, mode='r') -> str|bytes:
        ...

    def files(self, instance_id : str) -> list[str]:
        ...

    def events(self, start=None, listen=False) -> Iterable:
        ...

    def __repr__(self):
        return f"<HttpArchive({self.instance_id}) @ {hex(id(self))}>"
