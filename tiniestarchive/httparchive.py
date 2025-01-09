from json import loads
from io import BufferedIOBase
from typing import Iterable, Union
from urllib.parse import urljoin
from . import Resource,Archive,Instance

class HttpResource(Resource):
    def __init__(self, url, mode='r'):
        self.root_url = url
        self.mode = mode

    def serialize(self) -> Iterable[bytes]:
        raise Exception('Not implemented')

    def open(self, path, mode='r') -> BufferedIOBase:
        if mode not in [ 'r', 'rb' ]:
            raise Exception(f"Invalid mode: {mode}")

        return open(self._resolve(path), mode)

    def read(self, path, mode='r') -> Union[str, bytes]:
        ...

    def add(self, path : str, filename : str = None, data : Union[bytes,str] = None, fobj = None, checksum : str = None):
        ...

    def finalize(self):
        ...

    def commit(self):
        ...

    def __repr__(self):
        return f"<HttpResource({self.instance_id}) @ {hex(id(self))}>"


class HttpArchive(Archive):
    def __init__(self, url):
        self.root_url = url

    def get(self, instance_id: str, mode : str = 'r') -> HttpResource:
        url = urljoin(self.root_url, instance_id)

        return HttpResource(url, mode=mode)

    def new(self, mode : str = 't') -> HttpResource:
        raise Exception('Not implemented')
    
    def meta(self, instance_id: str) -> dict:
        return loads(self._resolve(instance_id).joinpath('_meta.json').read_text())

    def serialize(self, instance_id: str) -> Iterable[bytes]:
        raise Exception('Not implemented')

    def deserialize(self, data: Union[Instance,bytes,Iterable[bytes]] = None, instance_id: str = None):
        raise Exception('Not implemented')

    def open(self, instance_id: str, filename : str, mode='r') -> BufferedIOBase:
        ...

    def read(self, instance_id: str, filename : str, mode='r') -> Union[str,bytes]:
        ...

    def files(self, instance_id : str) -> list[str]:
        ...

    def events(self, start=None, listen=False) -> Iterable:
        ...

    def __repr__(self):
        return f"<HttpArchive({self.instance_id}) @ {hex(id(self))}>"
