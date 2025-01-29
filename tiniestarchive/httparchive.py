from json import loads
from io import BufferedIOBase
from typing import Iterable, Union
from urllib.parse import urljoin
from uuid import UUID
from requests import Session

from tiniestarchive import Archive,Instance,Resource,FileInstance,PRESERVATION, WORM, READ, WRITE
from tiniestarchive.commitmanager import CommitManager

class HttpResource(Resource):
    def __init__(self, url, auth = None, mode=READ):
        self.root_url = url
        self.auth = auth
        self.mode = mode
        self.session = Session()

    def serialize(self) -> BufferedIOBase:
        r = self._get(self.root_url, stream=True)
        r.raw.decode_stream = True

        return r.raw

    def open(self, path, mode=READ) -> BufferedIOBase:
        if mode not in [ 'r', 'rb' ]:
            raise Exception(f"Invalid mode: {mode}")

        url = self._resolve(path)
        r = self._get(url, stream=True)
        r.raw.decode_stream = True

        return r.raw

    def read(self, path, mode='r') -> Union[str, bytes]:
        return self.open(path).read()

    def update(self, instance : Instance):
        # TODO deserialize instance instead of just POSTing the data directory
        files = { f: instance.open(f) for f in instance }
        r = self._post(urljoin(self.root_url, '_add'), files=files)

    def transaction(self):
        return CommitManager(
                    self,
                    lambda x: FileInstance(x, mode=WRITE),
                    finalize=self.archive.operation_mode in [ WORM, PRESERVATION ] if self.archive else False)

    def _resolve(self, path):
        return urljoin(self.root_url, path)

    def _get(self, url, params={}, headers={}, stream=False):
        return self.session.get(url,
                auth=self.auth, 
                params=params,
                headers=headers,
                stream=stream)

    def _post(self, url, params={}, headers={}, files=None, stream=False):
        return self.session.get(
                url,
                auth=self.auth, 
                params=params,
                headers=headers,
                files=files,
                stream=stream)

    def __repr__(self):
        return f"<HttpResource({self.instance_id}) @ {hex(id(self))}>"


class HttpArchive(Archive):
    def __init__(self, url):
        self.root_url = url

    def new(self, mode : str = 't') -> HttpResource:
        raise Exception('Not implemented')
    
    def open(self, resource_id: UUID, filename : str, mode=READ) -> BufferedIOBase:
        ...

    def read(self, resource_id: str, filename : str, mode=READ) -> Union[str,bytes]:
        ...

    def events(self, start=None, listen=False) -> Iterable:
        ...

    def __iter__(self):
        ...

    def __repr__(self):
        return f"<HttpArchive({self.instance_id}) @ {hex(id(self))}>"
