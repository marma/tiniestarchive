from json import loads
from io import BufferedIOBase, BytesIO
from typing import Iterable, Union
from urllib.parse import urljoin
from uuid import UUID
from requests import Session

from tiniestarchive import Archive,Instance,Resource,FileInstance,PRESERVATION, WORM, READ, WRITE, READ_BINARY
from tiniestarchive.commitmanager import CommitManager
from tiniestarchive.utils import chunker

class HttpResource(Resource):
    def __init__(self, url, archive=None, auth = None, mode=READ):
        self.root_url = url
        self.archive = archive
        self.auth = auth
        self.mode = mode
        self.session = Session()

    def serialize(self) -> BytesIO:
        r = self._get(urljoin(self.root_url, '_serialize'), stream=True)
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
        return self.open(path, mode=mode).read()

    def update(self, instance : Instance):
        files = { f: instance.open(f) for f in instance }
        r = self._post(urljoin(self.root_url, '_add'), files=files)

    # TODO serialize instance instead of just POSTing the data directory
    #def update(self, instance : Instance):
    #    self._post(urljoin(self.root_url, '_update'), data=chunker(instance.serialize()))

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

    def _post(self, url, params={}, headers={}, files=None, data=data, stream=False):
        return self.session.post(
                url,
                auth=self.auth, 
                params=params,
                headers=headers,
                files=files,
                data=data,
                stream=stream)

    def __repr__(self):
        return f"<HttpResource({self.instance_id}) @ {hex(id(self))}>"


class HttpArchive(Archive):
    def __init__(self, url):
        self.root_url = url

    def new(self) -> HttpResource:
        raise Exception('Not implemented')
    
    def serialize(self, resource_id: str) -> BytesIO:
        raise Exception('Not implemented')

    def open(self, resource_id: str, filename : str, instance_id : str = None, mode='r') -> BufferedIOBase:
        if mode not in [ READ, READ_BINARY ]:
            raise Exception(f"Invalid mode: '{mode}'")

        return self._resolve(resource_id, instance_id = instance_id, filename = filename).open(mode)

    def read(self, resource_id: str, filename : str, mode='r') -> Union[str,bytes]:
        with self.open(resource_id, filename, mode=mode) as f:
            return f.read()

    def exists(self, resource_id : str) -> bool:
        return self._resolve(resource_id).exists()

    def events(self, start=None, listen=False) -> Iterable:
        ...

    def __iter__(self):
        ...

    def __repr__(self):
        return f"<HttpArchive({self.instance_id}) @ {hex(id(self))}>"
