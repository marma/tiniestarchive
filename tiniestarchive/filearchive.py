from copy import copy
from io import BufferedIOBase
from json import dumps, load, loads
from os import makedirs
from os.path import join,exists
import shutil
from typing import Iterable
from uuid_utils import uuid7
from pathlib import Path
from time import time
from .utils import split_path, safe_path

SPECIAL_FILES = [ '_meta.json', '_files.json' ]

class EventLogger:
    def __init__(self, filename):
        self.filename = filename

    def log(self, ref : str, event : str):
        with self.filename.open('a') as f:
            f.write(f"{time()}\t{ref}\t{event}\n")

class FileInstance:
    def __init__(self, instance_id : str, path : str, mode='r', tmp_path : str = None, logger : EventLogger = None):
        if mode not in [ '1', 't', 'r', 'w', 'a']:
            raise Exception(f"Invalid mode: {mode}")

        if mode in [ '1', 't' ] and not tmp_path:
            raise Exception("Temporary path must be provided for mode '1' or 't'")

        if mode in [ 'r', 'a'] and not exists(path):
            raise Exception('Instance does not exist')

        if mode in [ '1', 'w', 't' ] and exists(path):
            raise Exception('Instance already exists')

        self.instance_id = instance_id
        self.path = tmp_path or path
        self.target_path = path if tmp_path else None
        self.mode = mode
        self.logger = logger

        if not exists(self.path):
            makedirs(join(self.path, 'data'))
            self.config = { "@id": f"urn:uuid:{instance_id}", "version": str(uuid7()), "status": "open" }
            self.files = []
            self._save()

            if mode == 'w':
                self.logger.log(self.instance_id, 'create')
        else:
            self._reload()
            if mode in [ 'a' ] and self.config['status'] == 'finalized':
                raise Exception('Instance is finalized')

    def serialize(self) -> Iterable[bytes]:
        raise Exception('Not implemented')

    def open(self, path, mode='r') -> BufferedIOBase:
        if mode not in [ 'r', 'rb' ]:
            raise Exception(f"Invalid mode: {mode}")

        return open(self._resolve(path), mode)

    def read(self, path, mode='r') -> str | bytes:  
        if mode not in [ 'r', 'rb' ]:
            raise Exception(f"Invalid mode: {mode}")

        with self.open(path, mode) as f:
            return f.read()       

    def add(self, path : str, filename : str = None, data : bytes|str = None, fobj = None, checksum : str = None):
        if not (filename or data or fobj):
            raise Exception('Either filename, data or a file-object must be provided')

        if self.mode == 'r':
            raise Exception('Adding files only allowed in writable modes')
        
        if self.config['status'] == 'finalized':
            raise Exception('Instance is finalized')

        # TODO: calculate / validate checksum

        if filename:
            shutil.copy(filename, self._resolve(path))
        elif data:
            with open(self._resolve(path), 'w' if isinstance(data, str) else 'wb') as f:
                f.write(data)
        elif fobj:
            with open(self._resolve(path), 'wb') as f:
                shutil.copyfileobj(fobj, f)

        if self.mode in [ 'w', 'a' ]:
            self.logger.log(f'{self.instance_id}/{path}', 'add')

        self.files.append(path)
        self._touch()

    def finalize(self):
        if self.config['status'] == 'finalized':
            raise Exception('Instance is already finalized')

        self.config['status'] = 'finalized'
        self.logger.log(self.instance_id, 'finalize')
        self._save()

    def commit(self):
        if self.mode not in [ '1', 't' ]:
            raise Exception('Commit only allowed in temporary or write-once mode')

        parent_dir = Path(self.target_path).parent
        parent_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(self.path, parent_dir)
        self.path = self.target_path

        self.logger.log(self.instance_id, 'create')
        for f in self.files:
            self.logger.log(f'{self.instance_id}/{f}', 'add')

        if self.mode == '1':
            self.finalize()

        self.mode = 'r'

    def _reload(self):
        with open(join(self.path, '_meta.json'), 'r') as f, open(join(self.path, '_files.json'), 'r') as g:
            self.config = load(f)
            self.files = load(g)

    def _save(self):
        with open(join(self.path, '_meta.json'), 'w') as f, open(join(self.path, '_files.json'), 'w') as g:
            f.write(dumps(self.config, indent=4))
            g.write(dumps(self.files, indent=4))

    def _touch(self):
        self.config['version'] = str(uuid7())
        self._save()

    def _resolve(self, path : str) -> str:
        return join(self.path, 'data', path)

    def __enter__(self):        
        return self
    
    def __exit__(self, type, value, traceback):
        if self.mode in [ '1', 't' ]:
            if type:
                shutil.rmtree(self.path)
            else:
                self.commit()

    def __repr__(self):
        return f"<Instance({self.instance_id}) @ {hex(id(self))}>"


class FileArchive:
    def __init__(self, path : str = None, mode='r'):
        path = Path(copy(path))

        if mode not in [ 'r', 'w' ]:
            raise Exception(f"Invalid mode: {mode}")

        if mode == 'r' and not path.exists():
            raise Exception(f'Archive ({path}) does not exist')

        if path.is_file():
            # assume this is a JSON config file
            self.config = loads(path.read_text())
            self.root_dir = Path(self.config['root_dir']).absolute()
        else:
            self.root_dir = path

        if mode == 'w' and not self.root_dir.exists():
            self.config = {}
            self.root_dir.mkdir(parents=True, exist_ok=True)
            self.root_dir.joinpath('config.json').write_text(dumps(self.config, indent=4))
            self.root_dir.joinpath('instances.txt').write_text('')
            self.root_dir.joinpath('log.txt').write_text('')
        else:
            self.config = loads(self.root_dir.joinpath('config.json').read_text())

        self.logger = EventLogger(self.root_dir.joinpath('log.txt'))

        self.mode = mode

    def meta(self, instance_id: str) -> dict:
        return loads(self._resolve(instance_id).joinpath('_meta.json').read_text())

    def get(self, instance_id: str, mode : str = 'r') -> FileInstance:
        if mode not in [ 'r', 'a' ]:
            raise Exception(f"Invalid mode: {mode}")
        
        if mode != 'r' and self.mode == 'r':
            raise Exception('Archive is in read-only mode')

        path = self._resolve(instance_id)

        return FileInstance(instance_id, path, mode, logger=self.logger)

    def new(self, mode : str = 't') -> FileInstance:
        if self.mode == 'r':
            raise Exception('Archive is in read-only mode')

        if mode not in [ '1', 't', 'w' ]:
            raise Exception(f"Invalid mode: {mode}")

        instance_id = self._new_id()
        path_segments = split_path(instance_id)

        tmp_path = None
        if mode in [ '1', 't' ]:
            tmp_path = self.root_dir.joinpath(path_segments[0], 'staging', instance_id)

        path = self.root_dir.joinpath(*path_segments)

        with self.root_dir.joinpath('instances.txt').open(mode='a') as f:
            f.write(f"{instance_id}\n")

        return FileInstance(instance_id, path=path, mode=mode, tmp_path=tmp_path, logger=self.logger)

    def serialize(self, instance_id: str) -> Iterable[bytes]:
        raise Exception('Not implemented')

    def deserialize(self, data: Instance|bytes|Iterable[bytes] = None, instance_id: str = None, ):
        raise Exception('Not implemented')

    def open(self, instance_id: str, filename : str, mode='r') -> BufferedIOBase:
        if mode not in [ 'r', 'rb' ]:
            raise Exception(f"Invalid mode: {mode}")

        return self._resolve(instance_id, filename).open(mode)

    def read(self, instance_id: str, filename : str, mode='r') -> str|bytes:
        with self.open(instance_id, filename, mode=mode) as f:
            return f.read()

    def files(self, instance_id : str) -> list[str]:
        return loads(self._resolve(instance_id).joinpath('_files.json').read_text())

    def events(self, start=None, listen=False) -> Iterable:
        # TODO: optimize
        e = self.root_dir.joinpath("log.txt").read_text().splitlines()

        for l in e:
            ts, ref, event = l.split('\t')
            
            if start and ts <= str(start):
                continue

            yield { "timestamp": ts, "ref": ref, "event": event }

    def _new_id(self) -> str:
        return str(uuid7())

    def _resolve(self, instance_id: str, filename: str = None) -> str:
        return self.root_dir.joinpath(*split_path(instance_id), *(['data', filename] if filename else []))

    def __iter__(self):
        return iter(self.root_dir.joinpath('instances.txt').read_text().splitlines())

    def __getitem__(self, instance_id: str) -> FileInstance:
        return self.get(instance_id)

    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        ...

    
