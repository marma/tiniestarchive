from copy import copy
from io import BufferedIOBase
from json import dumps, load, loads
from os import makedirs
from os.path import join,exists
import shutil
from typing import Iterable
from uuid_utils import uuid7
from pathlib import Path
from .utils import split_path, safe_path

SPECIAL_FILES = [ '_meta.json', '_files.json' ]

class Instance:
    def __init__(self, instance_id : str, path : str, mode='r', tmp_path : str = None):
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
        self.target_path = path
        self.mode = mode

        if not exists(self.path):
            makedirs(join(self.path, 'data'))
            self.config = { "@id": f"urn:uuid:{instance_id}", "version": str(uuid7()), "status": "open" }
            self.files = []
            self._save()
        else:
            if mode in [ 'a' ] and self.config['status'] == 'finalized':
                raise Exception('Instance is finalized')

            self._reload()

    def open(self, path, mode='r') -> BufferedIOBase:
        if mode not in [ 'r', 'rb' ]:
            raise Exception(f"Invalid mode: {mode}")

        return open(self._resolve(path), mode)

    def read(self, path, mode='r') -> str | bytes:
        if mode not in [ 'r', 'rb' ]:
            raise Exception(f"Invalid mode: {mode}")

        with self.open(path, mode) as f:
            return f.read()       

    def add(self, path : str, filename : str = None, data : bytes|str = None, checksum : str = None):
        if not (filename or data):
            raise Exception('Either filename or data must be provided')

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

        self.files.append(path)
        self._touch()

    def finalize(self):
        if self.config['status'] == 'finalized':
            raise Exception('Instance is already finalized')

        self.config['status'] = 'finalized'
        self._save()

    def commit(self):
        parent_dir = Path(self.target_path).parent
        parent_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(self.path, parent_dir)
        self.path = self.target_path
        mode = 'r'

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

        if self.mode == '1':
            self.finalize()


class Archive:
    def __init__(self, path : str, mode='r'):
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
        else:
            self.config = loads(self.root_dir.joinpath('config.json').read_text())

        self.mode = mode

    def get(self, instance_id: str, mode : str = 'r') -> Instance:
        if mode not in [ 'r', 'a' ]:
            raise Exception(f"Invalid mode: {mode}")
        
        if mode != 'r' and self.mode == 'r':
            raise Exception('Archive is in read-only mode')

        path = self._resolve(instance_id)

        return Instance(instance_id, path, mode)

    def new(self, mode : str = 't') -> Instance:
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

        return Instance(instance_id, path, mode=mode, tmp_path=tmp_path)

    def open(self, instance_id: str, filename : str, mode='r') -> BufferedIOBase:
        if mode not in [ 'r', 'rb' ]:
            raise Exception(f"Invalid mode: {mode}")

        return self._resolve(instance_id, filename).open(mode)

    def read(self, instance_id: str, filename : str, mode='r') -> str|bytes:
        with self.open(instance_id, filename, mode=mode) as f:
            return f.read()

    def events(self, start=None, listen=False) -> Iterable:
        return iter([])

    def _new_id(self) -> str:
        return str(uuid7())

    def _resolve(self, instance_id: str, filename: str = None) -> str:
        return self.root_dir.joinpath(*split_path(instance_id), *(['data', filename] if filename else []))

    def __getitem__(self, instance_id: str) -> Instance:
        return self.get(instance_id)

    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        ...

