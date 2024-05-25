from io import BufferedIOBase
from json import dumps, load
from os import makedirs
from os.path import join,exists
import shutil
from typing import Iterable
from uuid_utils import uuid7
from pathlib import Path

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
            self.config = { "urn": f"urn:uuid:{instance_id}", "version": str(uuid7()), "status": "open" }

            with open(join(self.path, '_meta.json'), 'w') as f:
                f.write(dumps(self.config, indent=4))

            with open(join(self.path, '_files.json'), 'w') as f:
                f.write(dumps([], indent=4))
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

        if self.mode not in [ '1', 'w', 't' ]:
            raise Exception('Adding files only allowed in write mode')
        
        if self.config['status'] == 'finalized':
            raise Exception('Instance is finalized')

        # TODO: calculate / validate checksum

        if filename:
            shutil.copy(filename, self._resolve(path))
        elif data:
            with open(self._resolve(path), 'w' if isinstance(data, str) else 'wb') as f:
                f.write(data)

        self._touch()

    def finalize(self):
        if self.config['status'] == 'finalized':
            raise Exception('Instance is already finalized')

        self.config['status'] = 'finalized'
        self._save()

    def _reload(self):
        with open(join(self.path, '_meta.json'), 'r') as f:
            self.config = load(f)

    def _save(self):
        with open(join(self.path, '_meta.json'), 'w') as f:
            f.write(dumps(self.config, indent=4))

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
                parent_dir = Path(self.target_path).parent
                parent_dir.mkdir(parents=True, exist_ok=True)
                shutil.move(self.path, parent_dir)

        if self.mode == '1':
            self.finalize()



class Archive:
    def __init__(self, directory : str):
        with open(join(directory, 'config.json'), 'r') as f:
            self.config = load(f)

    def _new_id(self) -> str:
        return str(uuid7())

    def get(self, instance_id: str, mode : str = 'r') -> Instance:
        ...

    def new(self, parent_id : str = None) -> Instance:
        ...

    def open(self, instance_id: str, filename : str) -> BufferedIOBase:
        ...

    def events(self, start=None, listen=False) -> Iterable:
        ...

    def _resolve(self, instance_id: str, filename: str = None) -> str:
        ...

    def __getitem__(self, instance_id: str) -> Instance:
        return self.get(instance_id)

    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        ...

