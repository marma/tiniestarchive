from io import BufferedIOBase
from json import load
from os import makedirs
from os.path import join,exists
from typing import Iterable
from uuid_utils import uuid7

SPECIAL_FILES = [ '_meta.json', '_files.json' ]

class Instance:
    def __init__(self, instance_id : str, path : str, new_instance : bool = False):
        self.instance_id = instance_id
        self.path = path
        self.data_path = join(self.path, 'data')
        self.new_instance = new_instance

        if exists(self._data_path) and new_instance:
            raise Exception('Instance is not new but new_instance is True')

        if not exists(self._data_path):
            makedirs(self._data_path)

    def open(self, path, mode='r'):
        return open(self._resolve(path), mode)

    def read(self, path, mode='r') -> str|bytes:
        with self.open(path, mode) as f:
            return f.read()       

    def _resolve(self, path : str) -> str:
         if path in SPECIAL_FILES:
             return join(self.path, path)
         else:
            return join(self.data_path, path)

    def __enter__(self):        
        return self
    
    def __exit__(self, type, value, traceback):
        # rollback if temporary and exited with exception
        if type:
            if self.new_instance:
                # 
                ...
        else:
            ...

        def merge(self, instance):
            ...


class Transaction:
    def __init__(self, instance : Instance, path : str):
        self.instance = instance

    def add(self, path : str, filename : str = None, data : bytes = None, it : Iterable[bytes] = None):
        if not (filename or data or it):
            raise Exception('Either filename or data must be provided')

    def commit(self):
        ...

    def rollback(self):
        ...

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if type:
            self.rollback()
        # rollback if needed


class Archive:
    def __init__(self, directory : str):
        with open(join(directory, 'config.json'), 'r') as f:
            self.config = load(f)

    def _new_id(self) -> str:
        return str(uuid7())

    def get(self, instance_id: str, mode : str = 'r') -> Instance|Transaction:
        ...

    def new(self) -> Instance:
        ...

    def open(self, instance_id: str, filename : str) -> BufferedIOBase:
        ...

    def _resolve(self, instance_id: str, filename: str = None) -> str:
        ...

    def __getitem__(self, instance_id: str) -> Instance:
        return self.get(instance_id)

