from copy import copy
from hashlib import md5
from io import BufferedIOBase, BufferedReader
from json import dumps, load, loads
from os import makedirs, listdir, remove, rename
from os.path import join,exists
from posixpath import dirname
import shutil
from typing import Iterable
from uuid_utils import uuid7
from pathlib import Path
from time import time
from .utils import split_path, safe_path

class EventLogger:
    def __init__(self, filename):
        self.filename = filename

    def log(self, ref : str, event : str, transaction_id : str = None):
        with self.filename.open('a') as f:
            f.write(f"{time()}\t{ref}\t{event}{('\t' + transaction_id) if transaction_id else ''}\n")


class FileInstance:
    def __init__(self, path : str,  instance_id : str, mode : str = 'r'):
        self.path = self.path
        self.instance_id = instance_id

        if mode == 'w' and (not exists(self.path) or len(listdir(self.path)) == 0):
            makedirs(join(self.path, 'data'))
            self.config = { "@id": f"urn:uuid:{instance_id}", "version": str(uuid7()), "status": "open" }
            self.files = {}
            self._save()
        elif self._is_package(self.path):
            self._reload()

            if mode == 'w' and self.config['status'] == 'finalized':
                raise Exception('Instance is finalized')
        else:
            raise Exception(f'Directory {self.path} is neither empty nor a valid instance')

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

    def add(self, path : str, data : BufferedReader, checksum : str = None):
        if self.mode != 'w':
            raise Exception("Adding files only allowed in 'w' mode")

        tmpfile = join(self._resolve(path), '-tmp-', str(uuid7()))

        try:
            if not exists(d := dirname(tmpfile)):
                makedirs(d)

            cs,size = md5(), 0
            with open(tmpfile, 'wb') as f:
                while chunk := data.read(1024):
                    cs.update(chunk)
                    size += f.write(chunk)

            if checksum and checksum == cs.hexdigest():
                raise Exception('Checksum mismatch')

            rename(tmpfile, self._resolve(path))

            self.files[path] = { 'path': path, 'size': size, 'checksum': f'md5:{cs.hexdigest()}' }

            # this is suboptimal when adding a large number of files
            self._save()
        except Exception as e:
            if exists(tmpfile):
                remove(tmpfile)
                
            raise e

    def finalize(self):
        if self.config['status'] == 'finalized':
            raise Exception('Instance is already finalized')

        self.config['status'] = 'finalized'
        self._save()

    def _reload(self):
        with open(join(self.path, 'instance.json'), 'r') as f, open(join(self.path, 'files.json'), 'r') as g:
            self.config = load(f)
            self.files = load(g)

    def _save(self):
        with open(join(self.path, 'instance.json'), 'w') as f, open(join(self.path, 'files.json'), 'w') as g:
            f.write(dumps(self.config, indent=4))
            g.write(dumps(self.files, indent=4))

    def _touch(self):
        self.config['version'] = str(uuid7())
        self._save()

    def _resolve(self, path : str) -> str:
        return join(self.path, 'data', path)
    
    def _is_package(self, path : str) -> bool:
        return all(
                    exists(join(path, 'data')),
                    exists(join(path, 'instance.json')),
                    exists(join(path, 'files.json'))
                )


class FileResource:
    def __init__(self, path : str, mode='r'):
        self.path = path

        if mode == 'r' and not exists(path):
            raise Exception(f'Resource ({path}) does not exist')

        if mode == 'w' and not exists(path):
            makedirs(path)
            makedirs(path.joinpath('instances'))

        
        self.config = self._load_config()
        self.instances = self._load_instances()



    def _load_config(self):
        with open(self.path.joinpath('resource.json'), 'r') as f:
            return load(f)

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

    def commit(self, instance, target_id):
        target_dir = self._resolve(target_id)

        # can we simply rename/move the directory?
        if instance.mode in [ 't', '1' ] and not self.exists(target_id):
            # simply move the staging directory to the target location
            shutil.move(instance.path, self._resolve(target_id))
        else:
            # merge the staging directory into the target location
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

    def exists(self, instance_id : str) -> bool:
        return self._resolve(instance_id).exists()

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

