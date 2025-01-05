from copy import copy
from hashlib import md5
from io import BufferedIOBase, BufferedReader
from json import dumps, load, loads
from os import makedirs, listdir, remove, rename, stat
from os.path import join,exists
from posixpath import dirname
from shutil import move,copy
from typing import Iterable
from tiniestarchive.commitmanager import CommitManager
from uuid_utils import uuid7
from pathlib import Path
from time import time
from .utils import split_path, safe_path
from enum import Enum
from . import Archive,Instance,READ,READ_BINARY,WRITE,FINALIZED,DELETED

class EventLogger:
    def __init__(self, filename):
        self.filename = filename

    def log(self, ref : str, event : str, transaction_id : str = None):
        with self.filename.open('a') as f:
            f.write(f"{time()}\t{ref}\t{event}{('\t' + transaction_id) if transaction_id else ''}\n")

class FileInstance(Instance):
    def __init__(self, base : str, mode : str = READ):
        self.path = self.path
        self.mode = mode

        if not exists(self.path) and mode == WRITE:
            FileInstance.create(self.path)

        with open(join(self.path, 'instance.json'), READ) as f:
            self.config = load(f)

        self.instance_id = self.config['id']

        if self.mode == WRITE and self.config['status'] == FINALIZED:
            raise Exception('Instance is finalized')

    def open(self, path, mode=READ) -> BufferedIOBase:
        if mode not in [ READ, READ_BINARY ]:
            raise Exception(f"Invalid mode: {mode}")

        return open(self._resolve(path), mode)

    def read(self, path, mode=READ) -> str | bytes:  
        if mode not in [ READ, READ_BINARY ]:
            raise Exception(f"Invalid mode: {mode}")

        with self.open(path, mode) as f:
            return f.read()       
        
    def delete(self, path : str):
        if self.mode != 'w':
            raise Exception("Deleting files only allowed in 'w' mode")

        self.config['files'][path] = { 'path': path, 'status': 'deleted' }

        if exists(f := self._resolve(path)):
            remove(f)

        self._save()

    def merge(self, instance : Instance):
        # WARNING: this is an operation that can fail half-way through given
        # catastrophic loss of connection to the storage. With no way to
        # recover this would leave the instance in an inconsistent state.
        # However, individual files will be either written in full or not at
        # all since shutil.move is atomic within the same filesystem
        if self.mode != WRITE:
            raise Exception("Merging instances only allowed in 'w' mode")

        for path in instance:
            if instance[path].get('status', None) == DELETED:
                self.delete(path)
            else:
                source = instance._resolve(path)
                target = self._resolve(path)

                # will shutil.move be nonatomic?
                if stat(source).st_dev != stat(target).st_dev:
                    tmp_target = f'{target}-tmp-{str(uuid7())}'
                    move(source, tmp_target)
                    source = tmp_target

                # this operation is atomic
                move(source, target)

                self.files[path] = instance[path]

        self._save()

    def serialize(self) -> Iterable[bytes]:
        raise Exception('Not implemented')

    def finalize(self):
        if self.config['status'] == FINALIZED:
            raise Exception('Instance is already finalized')

        self.config['status'] = FINALIZED
        self._save()

    def create(path : str):
        if exists(path):
            raise Exception('Path already exists')  
        
        makedirs(join(path, 'data'))

        with open(join(path, 'instance.json'), 'w') as f:
            f.write(dumps({ 'id': str(uuid7()), 'version': str(uuid7()), 'status': 'open', 'files': {} }, indent=4))

    def _resolve(self, path : str) -> str:
        return join(self.path, 'data', path)

    def _add(self, path : str, data : BufferedReader, checksum : str = None):
        if self.mode != WRITE:
            raise Exception("Adding files only allowed in 'w' mode")

        tmpfile = f'{self._resolve(path)}-tmp-{str(uuid7())}'

        try:
            if not exists(d := dirname(tmpfile)):
                makedirs(d)

            cs,size = md5(), 0
            with open(tmpfile, 'wb') as f:
                while chunk := data.read(1024):
                    cs.update(chunk)
                    size += f.write(chunk)

            if checksum and checksum.lower() == cs.hexdigest().lower():
                raise Exception('Checksum mismatch')

            rename(tmpfile, self._resolve(path))

            self.config['files'][path] = { 'path': path, 'size': size, 'checksum': f'md5:{cs.hexdigest()}' }
        except Exception as e:
            if exists(tmpfile):
                remove(tmpfile)
                
            raise e

    def _save(self):
        self.config['version'] = str(uuid7())
        with open(join(self.path, 'instance.json'), 'w') as f:
            f.write(dumps(self.config, indent=4))

    def _remove(self, path):
        del(self.config['files'][path])
        self._save()

    def __iter__(self):
        return iter(self.config['files'].keys())


class FileResource:
    def __init__(self, path : str, mode : str = 'r', archive : Archive = None):
        self.path = path
        self.mode = mode
        self._archive = archive

        if not exists(self.path) and mode == READ:
            FileResource.create(self.path)

        with open(join(self.path, 'resource.json'), 'r') as f:
            self.config = load(f)

        # @TODO: add file map
        # @TODO: add checksum map

    def transaction(self) -> CommitManager:
        if self.mode == READ:
            raise Exception('Resource is in read-only mode')

        if self.config.get('status', None) == 'finalized':
            raise Exception('Resource is finalized')

        return self._archive.transaction(resource=self)

    def create(path : str):
        if not exists(path):
            makedirs(path)
        
        makedirs(join(path, 'instances'))

        instance_id = str(uuid7())
        instance_path = join(path, 'instances', instance_id)
        FileInstance.create(instance_path)

        with open(join(path, 'resource.json'), 'w') as f:
            f.write(dumps({ 'id': str(uuid7()), 'version': str(uuid7()), 'status': 'open', 'instances': [ instance_id ] }, indent=4))

    def _add_instance(self, instance : Instance):
        ...

    def __iter__(self):
        return iter(self.config['instances'].keys())

    def _resolve(self, path : str) -> str:
        for instance_id in reversed(self.config['instances']):
            if exists(f := join(self.path, instance_id, 'data', path)):
                return f

class FileArchive:
    def __init__(self, path : str = None, mode=READ):
        path = Path(copy(path))

        if mode not in [ READ, WRITE ]:
            raise Exception(f"Invalid mode: {mode}")

        if mode == READ and not path.exists():
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
        return self.config['files'].keys()

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

