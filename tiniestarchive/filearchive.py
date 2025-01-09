from copy import copy
from hashlib import md5
from io import BufferedIOBase, BufferedReader
from json import dumps, load, loads
from os import makedirs, listdir, remove, rename, stat
from os.path import join,exists
from posixpath import dirname
from shutil import move,copy
from typing import Iterable, Union
from tiniestarchive.commitmanager import CommitManager
from uuid_utils import uuid7
from pathlib import Path
from time import time
from .utils import split_path, safe_path
from enum import Enum
from . import Archive,Instance,READ,READ_BINARY,WRITE,OPEN,FINALIZED,DELETED

class EventLogger:
    def __init__(self, filename):
        self.filename = filename

    def log(self, ref : str, event : str, transaction_id : str = None):
        with self.filename.open('a') as f:
            x = { 'timestamp': time(), 'ref': ref, 'event': event }

            if transaction_id:
                x['transaction_id'] = transaction_id

            f.write(dumps(x))

class FileInstance(Instance):
    def __init__(self, base : str, mode : str = READ, resource_id : str = None):
        self.path = self.path
        self.mode = mode

        if not exists(self.path) and mode == WRITE:
            FileInstance.create(self.path, resource_id)

        with open(join(self.path, 'instance.json'), READ) as f:
            self.config = load(f)

        self.instance_id = self.config['id']

        if self.mode == WRITE and self.config['status'] == FINALIZED:
            raise Exception('Instance is finalized')

    def open(self, path, mode=READ) -> BufferedIOBase:
        if mode not in [ READ, READ_BINARY ]:
            raise Exception(f"Invalid mode: {mode}")

        return open(self._resolve(path), mode)

    def read(self, path, mode=READ) -> Union[str,bytes]:
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

    def update(self, instance : Instance):
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

    def create(path : str, resource_id : str):
        if exists(path):
            raise Exception('Path already exists')  
        
        makedirs(join(path, 'data'))

        with open(join(path, 'instance.json'), 'w') as f:
            f.write(dumps({ 'id': str(uuid7()), 'resource': resource_id, 'version': str(uuid7()), 'status': 'open', 'files': {} }, indent=4))

    def status(self) -> str:
        return self.config['status']

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

    def _load(self):
        with open(join(self.path, 'instance.json'), 'r') as f:
            self.config = load(f)

    def __iter__(self):
        return iter(self.config['files'].keys())


class FileResource:
    def __init__(self, path : str, mode : str = 'r'):
        self.path = path
        self.mode = mode

        if not exists(self.path) and mode == WRITE:
            FileResource.create(self.path)

        self._reload()

        if self.mode == WRITE and self.status() == FINALIZED:
            raise Exception('Resource is finalized')

    def transaction(self) -> CommitManager:
        self._writeable_check()

        if self.status() == FINALIZED:
            raise Exception('Resource is finalized')

        return CommitManager(self, self, lambda x: FileInstance(x, mode=WRITE))

    def update(self, instance : Instance):
        self._writable_check()

        last_instance = self.get_instance(self.config['instances'][-1])
        if last_instance.status() == OPEN:
            # merge the instances
            last_instance.update(instance)
        else:
            # would shutil.move be nonatomic?
            instance_path = instance.path
            if stat(join(self.path, 'instances')).st_dev != stat(instance_path).st_dev:
                tmp_target = f'{self.path}/instances/tmp-{str(uuid7())}'
                move(instance_path, tmp_target)
                instance_path = tmp_target

            # this operation is atomic
            move(instance_path, join(self.path, 'instances', instance.instance_id))
            self.config['instances'].append(instance.instance_id)

        self._save()

    def get_instance(self, instance_id : str, mode : str = READ) -> FileInstance:
        instance_path = join(self.path, 'instances', instance_id)

        return FileInstance(instance_path, mode=mode)


    def open(self, path, mode=READ) -> BufferedIOBase:
        if mode not in [ READ, READ_BINARY ]:
            raise Exception(f"Invalid mode: {mode}")

        return open(self._resolve(path), mode)

    def read(self, path, mode=READ) -> Union[str,bytes]:
        if mode not in [ READ, READ_BINARY ]:
            raise Exception(f"Invalid mode: {mode}")

        with self.open(path, mode) as f:
            return f.read()       

    def create(path : str):
        resource_id = str(uuid7())

        if not exists(path):
            makedirs(path)
        
        makedirs(join(path, 'instances'))

        instance_id = str(uuid7())
        instance_path = join(path, 'instances', instance_id)
        FileInstance.create(instance_path, resource_id)

        with open(join(path, 'resource.json'), 'w') as f:
            f.write(
                dumps(
                    { 'id': resource_id,
                      'version': str(uuid7()),
                      'status': 'open',
                      'instances': [ instance_id ]
                    }, indent=4))

    def finalize(self):
        self._writable_check()

        if self.status() == FINALIZED:
            raise Exception('Resource is already finalized')

        self.config['status'] = FINALIZED
        self._save()

    def status(self) -> str:
        return self.config['status']

    def _save(self):
        with open(join(self.path, 'resource.json'), 'w') as f:
            f.write(dumps(self.config, indent=4))

    def __iter__(self):
        return iter(self.config['instances'])

    def _writable_check(self):
        if self.mode != WRITE:
            raise Exception('Resource is in read-only mode')

    def _reload(self):
        with open(join(self.path, 'resource.json'), 'r') as f:
            self.config = load(f)

        # create resolve and checksum maps
        self.files, self.checksums = {}, {}
        for instance_id in self.config['instances']:
            j = join(self.path, 'instances', instance_id, 'instance.json')
            self.files.update(
                {
                  x['path']:(join('instances', instance_id, x['path']) if x.get('status', None) != DELETED else None)
                  for x in j['files'].values()
                  if x.get('status', None) != DELETED
                })
            
            self.checksums.update({ x['checksum']:x['path'] for x in j['files'].values() })

    def _resolve(self, path : str) -> str:
        if path in self.files and self.files[path]:
            return join(self.path, self.files[path])
        

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

        if mode == WRITE and not self.root_dir.exists():
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

    def deserialize(self, data: Union[Instance,bytes,Iterable[bytes]] = None, instance_id: str = None, ):
        raise Exception('Not implemented')

    def commit(self, instance, target_id):
        target_dir = self._resolve(target_id)

        # can we simply rename/move the directory?
        if instance.mode in [ 't', '1' ] and not self.exists(target_id):
            # simply move the staging directory to the target location
            move(instance.path, self._resolve(target_id))
        else:
            # merge the staging directory into the target location
            raise Exception('Not implemented')

    def open(self, instance_id: str, filename : str, mode='r') -> BufferedIOBase:
        if mode not in [ 'r', 'rb' ]:
            raise Exception(f"Invalid mode: {mode}")

        return self._resolve(instance_id, filename).open(mode)

    def read(self, instance_id: str, filename : str, mode='r') -> Union[str,bytes]:
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

