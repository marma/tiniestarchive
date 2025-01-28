from copy import copy, deepcopy
from hashlib import md5
from sys import stderr
from io import BufferedIOBase, BufferedReader
from json import dumps, load, loads
from os import makedirs, listdir, remove, rename, stat
from os.path import join,exists
from posixpath import dirname
from shutil import move,copy, rmtree
from tempfile import gettempdir
from typing import Iterable, Union
from .commitmanager import CommitManager
from .ingestmanager import IngestManager
from uuid_utils import uuid7
from uuid import uuid4
from pathlib import Path
from time import time
from .utils import split_path, safe_path
from enum import Enum
from . import Archive,Instance,READ,READ_BINARY,WRITE,OPEN,FINALIZED,DELETED,READ_ONLY,READ_WRITE,DYNAMIC,WORM,PRESERVATION

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
    def __init__(self, path : str, mode : str = READ):
        self.path = Path(path) if path else gettempdir().joinpath(str(uuid4()))
        self.mode = mode

        if not self.path.exists() and mode == WRITE:
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

    def read(self, path, mode=READ) -> Union[str,bytes]:
        if mode not in [ READ, READ_BINARY ]:
            raise Exception(f"Invalid mode: {mode}")

        return self._resolve(path).read(mode)
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
                if stat(source).st_dev != stat(target.parent).st_dev:
                    tmp_target = f'{target}-tmp-{str(uuid7())}'
                    move(source, tmp_target)
                    source = tmp_target

                # this operation is atomic
                move(source, target)

                self.config['files'][path] = instance[path]

        self.config['version'] = str(uuid7())
        self._save()

    def add(self, filename, path : str = None, data : BufferedReader = None, checksum : str = None):
        if self.mode != WRITE:
            raise Exception("Adding files only allowed in 'w' mode")

        if not path:
            path = Path(filename).name
            
        tmpfile = Path(f'{self._resolve(path)}-tmp-{str(uuid7())}')

        with (data or open(filename, 'rb')) as d:
            try:
                tmpfile.parent.mkdir(parents=True, exist_ok=True)

                cs,size = md5(), 0
                with open(tmpfile, 'wb') as f:
                    while chunk := d.read(1024):
                        cs.update(chunk)
                        size += f.write(chunk)

                if checksum and checksum.lower() == cs.hexdigest().lower():
                    raise Exception('Checksum mismatch')

                tmpfile.rename(self._resolve(path))
                self.config['files'][path] = { 'id': str(uuid7()), 'path': path, 'size': size, 'checksum': f'md5:{cs.hexdigest()}' }
                self._save()
            except Exception as e:
                if exists(tmpfile):
                    remove(tmpfile)
                    
                raise e

    def finalize(self):
        if self.config['status'] == FINALIZED:
            raise Exception('Instance is already finalized')

        self.config['status'] = FINALIZED
        self._save()

    def create(path : str):
        path = Path(path)
        if path.exists():
            raise Exception('Path already exists')  
        
        path.joinpath('data').mkdir(parents=True, exist_ok=True)
        path.joinpath('instance.json').write_text(
            dumps(
                {
                    'id': str(uuid7()),
                    'resource': None,
                    'version': str(uuid7()),
                    'status': 'open',
                    'files': {}
                },
                indent=4))
        
    def status(self) -> str:
        return self.config['status']

    def json(self) -> dict:
        return deepcopy(self.config)

    def _resolve(self, path : Union[str,Path]) -> Path:
        return self.path.joinpath('data', path)

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
    
    def __getitem__(self, path : str):
        return self.config['files'][path]

    def __str__(self):
        return f"<FileInstance({self.instance_id}) @ {self.path}>"


class FileResource:
    def __init__(self, path : str = None, archive = None, mode : str = 'r'):
        self.path = Path(path) if path else Path(gettempdir()).joinpath(str(uuid4()))
        self.archive = archive
        self.mode = mode

        if not self.path.exists() and mode == WRITE:
            FileResource.create(self.path)

        self._reload()

        if self.mode == WRITE and self.status() == FINALIZED:
            raise Exception('Resource is finalized')

        self.resource_id = self.config['id']

    def transaction(self, finalize=False) -> CommitManager:
        self._writable_check()

        if self.status() == FINALIZED:
            raise Exception('Resource is finalized')

        return CommitManager(
                    self,
                    lambda x: FileInstance(x, mode=WRITE),
                    finalize=self.archive.operation_mode in [ WORM, PRESERVATION ] if self.archive else False)

    def update(self, instance : Instance):
        self._writable_check()

        if self.last_instance() and self.get_instance(self.last_instance()).status() == OPEN:
            if instance.status() == FINALIZED:
                raise Exception('Will not merge finalized instance into open instance')
            
            # open in write-mode to merge the instances
            last_instance = self.get_instance(self.last_instance(), mode=WRITE)
            last_instance.update(instance)
        else:
            # would shutil.move be nonatomic?
            instance_path = instance.path

            if stat(join(self.path, 'instances')).st_dev != stat(instance_path).st_dev:
                tmp_target = self.path.joinpath('instances', 'tmp-{str(uuid7())}')
                move(instance_path, tmp_target)
                instance_path = tmp_target

            # inject resource id into instance in a fugly way
            j = loads(instance_path.joinpath('instance.json').read_text())
            j['resource'] = self.resource_id
            instance_path.joinpath('instance.json').write_text(dumps(j, indent=4))

            # this operation is atomic
            move(instance_path, join(self.path, 'instances', instance.instance_id))

            self.config['instances'].append(instance.instance_id)

            self._save()
            self._reload()

    def get_instance(self, instance_id : str, mode : str = READ) -> FileInstance:
        instance_path = join(self.path, 'instances', instance_id)

        return FileInstance(instance_path, mode=mode)

    def last_instance(self) -> str:
        return self.config['instances'][-1] if len(self.config['instances']) > 0 else None

    def open(self, path, mode=READ) -> BufferedIOBase:
        if mode not in [ READ, READ_BINARY ]:
            raise Exception(f"Invalid mode: {mode}")

        return open(self._resolve(path), mode)

    def read(self, path, mode=READ) -> Union[str,bytes]:
        if mode not in [ READ, READ_BINARY ]:
            raise Exception(f"Invalid mode: {mode}")

        with self.open(path, mode) as f:
            return f.read()       

    def exists(self, path : str) -> bool:
        return path in self.files

    def create(path : str):
        resource_id = str(uuid7())

        if not exists(path):
            makedirs(path)
        
        makedirs(join(path, 'instances'))

        with open(join(path, 'resource.json'), 'w') as f:
            f.write(
                dumps(
                    { 'id': resource_id,
                      'version': str(uuid7()),
                      'status': 'open',
                      'instances': []
                    }, indent=4))

    def finalize(self):
        self._writable_check()

        if self.status() == FINALIZED:
            raise Exception('Resource is already finalized')

        self.config['status'] = FINALIZED
        self._save()

    def status(self) -> str:
        return self.config['status']

    def serialize(self) -> Iterable[bytes]:
        ...

    def json(self) -> dict:
        ret = loads(self.path.joinpath('resource.json').read_text())
        ret.update({ 'instances': { instance_id:loads(self.path.joinpath('instances', instance_id, 'instance.json').read_text()) for instance_id in ret['instances'] } })
        ret['files'] = deepcopy(self.files)

        return ret

    def _save(self):
        self.config['version'] = str(uuid7())
        with open(join(self.path, 'resource.json'), 'w') as f:
            f.write(dumps(self.config, indent=4))

    def __iter__(self):
        return iter(self.config['instances'])

    def _writable_check(self):
        if self.mode != WRITE:
            raise Exception('Resource is in read-only mode')

    def _clear(self):
        del(self.path)
        del(self.mode)
        del(self.config)

    def _reload(self):
        with open(join(self.path, 'resource.json'), 'r') as f:
            self.config = load(f)

        # create resolve and checksum maps
        self.files, self.checksums = {}, {}
        for instance_id in self.config['instances']:
            with open(join(self.path, 'instances', instance_id, 'instance.json')) as f:
                j = load(f)

            self.files.update(
                {
                  x['path']:(join('instances', instance_id, 'data', x['path']) if x.get('status', None) != DELETED else None)
                  for x in j['files'].values()
                })
            
            self.checksums.update({ x['checksum']:x['path'] for x in j['files'].values() if x.get('checksum', None) })

        self.files = { k:v for k,v in self.files.items() if v }

    def _resolve(self, path : str, instance_id : str = None) -> Path:
        if instance_id:
            return self.path.joinpath('instances', instance_id, 'data', path)
        
        return join(self.path, self.files[path])
        
    def __str__(self):
        return f"<FileResource({self.resource_id}) @ {self.path}>"
    
    def __enter__(self):
        # TODO: lock resource if in write mode
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        # TODO release lock if in write mode
        ...

class FileArchive:
    def __init__(self, path : str = None, operation_mode : str = None):
        if operation_mode not in [ None, DYNAMIC, WORM, PRESERVATION ]:
            raise Exception(f"Invalid operation mode: {operation_mode}")

        self.temporary = path is None
        self.root_dir = Path(path if path else gettempdir().joinpath(str(uuid4()))).absolute()
        self.operation_mode = operation_mode or PRESERVATION

        if not self.root_dir.exists():
            self.root_dir.mkdir(parents=True)

        if self.root_dir.joinpath('config.json').exists():
            self.config = loads(self.root_dir.joinpath('config.json').read_text())
        elif len(listdir(self.root_dir)) == 0:
            self.config = { 'mode': 'read-write', 'operation_mode': self.operation_mode }
            self.root_dir.joinpath('config.json').write_text(dumps(self.config, indent=4))
            self.root_dir.joinpath('resources.txt').write_text('')
            self.root_dir.joinpath('log.jsonl').write_text('')
        else:
            raise Exception('Invalid archive')

        if operation_mode and self.operation_mode != operation_mode:
            raise Exception(f"Operation mode cannot be changed")

        self.mode = self.config['mode']

        self.logger = EventLogger(self.root_dir.joinpath('log.jsonl'))

    def get(self, resource_id: str, mode : str = READ) -> FileResource:
        if mode not in [ READ, WRITE ]:
            raise Exception(f"Invalid mode: {mode}")
        
        if mode != READ and self.mode == READ:
            raise Exception('Archive is not in read-write mode')

        return FileResource(self._resolve(resource_id), archive=self, mode=mode)

    def new(self) -> IngestManager:
        if self.mode != READ_WRITE:
            raise Exception('Archive is not in read-write mode')

        tmpdir = Path(gettempdir()).joinpath(str(uuid4()))

        return IngestManager(self, FileResource(tmpdir, archive=self, mode=WRITE))

    def ingest(self, resource : FileResource):
        if self.mode != READ_WRITE:
            raise Exception('Archive is not in read-write mode')

        target_dir = self._resolve(resource.resource_id)
        target_dir.parent.mkdir(parents=True, exist_ok=True)

        if target_dir.parent.stat().st_dev != resource.path.stat().st_dev:
            tmp_dir = self._resolve(resource.resource_id).parent.joinpath(f'tmp-{str(uuid7())}')
            move(resource.path, tmp_dir)
            resource.path = tmp_dir

        move(resource.path, target_dir)

        with self.root_dir.joinpath('resources.txt').open(mode='a') as f:
            f.write(f"{resource.resource_id}\n")
            #('ingest - ' + resource.resource_id, file=stderr)

    def serialize(self, resource_id: str) -> Iterable[bytes]:
        raise Exception('Not implemented')

    def deserialize(self, data: Union[Instance,bytes,Iterable[bytes]] = None, instance_id: str = None, ):
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
        # TODO: optimize
        e = self.root_dir.joinpath("log.jsonl").read_text().splitlines()

        for l in e:
            ts, ref, event = l.split('\t')
            
            if start and ts <= str(start):
                continue

            yield { "timestamp": ts, "ref": ref, "event": event }

    #def operation_mode(self) -> str:
    #    return self.config['operation_mode']

    def json(self, resource_id: str) -> dict:
        return self.get(resource_id).json()

    def _new_id(self) -> str:
        return str(uuid7())

    def _resolve(self, resource_id, filename : str = None, instance_id: str = None) -> Path:
        resource_path = self.root_dir.joinpath(*split_path(resource_id))

        if instance_id is None:
            return self.get(resource_id)._resolve(filename) if filename else resource_path
        else:
            instance_path = resource_path.joinpath('instances', instance_id)

            if filename:
                file_path = instance_path.joinpath(filename)

                # Quick check if file exists to avoid loading the instance
                if file_path.exists():
                    return file_path
                else:
                    # Ugh. Last resort since the instance might contain a reference
                    return FileInstance(instance_path)._resolve(filename)

            return instance_path

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        # Double-check that the root_dir is within the temporary directory
        # to avoid catastrophic data loss on accidentaly setting the
        # `self.temporary` flag to `False`.
        # 
        # Consider removing the ContextManager-functionality for FileArchive
        # altogether to avoid this risk entirely.
        if self.temporary and gettempdir() in str(self.root_dir):
            rmtree(self.root_dir)

    def __iter__(self):
        return iter(self.root_dir.joinpath('resources.txt').read_text().splitlines())

    def __getitem__(self, resource_id: str) -> FileResource:
        return self.get(resource_id)
    
    def __str__(self):
        return f"<FileArchive @ {self.root_dir }>"
