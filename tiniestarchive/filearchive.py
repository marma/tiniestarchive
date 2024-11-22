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

SPECIAL_FILES = [ '_meta.json', '_files.json', '_checksums.json' ]

class EventLogger:
    def __init__(self, filename):
        self.filename = filename

    def log(self, ref : str, event : str, transaction_id : str = None):
        with self.filename.open('a') as f:
            f.write(f"{time()}\t{ref}\t{event}{('\t' + transaction_id) if transaction_id else ''}\n")


class FileInstance:
    def __init__(self, path : str,  instance_id : str = uuid7().hex, mode : str = 'r', target_path : str = None, target_version : str = None, logger : EventLogger = None, archive = None):
        if mode not in [ '1', 't', 'r', 'w' ]:
            raise Exception(f"Invalid mode: {mode}")

        if mode in [ '1', 't' ] and not target_path or mode in [ 'r', 'w'] and target_path:
            raise Exception("Target path must be provided for mode 't', and must not for 'r', 'w'")

        if mode in [ 'r', 'w' ] and not exists(path):
            raise Exception('Instance does not exist')

        if mode in [ 't' ] and exists(path):
            raise Exception('Path already exists')

        if mode == '1' and exists(target_path):
            raise Exception('Target path already exists')

        if mode == 't' and archive.finalized(path=target_path):
            raise Exception("Target instance is finalized")

        self.instance_id = instance_id
        self.path = path
        self.target_path = target_path
        self.target_version = target_version
        self.mode = mode
        self.logger = logger
        self.archive = archive

        if not exists(self.path):
            makedirs(join(self.path, 'data'))
            self.config = { "@id": f"urn:uuid:{instance_id}", "version": str(uuid7()), "status": "open" }
            self.files = []
            self._save()

            if mode == 'w':
                self.logger.log(self.instance_id, 'create')

            if self.target_id:
                self.config['target_id'] = self.target_id
        else:
            self._reload()

            if mode in [ 'w' ] and self.config['status'] == 'finalized':
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

        self.logger.log(f'{self.instance_id}/{path}', 'add')

        if file not in self.files:
            self.files.append(path)

        self._touch()

    def finalize(self):
        if self.config['status'] == 'finalized':
            raise Exception('Instance is already finalized')

        self.config['status'] = 'finalized'
        self.logger.log(self.instance_id, 'finalize')
        self._save()

    def commit(self):
        if self.mode is '1' or self.mode is 't' and not exists(self.target_path):
            # simply rename/move the directory
            self.logger.log(self.instance_id, 'commit', self.version)
            parent_dir = Path(self.target_path).parent
            parent_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(self.path, parent_dir)
            self.path = self.target_path
            self.target_path = None
            self.target_version = None
        elif self.mode is 't' and exists(self.target_path):
            # merge the staging directory into the target location
            with self.archive.get(self.target_id, mode='w') as target:
                # TODO: lock target instance
                # TODO: check target version against last known version
                for f in self.files:
                    shutil.move(self._resolve(f), self.archive._resolve(p.path, f))
                    self.logger.log(f'{self.instance_id}/{f}', 'add')

                self.path = self.target_path
                self.target_path = None
                self.target_version = None
                
                # remove the staging directory
                shutil.rmtree(self.path)
        else:
            raise Exception('Commit only allowed in transaction or write-once mode')

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
            # remove the directory on error
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

    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        ...

    
