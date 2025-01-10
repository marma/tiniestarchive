# The Tiniest Archive

** This is proof-of-concept / experimental software not intended for production use  **

The Tiniest Archive is an attempt at building a minimal digital archive that still support some OAIS fundamentals through the use of package instances and package resources. It does, on purpose, not deal with metadata or any understanding of the content within these instances/resources. The aim is to provide a clear separation between package descriptions / file integrity and information concerning 

## Design considerations

Three modes of operation:

- *Preservation mode* - In this mode no instance files will ever get updated, overwritten or deleted in-place. Rather files will be replaced only by adding new instances that will logically add, replace or delete files. 

- *WORM mode* - a sub-mode of Preservation mode where all files, even the package resource description and administrative files, can only be written once and never updated, deleted or renamed. This puts extra pressure on the disk and compute which lowers performance. This is a problem that should be solved outside of the archive using caching (see `CachedArchive`).

- *Unsafe mode* - in "Unsafe mode" files *can* be changed, deleted and updated.

Optimally these modes can be combined using `MultiArchive` with one archive in preservation mode and one (or more) in unsafe mode.

### Simplicity 

Complexity adds to the probability of failure so important things need to have as few moving parts as possible, i.e things that must not fail should be simple. 

- Few and and atomic operations that either succeed or fail in a very visible manner and can then be rolled back
- Using the archive as it exists on disk as a single source of truth, i.e no database or index should be used to ensure that operations maintain the integrity of the archive

### Design for operations

### Robustness

- Errors/mistakes/bad code on the client side must **never** result in broken packages
- Catastrophic failures, such as loss of connection to the storage, must result in a state that can be either rolled back or "replayed" at a later stage

### Flexibility

### Code base size

The core code base should be fewer lines than this README-file.

### Feature: Write-once, read many

The archive should optionally be able to operate in a write-once-read-many (WORM) mode. This means that files are never deleted or changed. Optimally this mode should be backed by a storage solution that enforces this mode of operation[1,2].

### Feature: Merge-on-migrate

### Feature: using checksums to avoid storing the same file more than once within a `PackageResource`

- Check every added file against existing checksums in finalized `Instance`s so that only one copy is actually saved to disk

## Archive, Resource, Instance and Transaction objects

TTA has three main classes:

- Instance - the class that actually deals with files
- Resource - a Resource contains one or more instances and fronts these instances for access. It also handles Transactions 
- Archive - deals with access to an archive either through file or HTTP(S) access

A Transaction is simply a temporary Instance used to enable commit/rollback functionality. 

## Usage

### Usage - read file from resource

```
from tiniestarchive import Archive

archive = Archive('/archive')
resource = archive.get('1234-5678-9012')
m = resource.read('meta.json')
```

### Usage - create resource and add files

`archive.new()` returns a temporary Resource object with an open Instance. When used as a context manager (`CommitManager`) it will automatically add the resource to the archive on a clean exit or delete it if an exception occurs. Depending on the archive mode the instance will either be finalized or left open.

```
from tiniestarchive import Archive

archive = Archive('/archive')
with archive.new() as resource:
    with resource.transaction() as transaction:
        transaction.add('meta.json')
        transaction.add('car.jp2')
        transaction.add('boat.jp2')
```

### Usage - add file to existing resource 

`archive.get(...)` will, depending on the `mode` parameter, return either a Transaction (for `mode='w'`) or an Resource (for the default `mode='r'`). Take care when using `mode='a'` without a ContextManager, as the Transaction will not be committed automatically, but rather leave a temporary Instance behind.

```
from tiniestarchive import Archive

archive = Archive('/archive')
with archive.get('1234-5678-9012', mode='w') as resource:
    with resource.transaction() as transaction:
        resource.add('resource.png')
```

### Usage - exception while adding files results in transaction rollback

In this example the file added to the transaction will never added to the resource and the resource will never be added to the archive.

```
from tiniestarchive import Archive

archive = Archive('/archive')
with archive.new() as resource: 
    with resource.transaction() as transaction:
        transaction.add('meta.json')

        raise Exception('Something went wrong')
```

### Usage - iterate over packages

```
from tiniestarchive import Archive

with Archive('https://example.org/') as archive:
    for instance_id in archive:
        print(instance_id)
```

### Usage - iterate over events

```
from tiniestarchive import Archive

with Archive('https://example.org/') as archive:
    for event in archive.events(start='2021-12-01T12:00:00Z'):
        print(event)
```

### Usage - 


### File structure for `FileResource`

Tiniestarchive uses BagIt with a profile[3]

```
1234-5678-9012/
|-- instances/
|   |-- 3456-7890-1234/
|       |-- data/
|           |-- img01.jpg
|       |-- instance.json
|   |-- 7890-1234-5678/
|       |-- data/
|           |-- img01.xml
|       |-- instance.json
|   |-- resource.json
```

### Optionals

1. https://en.wikipedia.org/wiki/Write_once_read_many
2. https://en.wikipedia.org/wiki/Append-only
3. https://bagit-profiles.github.io/bagit-profiles-specification/
