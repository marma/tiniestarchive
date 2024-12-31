# The Tiniest Archive

** This is currently experimental software not intended for production use  **

The Tiniest Archive is an attempt at building a minimal digital archive that still support some OAIS fundamentals through the use of package instances and package resources. It does, on purpose, not deal with metadata or any understanding of the content within these instances/resources. The aim is to provide a clear separation between package descriptions / file integrity and information concerning 

## Design considerations

Three modes of operation:

- Preservation mode - In this mode no instance files will ever get updated, overwritten or deleted, but rather replaced only using new instances that will logically replace or delete files.

- WORM mode - a sub-mode of Preservation mode where all files, even the package resource description and administrative files, can only be written once and never updated, deleted or renamed. This puts extra pressure on the disk and comput and so lowers performance. This is a problem that should be solved using caching (see `CachedArchive`).

- Dynamic mode - in "Dynamic mode" files can be changed, deleted and updated.

Optimally these two modes can be combined using `MultiArchive` with one archive in preservation mode and one (or more) in dynamic mode.

### Simplicity 

Complexity adds to the probability of failure so important things need to have as few moving parts as possible, i.e things that must not fail shuld be simple. 

- Few and and atomic operations that either succeed or fail in a very visible manner and can then be rolled back
- Using the archive as it exists on disk as a single source of truth, i.e no database or index should be used to ensure that operations maintain the integrity of the archive

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

## Archive, Instance and Transaction objects

A Transaction is simply a temporary Instance. The Transaction will be merged (or moved) into the target `Instance` when `commit()` is called on it. This is done by the CommitManager.

### Usage - create instance and add file

`archive.new()` returns a Transaction object which, when used as a ContextManager, will automatically call `commit()` on a clean exit and `rollback()` if an exception occurs during processing.

```
from tiniestarchive import Archive

with Archive('https://example.org/') as archive:
    with archive.new() as resource:
        with resource.new() as transaction:
            resource.add('car.png')
```

### Usage - add file to existing instance 

`archive.get(...)` will, depending on the `mode` parameter, return either a Transaction (for `mode='a'`) or an Instance (for the default `mode='r'`). Take care when using `mode='a'` without a ContextManager, as the Transaction will not be committed automatically, but rather leave a temporary Instance behind.

```
from tiniestarchive import Archive

with Archive('https://example.org/') as archive:
    with archive.get('1234-5678-9012', mode='t') as transaction:
        transaction.add('car.png')
```

### Usage - exception while adding files results in transaction rollback

```
from tiniestarchive import Archive

with Archive('https://example.org/') as archive:
    with archive.get('1234-5678-9012', mode='a') as instance:
        instance.add('car.png')
        instance.add('horse.png')

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


### File structure for `FileInstance`

Tiniestarchive uses BagIt with a profile[3]

```
1234-5678-9012/
|-- data
|   |-- instances/
|       |-- 3456-7890-1234/
|           |-- instance.json
|           |-- img01.jpg
|       |-- 3456-7890-1234/
|           |-- instance.json
|           |-- img01.xml
|   |-- resource.json
|-- bagit.txt
|-- manifest-md5.txt
```

### Optionals



1. https://en.wikipedia.org/wiki/Write_once_read_many
2. https://en.wikipedia.org/wiki/Append-only
3. https://bagit-profiles.github.io/bagit-profiles-specification/
