# The Tiniest Archive

**This is experimental software. Beware**

The Tiniest Archive is an attempt at building a minimal archive that still support basic OAIS functionality.

## Archive, Instance and Transaction objects

A Transaction is simply a temporary Instance. The Transaction will be merged into the Instance when `commit()` is called on it. This is done by the ContextManager.

### Usage - create instance and add file

`archive.new()` returns a Transaction object which, when used as a ContextManager, will automatically call `commit()` on a clean exit and `rollback()` if an exception occurs during processing.

```
from tiniestarchive import Archive

with Archive('https://example.org/') as archive:
    with archive.new() as instance:
        instance.add('car.png')
```

### Usage - add file to existing instance 

`archive.get(...)` will, depending on the `mode` parameter, return either a Transaction (for `mode='a'`) or an Instance (for the default `mode='r'`). Take care when using `mode='a'` without a ContextManager, as the Transaction will not be committed automatically, but rather leave a temporary Instance behind.

```
from tiniestarchive import Archive

with Archive('https://example.org/') as archive:
    with archive.get('1234-5678-9012', mode='a') as instance:
        instance.add('car.png')
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