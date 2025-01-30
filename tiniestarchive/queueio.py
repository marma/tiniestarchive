# Origin: https://github.com/marma/starch/blob/master/starch/queueio.py  

from sys import stderr
from io import RawIOBase,UnsupportedOperation,BufferedWriter,TextIOWrapper,BlockingIOError,DEFAULT_BUFFER_SIZE
from queue import Queue
from time import time,sleep

def open(q, mode='wb', buffering=-1, encoding=None, maxsize=10, timeout=None):
    if mode != 'wb':
        raise Exception('only write-only binary streams supported')

    if encoding != None:
        raise ValueError('binary mode doesn\'t take an encoding argument')

    if buffering == 1:
        raise ValueError('line buffering not supported in binary mode')

    if not isinstance(buffering, int):
        raise TypeError('an integer is required (got type %s)' % type(buffering).__name__)

    raw = QueueIO(q, maxsize=maxsize, timeout=timeout)
    buf = raw

    if buffering != 0:
        buf = BufferedWriter(raw, buffer_size=DEFAULT_BUFFER_SIZE if buffering < 2 else buffering)

    return buf

class QueueIO(RawIOBase):
    def __init__(self, q, maxsize=10, timeout=None):
        self.queue = q
        self.maxsize = q.maxsize or maxsize
        self.timeout = timeout
        self.timed_out=False

        if self.maxsize == 1:
            raise ValueError('maxsize must be either 0 or greater than 1')


    def write(self, b):
        #print(f'write {len(b)}', file=stderr)
        t1, t2 = time(), time()

        try:
            if self.maxsize != 0:
                wait=0.000001
                #wait=1
                # always keep room to store one exception
                while (self.timeout == None or t2 - t1 < self.timeout) and self.queue.qsize() >= self.maxsize - 1 and not self.closed:
                    #print(f'waiting ... {t2-t1}', file=stderr)
                    sleep(wait)
                    t2 = time()

                    if wait < 0.001:
                        wait *= 2
                        print(wait, file=stderr)

            if self.closed:
                raise ValueError('I/O operation on closed stream')

            if self.timeout and t2 - t1 >= self.timeout:
                e = TimeoutError(f'{t2-t1} > {self.timeout}')
                self.queue.put_nowait(e)

                raise e

            # put *should* never block unless queue is manipulated
            # somewhere else
            self.queue.put_nowait(bytes(b))
        except Exception as e:
            self.close()
            raise e

        return len(b)


    def flush(self):
        ...


    def writable(self):
        return True


    def readable(self):
        return False


    def close(self):
        if not self.closed and not self.queue.full():
            self.queue.put_nowait(None)

        super().close()