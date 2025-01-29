from io import RawIOBase,UnsupportedOperation,BufferedReader,TextIOWrapper,DEFAULT_BUFFER_SIZE
from sys import stderr

def open(it, mode='r', auth=None, buffering=-1, encoding=None):
    binary = mode[-1] == 'b'

    if mode[0] != 'r':
        raise Exception('only read-only streams supported')

    if binary:
        if encoding != None:
            raise ValueError('binary mode doesn\'t take an encoding argument')
    
        if buffering == 1:
            raise ValueError('line buffering not supported in binary mode')

    if not binary and buffering == 0:
        raise ValueError('can\'t have unbuffered text I/O')

    if not isinstance(buffering, int):
        raise TypeError('an integer is required (got type %s)' % type(buffering).__name__)

    raw = IterIO(it)
    buf = raw

    if buffering != 0:
        buf = BufferedReader(raw, buffer_size=DEFAULT_BUFFER_SIZE if buffering < 2 else buffering)

    return buf if binary else TextIOWrapper(buf, encoding)

class IterIO(RawIOBase):
    def __init__(self, i):
        self.it = iter(i)
        self.current = next(self.it)
        self.n = 0
 
    def read(self, n=-1):
        self._assertOpen()

        if self.current == None:
            return b''

        if n == -1:
            return self.readall()

        b = None
        ret = []
        while n > 0 and b != b'':
            if self.n == len(self.current):
                try:
                    self.current = next(self.it)
                except StopIteration:
                    self.current = None
                    break
                finally:
                    self.n = 0

            b = self.current[self.n : self.n + min(n, len(self.current) - self.n) ]
            self.n += len(b)
            n -= len(b)
            ret += [ b ]

        return b''.join(ret)

    def readinto(self, ba):
        b = self.read(len(ba))
        ba[:len(b)] = b

        return len(b)
        
    def readable(self):
        return True

    def readall(self):
        self._assertOpen()

        if self.current == None:
            return b''

        ret = b''.join([ self.current[self.n:len(self.current)] ] + [ x for x in self.it ])
        self.current = None
        self.n = 0
        
        return ret

    def seek(self, n, whence):
        self._assertOpen()

        raise Exception('Stream not seekable')

    def seekable(self):
        return False

    def writable(self):
        return False

    def _assertOpen(self):
        if self.closed:
            raise ValueError('I/O operation on closed stream')
