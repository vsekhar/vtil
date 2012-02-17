import os
import shutil
from cStringIO import StringIO

class TransactionWriter(object):
    '''
    Allows undoing writes against a file_obj that does not support seek or
    truncate by buffering writes until a commit(). No commit == no write to file_obj.
    
    Usage:
        sio = StringIO()
        writer = TransactionWriter(sio)
        with writer:
            writer.write('123')
            writer.write('456')
        sio.getvalue() # ''
        with writer:
            writer.write('78')
            writer.commit()
        sio.getvalue() # '78'
        with writer:
            writer.write('abc')
            writer.commit()
            writer.write('xyz')
        sio.getvalue() # '78abc'
    '''
    def __init__(self, file_obj):
        self._file_obj = file_obj
        self._txnbuffer = None
    
    def __enter__(self):
        self._txnbuffer = StringIO()
    def __exit__(self, et, ev, tb):
        self._txnbuffer = None
        return False

    def commit(self):
        self._file_obj.write(self._txnbuffer.getvalue())
        self._txnbuffer = StringIO()
    
    def write(self, data):
        if self._txnbuffer is not None:
            self._txnbuffer.write(data) # in txn
        else:
            self._file_obj.write(data)
    
    def mem_use(self):
        return self._txnbuffer.tell() if self._txnbuffer is not None else 0

class TransactionReader(object):
    '''
    Allows undoing reads against a file_obj that does not support seek.
    
    Usage:
        sio = String('1234567890')
        sio.seek(0)
        reader = TransactionReader(sio)
        with reader:
            data1 = reader.read(2) # '12'
            data2 = reader.read(5) # '34567'
            data3 = reader.read() # '890'
        with reader:
            data4 = reader.read(2) # '12'
            reader.commit()
        with reader:
            data5 = reader.read(2) # '34' 
    '''
    def __init__(self, file_obj):
        self._file_obj = file_obj
        self._buffer = StringIO()
    
    def __enter__(self): pass
    def __exit__(self, et, ev, tb):
        # cache un-committed data for later reads
        self._buffer.seek(0, os.SEEK_SET)
        return False

    def commit(self, n=None):
        pos = self._buffer.tell()
        if n is None:
            n = pos
        self._buffer.seek(n, os.SEEK_SET)
        new_buffer = StringIO()
        shutil.copyfileobj(self._buffer, new_buffer)
        self._buffer = new_buffer

    def read(self, n=None):
        if n is None:
            bdata = self._buffer.read()
            new_data = self._file_obj.read()
            self._buffer.write(new_data)
            return bdata + new_data
        else:
            bdata = self._buffer.read(n)
            if len(bdata) < n:
                new_data = self._file_obj.read(n-len(bdata))
                self._buffer.write(new_data)
                return bdata + new_data
            else:
                return bdata

    def readline(self):
        data = self._buffer.readline()
        if data[-1] == '\n':
            return data
        else:
            return data + self._file_obj.readline()
    
    def mem_use(self):
        pos = self._buffer.tell()
        self._buffer.seek(0, os.SEEK_END)
        size = self._buffer.tell()
        self._buffer.seek(pos, os.SEEK_SET)
        return size
