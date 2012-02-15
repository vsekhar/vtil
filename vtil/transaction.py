from cStringIO import StringIO

class TransactionWriter(object):
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
        return self._txnbuffer.tell()if self._txnbuffer is not None else 0

class TransactionReader(object):
    '''
    Allows undoing reads against a file_obj that does not support seek.
    
    Use as:
        sio = String()
        sio.write('1234567890')
        sio.seek(0)
        reader = TransactionReader(sio)
        with reader:
            data1 = reader.read(2) # '12'
            data2 = reader.read(5) # '34567'
            data3 = reader.read() # '890'
            raise TransactionReader.Cancel # 'undoes' all reads since entry
        with reader:
            data4 = reader.read(2) # '12'
        with reader:
            data5 = reader.read(2) # '34' 
    '''
    def __init__(self, file_obj):
        self._file_obj = file_obj
        self._preread = ''
        self._txnbuffer = StringIO()
    
    def __enter__(self): pass
    def __exit__(self, et, ev, tb):
        # cache data for later reads
        self._preread = self._preread + self._txnbuffer.getvalue()
        self._txnbuffer = StringIO()
        return False

    def commit(self):
        self._txnbuffer = StringIO()

    def read(self, n=None):
        if n is None:
            data = self._preread + self._file_obj.read()
            self._txnbuffer.write(data)
            return data
        else:
            pdata = self._preread[:n]
            self._preread = self._preread[n:]
            fdata = self._file_obj.read(n-len(pdata))
            self._txnbuffer.write(pdata + fdata)
            return pdata + fdata
    
    def mem_use(self):
        ret = len(self._preread)
        try: ret += self._txnbuffer.tell()
        except AttributeError: pass
        return ret
