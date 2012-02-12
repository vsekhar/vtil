import os

class RangeReader(object):
    '''
    A seekable file-like object that reads a range of another seekable file-like
    object.
    
    If *end* is not specified, then the end of the file is assumed.
    
    If *hard_end* is True and *end* is specified, then no reads past the given
    end are permitted. Otherwise reads past the end work, but eof() will then
    return True.
    
    If *rebase* is True, then all offsets used by seek() and tell() will be
    zero-based (from the *start* position). Otherwise, the regular offsets of
    the underlying file will be used.
    
    Seeks or reads that would access points in the file prior to *start* are
    never permitted.
    '''
    def __init__(self, file_obj, start, end=None, rebase=False, hard_end=False):
        self._file_obj = file_obj
        self._start = start
        self._end = end
        self._rebase = rebase
        self._hard_end = hard_end
        file_obj.seek(start, os.SEEK_SET)
    
    def read(self, n=-1):
        if n < 0:
            if self._hard_end and self._end is not None:
                return self._file_obj.read(self._end - self._file_obj.tell())
            else:
                return self._file_obj.read()
        else:
            if self._hard_end and self._end is not None:
                n = min(n, self._end - self._file_obj.tell())
            return self._file_obj.read(n)
    
    def tell(self):
        if self._rebase:
            return self._file_obj.tell() - self._start
        else:
            return self._file_obj.tell()
    
    def seek(self, n, rel=os.SEEK_SET):
        if rel == os.SEEK_SET:
            if self._rebase:
                n += self._start
        cur_pos = self._file_obj.tell()
        self._file_obj.seek(n, rel)
        if self._file_obj.tell() < self._start:
            self._file_obj.seek(cur_pos)
            raise ValueError('cannot seek past beginning of RangeReader (start is %d, seeked to %d)' % (self._start, self._file_obj.tell()))
    
    def eof(self):
        if self._end is not None:
            return self._file_obj.tell() >= self._end
        else:
            check = self._file_obj.read(1)
            self._file_obj.seek(-1, os.SEEK_CUR)
            return bool(check)
    
    def close(self):
        self._file_obj.close()

if __name__ == '__main__':
    import random
    import StringIO
    
    sio = StringIO.StringIO()
    [sio.write(random.random()) for _ in xrange(1000)]
    size = sio.tell()

    # soft end
    sio.seek(0)
    rr = RangeReader(sio, 20, 50)
    assert len(rr.read()) == size-20
    assert rr.eof()
    
    # hard end
    sio.seek(0)
    rr = RangeReader(sio, 20, 50, hard_end=True)
    assert len(rr.read()) == 50-20
    assert rr.eof()

    # rebase absolute
    sio.seek(0)
    rr1 = RangeReader(sio, 20, 50, rebase=False)
    rr2 = RangeReader(sio, 20, 50, rebase=True)
    rr1.seek(20)
    d1 = rr1.read()
    rr2.seek(0)
    d2 = rr2.read()
    assert d1 == d2
    assert rr1.eof() and rr2.eof()

    # rebase relative
    rr1.seek(20)
    rr1.seek(2, os.SEEK_CUR)
    d1 = rr1.read()
    rr2.seek(0)
    rr2.seek(2, os.SEEK_CUR)
    d2 = rr2.read()
    assert d1 == d2
    assert rr1.eof() and rr2.eof()

    # no end specified
    sio.seek(0)
    rr = RangeReader(sio, 20, rebase=True)
    assert len(rr.read()) == size-20

    # fixed quantity read
    rr.seek(0)
    assert len(rr.read(5)) == 5

    # seek past beginning
    sio.seek(0)
    rr = RangeReader(sio, 20, 50, rebase=False)
    try:
        rr.seek(0)
        assert False # should never happen
    except ValueError:
        pass
