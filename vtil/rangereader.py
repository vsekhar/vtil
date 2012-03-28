import os

class RangeReader(object):
    '''
    A seekable file-like adaptor that reads a range of another seekable file-like
    object.
    
    If *end* is not specified, then the end of *file_obj* is assumed.
    
    If *hard_end* is True and *end* is specified, then no reads past the given
    end are permitted. This makes the file appear to be genuinely truncated to
    the range given. Otherwise reads past the end work (the default), and a
    'soft' end is indicated by eof() returning True.
    
    If *rebase* is True, then all offsets used by seek() and tell() will be
    zero-based (from the *start* position). Otherwise, the regular offsets of
    the underlying *file_obj* will be used.
    
    Seeks or reads that would access points in the file prior to *start* are
    never permitted. If *start* is beyond the end of *file_obj*, all reads will
    return an empty string indicating EOF.
    '''
    def __init__(self, file_obj, start, end=None, rebase=False, hard_end=False):
        if end is not None and end < start:
            raise ValueError('RangeReader: end (%d) cannot be before start (%d)' % (end, start))
        self._file_obj = file_obj
        self._start = start
        self._end = end
        self._rebase = rebase
        self._hard_end = hard_end
        file_obj.seek(start, os.SEEK_SET)
    
    def read(self, n=None):
        if self._hard_end and self._end is not None:
            if n is not None and n >= 0: # checked "read range"
                n = min(n, self._end - self._file_obj.tell())
            else: # computed "read to end"
                n = self._end - self._file_obj.tell()
        if n is not None:
            return self._file_obj.read(n)
        else:
            return self._file_obj.read()
    
    def readline(self):
        if self._hard_end and self._end is not None:
            bytes_left = self._end - self._file_obj.tell()
            if bytes_left > 0:
                line = self._file_obj.readline()
                self._file_obj.seek(min(bytes_left-len(line), 0), os.SEEK_CUR)
                return line[:bytes_left]
            else:
                return ''
        else:
            return self._file_obj.readline()

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
            if check:
                self._file_obj.seek(-1, os.SEEK_CUR)
                return False
            else:
                return True
