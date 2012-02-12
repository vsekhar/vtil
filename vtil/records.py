'''
Created on Jan 18, 2012

@author: vsekhar
'''

import os
import re
import cPickle
from functools import partial
from cStringIO import StringIO

from vtil import exception

SENTINEL = '#S'
KB = 2 ** 10
READ_BUFFER_SIZE = 32 * KB

# when scanning look for #S, but NOT ##S
sentinel_pattern = '(?<!#)#S' # look for a non-duplicated #, then S
sentinel_re = re.compile(sentinel_pattern)
find_sentinel = sentinel_re.search

# when writing replace single # with double ##
write_pattern = '#'
write_re = re.compile(write_pattern)
fix_write = partial(write_re.sub, '##')

# when reading, replace double ## with single #
read_pattern = '##'
read_re = re.compile(read_pattern)
fix_read = partial(read_re.sub, '#') 

class RecordWriter(object):
    def __init__(self, stream):
        self._stream = stream
        self._write_buffer = None

    def __enter__(self):
        self._write_buffer = StringIO()
        return self
    
    def __exit__(self, et, ex, tb):
        if self._write_buffer.tell():
            self._stream.write(SENTINEL) # start
            cPickle.dump(self._write_buffer.tell(), self._stream, cPickle.HIGHEST_PROTOCOL) # byte length of user's original data
            self._stream.write(fix_write(self._write_buffer.getvalue()))
            self._write_buffer = None
        return False

    def write(self, data):
        if not self._write_buffer:
            raise TypeError("Must use RecordWriter as a context manager")
        self._write_buffer.write(data)

class BadBlock(Exception): pass

def verify_length(block):
    fobj = StringIO(block)
    try:
        stated_length = cPickle.load(fobj)
    except (ValueError, IndexError, cPickle.UnpicklingError):
        raise BadBlock
    data = fobj.read()
    if len(data) != stated_length:
        raise BadBlock
    return data

def RecordReader(stream):
    ' Read one record '
    accum = StringIO()
    seen_sentinel = False
    data = ''
    while True:
        m = find_sentinel(data)
        if not m:
            if seen_sentinel:
                accum.write(data)
            data = stream.read(READ_BUFFER_SIZE)
            if not data:
                if accum.tell():
                    with exception.swallowed(BadBlock):
                        yield verify_length(fix_read(accum.getvalue()))
                return
        else:
            if seen_sentinel:
                accum.write(data[:m.start()])
                with exception.swallowed(BadBlock):
                    yield verify_length(fix_read(accum.getvalue()))
                accum = StringIO()
            else:
                seen_sentinel = True
            data = data[m.end():] # toss

if __name__ == '__main__':
    import random

    stream = StringIO()
    data = [str(random.random()) for _ in xrange(3)]
    data.append('abc12#jeoht38#SoSooihetS#') # contains sentinel
    # TODO: add length/checksum
    count = len(data)
    for i in data:
        with RecordWriter(stream) as r:
            r.write(i)

    print 'Stream: '
    print stream.getvalue()
    size = stream.tell()
    stream.seek(0, os.SEEK_SET)
    read_data = [s for s in RecordReader(stream)]
    print 'Original data: ', data
    print 'RecordReader returned: ', read_data
    print '%d records read' % len(read_data)
    assert(len(read_data) == count)
    assert(data == read_data)

    for offset in xrange(size):
        print 'Values starting at offset %d:' % offset
        stream.seek(offset, os.SEEK_SET)
        for s in RecordReader(stream):
            print s
