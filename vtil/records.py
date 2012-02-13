'''
Created on Jan 18, 2012

@author: vsekhar
'''

import os
import re
import cPickle
from functools import partial
from cStringIO import StringIO
from contextlib import contextmanager

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

class _RecordWriterWrapper(object):
    def __init__(self, stream):
        self._stream = stream
        self.write = stream.write

@contextmanager
def RecordWriter(stream):
    write_buffer = StringIO()
    yield _RecordWriterWrapper(write_buffer)
    if write_buffer.tell():
        stream.write(SENTINEL) # start
        cPickle.dump(write_buffer.tell(), stream, cPickle.HIGHEST_PROTOCOL) # byte length of user's original data
        stream.write(fix_write(write_buffer.getvalue())) # data with sentinels replaced

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
        if not m: # no sentinel in current block
            if seen_sentinel: # writing good data
                accum.write(data)
            data = stream.read(READ_BUFFER_SIZE) # read some more
            if not data: # no more, process what you have
                if accum.tell():
                    with exception.swallowed(BadBlock):
                        yield verify_length(fix_read(accum.getvalue()))
                return
        else: # sentinel in this block
            if seen_sentinel: # ending a block, write it
                accum.write(data[:m.start()])
                with exception.swallowed(BadBlock):
                    yield verify_length(fix_read(accum.getvalue()))
                accum = StringIO()
            else: # starting a block
                seen_sentinel = True
            data = data[m.end():] # toss
