'''
Created on Jan 18, 2012

@author: vsekhar
'''

import os
import re
import cPickle
import tempfile
import shutil
import binascii
from functools import partial
from cStringIO import StringIO
from contextlib import contextmanager
from collections import deque

from vtil import exception
from vtil.transaction import TransactionReader

SENTINEL = '#S'
KB = 2 ** 10
READ_BUFFER_SIZE = 32 * KB

# when scanning look for #S, but NOT ##S
# find_sentinel('abc12##Sjeoht38#SoSo') --> match(16, 18)
sentinel_pattern = '(?<!#)#S' # look for a non-duplicated #, then S
sentinel_re = re.compile(sentinel_pattern)
find_sentinel = sentinel_re.search

# when writing replace single # with double ##
# fix_write('abc#123#S##11') --> 'abc##123##S####11'
write_pattern = '#'
write_re = re.compile(write_pattern)
fix_write = partial(write_re.sub, '##')

# when reading, replace double ## with single #
# fix_read('abc##123##S####11') --> 'abc#123#S##11'
read_pattern = '##'
read_re = re.compile(read_pattern)
fix_read = partial(read_re.sub, '#')

class _write_only(object):
    def __init__(self, stream):
        self._stream = stream
        self.write = stream.write

@contextmanager
def RecordWriter(stream):
    buffer = StringIO()
    yield _write_only(buffer)
    if buffer.tell():
        buffer = buffer.getvalue()
        buffer = fix_write(buffer) # replace sentinels
        stream.write(SENTINEL)
        cPickle.dump(len(buffer), stream, cPickle.HIGHEST_PROTOCOL) # byte length of fixed data
        stream.write(buffer)
        cPickle.dump(binascii.crc32(buffer), stream, cPickle.HIGHEST_PROTOCOL)

class BadBlock(Exception): pass

def read_block(stream):
    try:
        length = cPickle.load(stream)
        data = stream.read(length)
        crc = cPickle.load(stream)
    except EOFError, cPickle.PickleError:
        raise BadBlock
    else:
        if crc == binascii.crc32(data):
            return data
        else:
            raise BadBlock

class FileTee(object):
    def __init__(self, *files):
        self._files = list(files)
    
    def write(self, data):
        [f.write(data) for f in self._files]

class LoggedReader(object):
    def __init__(self, stream):
        self._stream = stream
        self._buffer = StringIO()
        self._buffer_pos = 0
    
    def read(self, n=None):
        if n is None:
            data = self._buffer.getvalue() + self._stream.read()
        else:
            self._buffer.seek(self._buffer_pos)
            bdata = self._buffer.read(n)
            sdata = self._stream.read(n-len(bdata))
            
            self._buffer.write(sdata)
        return data
    
    def unread(self, bytes):
        self._prereader.unread(bytes)
        
    
    def log(self):
        return self._buffer.getvalue()
    
    def clear(self):
        self._buffer = StringIO()
    def reset(self): self.clear()

def RecordReader2(stream):
    at_beginning = True
    reader = PreReader(stream)
    while True:
        pre_load = reader.read(len(SENTINEL))
        if not pre_load:
            break
        m = find_sentinel(pre_load)
        if m is not None: # found sentinel
            logged_reader = LoggedReader(stream)
            length = cPickle.load(logged_reader)
            data = logged_reader.read(length)
            crc = cPickle.load(logged_reader)
            if length == len(data) and crc == binascii.crc32(data):
                yield data
            else:
                pass
        reader.unread(pre_load[1:])

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
            print m.start(), m.end()
            if seen_sentinel: # ending a block, write it
                accum.write(data[:m.start()])
                with exception.swallowed(BadBlock):
                    yield verify_length(fix_read(accum.getvalue()))
                accum = StringIO()
            else: # starting a block
                seen_sentinel = True
            data = data[m.end():] # toss
