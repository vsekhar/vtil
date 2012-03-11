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

class _CRCFail(Exception): pass
class _NoSentinel(Exception): pass
class RecordReadError(Exception): pass

def RecordReader(stream, tolerate_pre_error=True, tolerate_subsequent_error=False):
    got_first = False
    reader = TransactionReader(stream)
    while True:
        with reader:
            sen_check = reader.read(len(SENTINEL))
            if not sen_check:
                break # eof
            try:
                if find_sentinel(sen_check) is None:
                    raise _NoSentinel
                length = cPickle.load(reader)
                data = reader.read(length)
                crc = cPickle.load(reader)
                if crc != binascii.crc32(data) or length != len(data):
                    raise _CRCFail
                reader.commit()
                got_first = True
                yield fix_read(data)
            except (EOFError, cPickle.UnpicklingError, _NoSentinel, _CRCFail):
                if not got_first and not tolerate_pre_error:
                    raise RecordReadError
                elif got_first and not tolerate_subsequent_error:
                    raise RecordReadError
                else:
                    reader.commit(1)
