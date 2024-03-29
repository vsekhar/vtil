'''
Created on Jan 16, 2012

@author: vsekhar
'''

import os
import cPickle
import tempfile
import shutil
import operator

from collections import deque

import vtil.exception
from vtil.sorting import sortingPipe

class IndexedKVWriter(object):
    ''' Writes (cPickles) key value pairs while also building an index, which is itself written upon close().
    
    The format of the resulting file is as follows (binary, no newlines):
        <n: cPickled number of entries>
        <Index: 'n' cPickled (key, pos) tuples...>
        <b: number of subsequent bytes containing values>
        <Values: 'n' cPickled values...>
    
    The pos is zero-based, starting from the start of the first value.
    
    TODO: Build read infrastructure:
          -block until all index portions are read
          -build and merge heapq from all indexes
          -pop from heapq, goto respective file, seek as needed (blocking) 
    '''
    
    def __init__(self, file_obj, reverse=False):
        ''' IndexedWriter takes ownership of file_obj (closes upon close()) '''
        self.file_obj = file_obj
        self.pickler = cPickle.Pickler(self.file_obj, cPickle.HIGHEST_PROTOCOL)
        self._value_file_obj = tempfile.TemporaryFile()
        self._value_pickler = cPickle.Pickler(self._value_file_obj, cPickle.HIGHEST_PROTOCOL)
        self._index = sortingPipe(key=operator.itemgetter(0), reverse=reverse)
    
    def __enter__(self): return self
    def __exit__(self, et, ex, tb):
        self.close()
        return False
    
    def write(self, key, value):
        ''' Adds key and value position to index, writes value '''
        pos = self._value_file_obj.tell()
        self._index.push((key,pos))
        self._value_pickler.dump(value)
        return self._value_file_obj.tell() - pos # bytes written
    
    def close(self):
        self.pickler.dump(len(self._index)) # number of elements
        [self.pickler.dump(o) for o in self._index] # index entries
        self._value_file_obj.flush()
        self.pickler.dump(self._value_file_obj.tell()) # number of value bytes
        self._value_file_obj.seek(0)
        shutil.copyfileobj(self._value_file_obj, self.file_obj) # copy values
        self._value_file_obj.close()
        self.file_obj.flush()
        #self.file_obj.close()

class IndexNotLoaded(Exception): pass
class Empty(Exception): pass

class IndexedKVReader(object):
    ''' Reads and returns key-value pairs from a file created with IndexedKVWriter '''
    def __init__(self, file_obj, read_index_now=False):
        self.file_obj = file_obj
        self.unpickler = cPickle.Unpickler(self.file_obj)
        self._index = None
        if read_index_now: self.read_index()
    
    __len__ = vtil.exception.convertedf(lambda x:len(x._index), TypeError, IndexNotLoaded)

    def __enter__(self): return self
    def __exit__(self, et, ex, tb): return False
    
    def __iter__(self):
        if not self._index:
            self.read_index()
        return self
    
    next = vtil.exception.convertedf(lambda x:x.get(), Empty, StopIteration)

    def read_index(self):
        count = self.unpickler.load()
        self._index = deque(maxlen=count)
        [self._index.append(self.unpickler.load()) for _ in xrange(count)]
        self._val_bytes = self.unpickler.load()
        self._val_start = self.file_obj.tell() # for rebasing
    
    def get(self):
        if self._index is None:
            raise IndexNotLoaded

        # convert IndexError to Empty (for granular exception catching)
        wrapped_func = vtil.exception.convertedf(self._index.popleft, IndexError, Empty)
        key, pos = wrapped_func()

        pos += self._val_start # rebase
        self.file_obj.seek(pos)
        value = self.unpickler.load()
        return key, value
