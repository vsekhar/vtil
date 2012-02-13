import cPickle
import tempfile
import heapq

from operator import itemgetter 

from vtil.chunk import mem_chunks
from vtil import exception
from vtil import sorting

MEG = 2**20
DEFAULT_MAX_MEM = 64 * MEG

def _sortedfilesreader(files, key=None, reverse=False):
    ' read from sorted files, in order '
    wrapper = sorting.SortingWrapper(key=key, reverse=reverse)
    files = [(wrapper(cPickle.load(tf)), tf) for tf in files]
    heapq.heapify(files)
    value, tf = heapq.heappop(files)
    with exception.swallowed(IndexError):
        while True:
            yield wrapper.unwrap(value)
            try:
                value = wrapper(cPickle.load(tf))
            except EOFError:
                value, tf = heapq.heappop(files)
            else:
                value, tf = heapq.heappushpop(files, (value,tf))

def dump_to_tempfile(iterable):
    tf = tempfile.TemporaryFile()
    [cPickle.dump(obj, tf, protocol=cPickle.HIGHEST_PROTOCOL) for obj in iterable]
    return tf

def extsorted(iterable, key=None, reverse=False, max_mem=DEFAULT_MAX_MEM):
    '''
    Generator taking an iterable and returning its sorted values. 
    
    Uses an external sort behind the scenes, so *iterable* can produce more
    values than can fit in memory.
    
    Maximum memory usage (in bytes) can be provided as max_mem. The default
    is 64 megabytes.
    '''
    tempfiles = [dump_to_tempfile(sorted(block, key=key, reverse=reverse))
                 for block in mem_chunks(iterable, max_mem)]

    [tf.seek(0) for tf in tempfiles]
    return _sortedfilesreader(tempfiles, key=key, reverse=reverse)
