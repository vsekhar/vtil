import cPickle
import tempfile
import heapq
import itertools

from operator import itemgetter

from vtil.chunk import mem_chunks
from vtil.pickle import PickleReader
from vtil import exception
from vtil import sorting

MEG = 2**20
DEFAULT_MAX_MEM = 64 * MEG


def _sortedfilesreader(files, key=None, reverse=False):
    ' read from sorted files, in order '
    wrap, unwrap = sorting.make_wrap_funcs(key=key, reverse=reverse)
    gens = (itertools.imap(wrap, PickleReader(tf)) for tf in files)
    return (unwrap(obj) for obj in heapq.merge(*gens))

def _dump_to_tempfile(iterable):
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
    tempfiles = [_dump_to_tempfile(sorted(block, key=key, reverse=reverse))
                 for block in mem_chunks(iterable, max_mem)]

    [tf.seek(0) for tf in tempfiles]
    return _sortedfilesreader(tempfiles, key=key, reverse=reverse)
