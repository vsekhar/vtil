import cPickle
import tempfile
import heapq
import itertools
import operator

from functools import partial

from vtil.iterator import pairwise, counted_all
from vtil.chunk import mem_chunks
from vtil.pickle import PickleReader

MEG = 2**20
DEFAULT_MAX_MEM = 64 * MEG

class _SortingWrapperInner(object):
    def __init__(self, obj, key, reverse):
        self._key = key if key is not None else lambda x:x
        self._cmp = operator.gt if reverse else operator.lt
        self.obj = obj

    def __lt__(self, other):
        return self._cmp(self._key(self.obj), self._key(other.obj))

def make_wrap_funcs(key=None, reverse=False):
    if key is not None or reverse:
        def wrap(obj): return _SortingWrapperInner(obj, key, reverse)
        unwrap = operator.attrgetter('obj') 
    else:
        def wrap(obj): return obj
        def unwrap(obj): return obj
    return wrap, unwrap

def is_sorted(iterable, key=lambda x:x, reverse=False, expected_len=None):
    ' checks if an iterable is sorted (consumes the iterable) '
    if reverse:
        gen = (key(a) >= key(b) for a,b in pairwise(iterable))
    else:
        gen = (key(a) <= key(b) for a,b in pairwise(iterable))

    # pairwise results in one fewer value
    if expected_len is not None:
        expected_len -= 1

    return counted_all(gen, expected_len=expected_len)

class sortingPipe(object):
    def __init__(self, key=None, reverse=False):
        self._heap = []
        self._wrap, self._unwrap = make_wrap_funcs(key=key, reverse=reverse)

    def __len__(self): return len(self._heap)
    def __bool__(self): return bool(self._heap)
    def __iter__(self): return self
    def next(self):
        try: return self.pop()
        except IndexError: raise StopIteration
    
    def push(self, obj):
        heapq.heappush(self._heap, self._wrap(obj))
    
    def pop(self):
        return self._unwrap(heapq.heappop(self._heap))

def _sortedfilesreader(files, key=None, reverse=False):
    ' read from sorted files, in order '
    wrap, unwrap = make_wrap_funcs(key=key, reverse=reverse)
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
    is 64 megabytes. This applies only to objects, additional memory will be
    used for container overheads, temporaries, etc.
    '''
    tempfiles = [_dump_to_tempfile(sorted(block, key=key, reverse=reverse))
                 for block in mem_chunks(iterable, max_mem)]

    [tf.seek(0) for tf in tempfiles]
    return _sortedfilesreader(tempfiles, key=key, reverse=reverse)
