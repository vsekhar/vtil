import cPickle
import tempfile
import sys
import itertools

from operator import itemgetter 

from vtil import accum
from vtil import sortingpipe

MEG = 2**20
DEFAULT_MAX_MEM = 64 * MEG

def mem_chunked(iterable, max_mem=None):
    '''
    Generates and yields the longest lists of values from *iterable* that each
    fit inside *max_mem* of memory. If max_mem is not specified, mem_chunked
    yields a single list of all values in iterable.
    '''
    block = []
    mem_use = 0
    averager = accum.Averager()
    for value in iterable:
        block.append(value)
        mem_use += sys.getsizeof(value)
        averager(mem_use)
        if max_mem is not None and mem_use + averager.value > max_mem:
            yield block
            block = []
            mem_use = 0
            averager = accum.Averager()
    if block:
        yield block

def sortedfilesreader(files, key=None, reverse=False):
    '''
    A generator yielding values from a collection of sorted files in order.

    The files must be sorted using the same *key* and *reverse* settings provided
    here, otherwise this generator will yield erroneous results.
    '''
    key = key or (lambda x:x)
    wrapped_key = lambda x:key(operator.itemgetter(0)(x))
    sp = sortingpipe.sortingPipe(key=wrapped_key, reverse=reverse)
    [sp.push((cPickle.load(tf), tf)) for tf in files]
    while sp:
        value,tf = sp.pop()
        yield value
        try:
            sp.push((cPickle.load(tf), tf))
        except EOFError:
            pass

def extsorted(iterable, key=None, reverse=False, max_mem=DEFAULT_MAX_MEM):
    '''
    Generator taking an iterable and returning its sorted values. 
    
    Uses an external sort behind the scenes, so *iterable* can produce more
    values than can fit in memory.
    
    Maximum memory usage (in bytes) can be provided as max_mem. The default
    is 64 megabytes.
    '''

    tempfiles = []
    for block in mem_chunked(iterable, max_mem):
        tf = tempfile.TemporaryFile()
        [cPickle.dump(obj, tf) for obj in sorted(block, key=key, reverse=reverse)]
        tempfiles.append(tf)
    
    [tf.seek(0) for tf in tempfiles]
    return sortedfilesreader(tempfiles)

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return itertools.izip(a, b)

if __name__ == '__main__':
    import random
    file_goal = 2.5
    count = int((DEFAULT_MAX_MEM * file_goal) / sys.getsizeof(random.random()))
    data = (random.random() for _ in xrange(count))
    s = set(a<b for a,b in pairwise(extsorted(data)))
    assert False not in s 