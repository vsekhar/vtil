import itertools
import threading
import Queue
import sys

from vtil import accum
from vtil import exception

def wrap_around(iterable, start_idx, how_many):
    '''
    Read from a finite iterable starting at *start_idx* (zero-based), and looping
    until *how_many* items are read.
    '''
    return itertools.islice(itertools.cycle(iterable),
                            start_idx,
                            start_idx + how_many,
                            1)

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return itertools.izip(a, b)

def _counted_all_inner(iterable):
    ' kludge: insert a string so sum() will raise TypeError '
    return ((1 if bool(i) else 'FAIL') for i in iterable)

def counted_all(iterable, expected_len=None):
    try:
        s = sum(_counted_all_inner(iterable))
    except TypeError:
        return False
    else:
        if expected_len is None or s == expected_len: return True
        else: return False

def chunks(n, iterable):
    "chunks(4, [1,2,3,4,5,6]) --> 1234 56"
    itr = iter(iterable)
    while True:
        ret = tuple(itertools.islice(itr, n))
        if ret: yield ret
        else: break

def padded_chunks(n, iterable, fillvalue=None):
    "padded_chunks(4, [1,2,3,4,5,6], '-') --> 1234 56--"
    return itertools.izip_longest(fillvalue=fillvalue, *[iter(iterable)]*n)

def full_chunks_only(n, iterable):
    "full_chunks_only(4, [1,2,3,4,5,6]) --> 1234"
    return itertools.izip(*[iter(iterable)]*n)

def mem_chunks(iterable, max_mem=None):
    '''
    Generates and yields the longest lists of values from *iterable* that each
    fit inside *max_mem* of memory. If max_mem is not specified, mem_chunked
    yields a single list of all values in *iterable*.

    List overhead is not considered. If a single value is larger than *max_mem*
    it is still returned, but in its own list.
    '''
    mem_use = 0
    sizeof = sys.getsizeof
    block = list()
    for value in iterable:
        if max_mem is not None:
            size = sizeof(value)
            if mem_use + size <= max_mem:
                block.append(value)
                mem_use += size
            elif block:
                yield block
                block = [value]
                mem_use = size
            else:
                yield [value]
        else:
            block.append(value)
    if block:
        yield block

class _STOPTYPE(object): pass
_STOP = _STOPTYPE()

def _load_thread(iterable, q):
    [q.put(obj) for obj in iterable]
    q.put(_STOP)

def _load_thread_mem(iterable, q, lock, not_empty, not_full, max_mem):
    averager = accum.Averager()
    sizeof = sys.getsizeof
    for value in itertools.chain(iterable, [_STOP]):
        averager(sizeof(value))
        with lock:
            if (q.qsize()+1) * averager.value > max_mem:
                not_full.wait()
            while True:
                try:
                    q.put_nowait(value)
                    not_empty.notify()
                    break
                except Queue.Full:
                    not_empty.notify()
                    not_full.wait()

def threaded(iterable, max_count=None, max_mem=None):
    '''
    Loads values from *iterable* in a background thread and yields them.

    If *max_count* is specified, only that many values will be loaded.

    If *max_mem* is specified, only values occupying less than that memory
    will be loaded.

    This is useful for wrapping iterators that perform IO. Though the benefit is
    small since it introduces pickling overhead (in the queue communication between
    threads). So the IO being wrapped should be very slow (e.g. network, not disk).
    
    TODO: Tests
    '''
    q = Queue.Queue(maxsize=max_count)
    if max_mem is None:
        target=_load_thread
        args = (iterable, q)
    else:
        lock = threading.Lock()
        not_empty = threading.Condition(lock)
        not_full = threading.Condition(lock)
        target=_load_thread_mem
        args = (iterable, q, lock, not_empty, not_full, max_mem)
    thread = threading.Thread(target=target, args=args)
    thread.start()
    while True:
        if max_mem is not None:
            with lock:
                try:
                    val = q.get_nowait()
                    not_full.notify()
                except Queue.Empty:
                    not_full.notify()
                    not_empty.wait()
                    continue
        else:
            val = q.get()
        if val is _STOP:
            raise StopIteration
        yield val

if __name__ == '__main__':
    import random
    import time
    g1 = (random.random() for _ in xrange(100))
    g2 = threaded(g1, max_mem=100)
    for val in g2:
        print val
