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
