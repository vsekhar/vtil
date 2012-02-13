import itertools
import sys

from vtil import accum

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
    yields a single list of all values in iterable.

    List overhead is not considered.
    '''
    block = []
    mem_use = 0
    averager = accum.Averager()
    sizeof = sys.getsizeof
    for value in iterable:
        block.append(value)
        size = sizeof(value)
        mem_use += size
        averager(size)
        if max_mem is not None and mem_use + averager.value > max_mem:
            yield block
            block = []
            mem_use = 0
            averager = accum.Averager()
    if block:
        yield block
