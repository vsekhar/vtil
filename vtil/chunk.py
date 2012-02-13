import itertools
import sys

from vtil import accum

def chunks(n, iterable):
	"chunkify(4, [1,2,3,4,5,6]) --> 1234 56"
	itr = iter(iterable)
	while True:
		ret = tuple(itertools.islice(itr, n))
		if ret: yield ret
		else: break

def padded_chunks(n, iterable, fillvalue=None):
	"chunkify_pad(4, [1,2,3,4,5,6], '-') --> 1234 56--"
	return itertools.izip_longest(fillvalue=fillvalue, *[iter(iterable)]*n)

def full_chunks_only(n, iterable):
	"chunkify_drop(4, [1,2,3,4,5,6]) --> 1234"
	return itertools.izip(*[iter(iterable)]*n)

def mem_chunks(iterable, max_mem=None):
    '''
    Generates and yields the longest lists of values from *iterable* that each
    fit inside *max_mem* of memory. If max_mem is not specified, mem_chunked
    yields a single list of all values in iterable.
    '''
    block = []
    mem_use = 0
    averager = accum.Averager()
    sizeof = sys.getsizeof
    for value in iterable:
        block.append(value)
        mem_use += sizeof(value)
        averager(mem_use)
        if max_mem is not None and mem_use + averager.value > max_mem:
            yield block
            block = []
            mem_use = 0
            averager = accum.Averager()
    if block:
        yield block