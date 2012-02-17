__version__ = '0.1'

import sys
import itertools

def fixed_int(value, fixed_size = sys.getsizeof(int()) * 2):
    ' Return an integer as a string of fixed length, pre-padding with zeros if needed '
    s = str(value)
    if len(s) > fixed_size:
        raise ValueError('fixed_int cannot represent %d with fixed_length %d' % (value, fixed_size))
    prefix = ''.join(['0' for _ in xrange(fixed_size- len(s))])
    assert(len(prefix+s) == fixed_size)
    return prefix+s

def wrap(iterable, start_idx, how_many):
    '''
    Read from a finite iterable starting at *start_idx* (zero-based), and looping
    until *how_many* items are read.
    '''
    return itertools.islice(itertools.cycle(iterable), start_idx, start_idx + how_many, 1)
