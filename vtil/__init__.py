__version__ = '0.1'

import sys
import itertools

def fixed_int(value, fixed_size = sys.getsizeof(int()) * 2):
    ' Return an integer as a string of fixed length, pre-padding with zeros if needed '
    s = str(value)
    if len(s) > fixed_size:
        raise ValueError('fixed_int cannot represent %d with fixed_length %d' % (value, fixed_size))
    l = ['0' for _ in xrange(fixed_size - len(s))]
    l.append(s)
    ret = ''.join(l)
    assert(len(ret) == fixed_size)
    return ret
