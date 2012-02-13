'''
Created on Jan 15, 2012

@author: vsekhar
'''

import heapq
import operator

from functools import partial

from vtil import sorting

class sortingPipe(object):
    def __init__(self, key=None, reverse=False):
        self._heap = []
        self._wrap, self._unwrap = sorting.make_wrap_funcs(key=key, reverse=reverse)

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
