'''
Created on Jan 15, 2012

@author: vsekhar
'''

import heapq
import operator

from functools import partial

class sortingPipe(object):
    def __init__(self, key=None, reverse=False):
        self._heap = []
        self._key = key
        self._reverse = reverse
        self.wrap = partial(self.ReverseWrapper, self._key) if reverse else lambda x:x
        self.unwrap = (lambda x: x.obj) if self._reverse else lambda x:x
    
    class ReverseWrapper(object):
        def __init__(self, key, obj):
            self._key = key or (lambda x:x)
            self.obj = obj
        def __lt__(self, other):
            return operator.gt(self._key(self.obj), self._key(other.obj))
    
    def __len__(self): return len(self._heap)
    def __bool__(self): return bool(self._heap)
    def __iter__(self): return self
    def next(self):
        try: return self.pop()
        except IndexError: raise StopIteration
    
    def push(self, obj):
        heapq.heappush(self._heap, self.wrap(obj))
    
    def pop(self):
        return self.unwrap(heapq.heappop(self._heap))

if __name__ == '__main__':
    import random
    
    iterations = 100
    
    s = sortingPipe()
    [s.push(random.random()) for _ in xrange(iterations)]
    for x in s:
        print x

    print '--'

    s = sortingPipe(reverse=True)
    [s.push(random.random()) for _ in xrange(iterations)]
    for x in s:
        print x
    