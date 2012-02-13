'''
Created on Jan 15, 2012

@author: vsekhar
'''

import threading
import Queue

STOP = object()

def runner(inqueue, outqueue, func):
    while True:
        data = inqueue.get(block=True)
        if data is STOP: break
        try:
            out = func(data)
        except KeyboardInterrupt:
            break
        except Exception as e:
            outqueue.put(e)
        else:
            outqueue.put(out)

class ThreadPool(object):
    def __init__(self, num, func):
        self._callable = callable
        self._threads = []
        self._inqueue = Queue.Queue()
        self._outqueue = Queue.Queue()
        for _ in xrange(num):
            self._threads.append(threading.Thread(target=runner, args=(self._inqueue, self._outqueue, func)))
    
    def __iter__(self): return self
    def next(self):
        try:
            return self._outqueue.get(block=False)
        except Queue.Empty:
            raise StopIteration
    
    def push(self, data):
        self._inqueue.put(data)
    
    def join(self):
        [self.push(STOP) for _ in self._threads]
        [thread.join() for thread in self._threads]
    
    def map(self, iterable):
        [self._inqueue.put(i) for i in iterable]
    
    def start(self):
        for thread in self._threads:
            thread.start()
    
def myprint(data): print data # print is a statement...

if __name__ == '__main__':
    pool = ThreadPool(5, myprint)
    for i in xrange(50):
        pool.push(i)
    pool.start()
    pool.join()
