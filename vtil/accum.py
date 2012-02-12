def make_accumulator(gen_type):
    class wrapper(object):
        def __init__(self):
            self._a = gen_type()
            self._last = self._a.next()
        
        def __call__(self, val=None):
            if val is None:
                return self._last
            else:
                return self.send(val)
        
        def __str__(self):
            return str(self._last)
        
        def next(self):
            self._last = self._a.next()
            return self._last
        
        def send(self, val):
            self._last = self._a.send(val)
            return self._last
        
        @property
        def value(self):
            return self._last

    return wrapper

@make_accumulator
def Averager():
    value = 0.0
    count = 0
    while True:
        try: value += yield value/count
        except ZeroDivisionError: value += yield None
        count += 1

@make_accumulator
def Summer():
    value = 0
    while True:
        value += yield value

if __name__ == '__main__':
    a = Averager()
    print a.send(4)
    print a.send(5)
    print a.send(9)
    print a(5)

    s = Summer()
    print s.send(5)
    print s.send(4)
    print s
