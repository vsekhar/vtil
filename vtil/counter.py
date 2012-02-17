try:
    from collections import Counter # only in 2.7
except ImportError:
    # imitate some basic functionality for <2.7
    from operator import itemgetter
    from collections import defaultdict
    class Counter(defaultdict):
        def __init__(self, iterable):
            super(defaultdict, self).__init__()
            self._dict = defaultdict(lambda: 0)
            for item in iterable:
                self._dict[item] += 1

        def most_common(self):
            for k,v in sorted(self._dict.iteritems(), key=itemgetter(1), reverse=True):
                yield k,v
