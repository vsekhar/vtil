import operator

from vtil.iterator import pairwise

class _SortingWrapperInner(object):
    def __init__(self, obj, key, reverse):
        self._key = key if key is not None else lambda x:x
        self._cmp = operator.gt if reverse else operator.lt
        self.obj = obj

    def __lt__(self, other):
        return self._cmp(self._key(self.obj), self._key(other.obj))

def make_wrap_funcs(key=None, reverse=False):
    if key is not None or reverse:
        def wrap(obj): return _SortingWrapperInner(obj, key, reverse)
        unwrap = operator.attrgetter('obj') 
    else:
        def wrap(obj): return obj
        def unwrap(obj): return obj
    return wrap, unwrap

def _counted_all_inner(iterable):
    ' kludge: insert a string so sum() will raise TypeError '
    return ((1 if bool(i) else 'FAIL') for i in iterable)

def counted_all(iterable, expected_len=None):
    try:
        s = sum(_counted_all_inner(iterable))
    except TypeError:
        return False
    else:
        if expected_len is None or s == expected_len: return True
        else: return False

def is_sorted(iterable, key=lambda x:x, reverse=False, expected_len=None):
    ' checks if an iterable is sorted (consumes the iterable) '
    if reverse:
        gen = (key(a) >= key(b) for a,b in pairwise(iterable))
    else:
        gen = (key(a) <= key(b) for a,b in pairwise(iterable))

    # pairwise results in one fewer value
    if expected_len is not None:
        expected_len -= 1

    return counted_all(gen, expected_len=expected_len)
