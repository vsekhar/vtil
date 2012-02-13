import operator

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
        def unwrap(obj): return obj.obj
    else:
        def wrap(obj): return obj
        def unwrap(obj): return obj
    return wrap, unwrap
