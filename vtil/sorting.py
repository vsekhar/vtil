import operator

class _SortingWrapperInner(object):
    def __init__(self, obj, key, reverse):
        self._key = key if key is not None else lambda x:x
        self._cmp = operator.gt if reverse else operator.lt
        self.obj = obj

    def __lt__(self, other):
        return self._cmp(self._key(self.obj), self._key(other.obj))

class SortingWrapper(object):
    def __init__(self, key=None, reverse=False):
        self._key = key
        self._reverse = reverse

    def __call__(self, obj):
        if self._key is None and not self._reverse:
            return obj
        else:
            return _SortingWrapperInner(obj, self._key, self._reverse)

    def unwrap(self, obj):
        return obj if self._key is None and not self._reverse else obj.obj
