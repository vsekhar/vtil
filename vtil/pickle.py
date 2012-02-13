import cPickle

def PickleReader(stream):
    unpickle = cPickle.load
    try:
        while True:
            yield unpickle(stream)
    except EOFError:
        pass
