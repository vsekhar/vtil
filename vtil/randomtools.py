'''
Created on Jan 14, 2012

@author: vsekhar
'''

import random as _random
import cPickle
import string

def chunkgen(num, chunksize):
    ''' Yield chunk counts up to total of num '''
    cur = num
    while cur:
        y = min(cur, chunksize)
        yield y
        cur -= y

def write_random(num, fobj):
    [cPickle.dump(_random.random(), fobj, cPickle.HIGHEST_PROTOCOL)
        for count in chunkgen(num, 50)
            for _ in xrange(count)]

chars = string.ascii_letters + string.digits

def random_string(length):
    return ''.join([_random.choice(chars) for _ in xrange(length)])
