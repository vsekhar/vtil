'''
Created on Jan 14, 2012

@author: vsekhar
'''

import random as _random
import cPickle
import string

from itertools import ifilter, product, imap, izip

from operator import itemgetter

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

lists = (string.uppercase, string.lowercase, string.digits, string.punctuation, string.whitespace)
char_bins = dict()
for l in product((True, False), repeat=5):
    print l
    gen = imap(itemgetter(1), ifilter(itemgetter(0), izip(l, lists)))
    char_bins[l] = ''.join(gen)

def random_string(length, uppercase=True, lowercase=True, digits=False, punctuation=False, whitespace=False):
    sig = (uppercase, lowercase, digits, punctuation, whitespace)
    return ''.join([_random.choice(char_bins[sig]) for _ in xrange(length)])
