'''
Created on Jan 14, 2012

@author: vsekhar
'''

import random as _random
import cPickle
import string

from itertools import ifilter, product, imap, izip

from operator import itemgetter

lists = (string.uppercase, string.lowercase, string.digits, string.punctuation, string.whitespace)
char_bins = dict()
for l in product((True, False), repeat=5):
    gen = imap(itemgetter(1), ifilter(itemgetter(0), izip(l, lists)))
    char_bins[l] = ''.join(gen)

def random_string(length, uppercase=True, lowercase=True, digits=False, punctuation=False, whitespace=False):
    sig = (uppercase, lowercase, digits, punctuation, whitespace)
    return ''.join([_random.choice(char_bins[sig]) for _ in xrange(length)])
