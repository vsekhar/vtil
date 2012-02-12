'''
Created on Jan 16, 2012

@author: vsekhar
'''

import string

class Partitioner(object):
    def __init__(self, bucket_count):
        self._bucket_count = bucket_count
    def __call__(self, key):
        return self.partition(key)
    def __len__(self):
        return self._bucket_count
    def partition(self, key):
        raise NotImplementedError("Must subclass Partitioner and implement partition() function")

class HashPartitioner(Partitioner):
    ''' Partition to reducers based on the modulo hash of the ID string.
    
    DO NOT use this if you are trying to get sorted keys at the reducers as the
    hashing function is not order-preserving.
    '''

    def __init__(self, bucket_count):
        super(HashPartitioner, self).__init__(bucket_count)
    
    def partition(self, key):
        return hash(key) % self._bucket_count

char_lists = [string.ascii_letters, string.digits, string.punctuation, string.whitespace]
all_chars = [c for clist in char_lists for c in clist]

class StringPartitioner(Partitioner):
    ''' Partition based on the string 'value' '''
    
    def __init__(self, bucket_count):
        super(StringPartitioner, self).__init__(bucket_count)
        if self._bucket_count > len(all_chars):
            raise ValueError("Bucket count must be less than %d" % len(all_chars))
        
        # Create lookup dict by separately partitioning different types of
        # characters into buckets
        self.lookup = dict()
        for clist in char_lists:
            count = len(clist)
            fcount = float(count)
            for i in xrange(count):
                bucket = int((i/fcount)*self._bucket_count)
                self.lookup[clist[i]] = bucket

    def partition(self, key):
        return self.lookup[key[0]]

class NumberPartitioner(Partitioner):
    ''' Partition based on numerical value '''
    def __init__(self, low, high, bucket_count):
        super(NumberPartitioner, self).__init__(bucket_count)
        self._low = float(low)
        self._high = float(high)
    
    def partition(self, key):
        if key < self._low or key >= self._high:
            raise ValueError("Key out of bounds for NumberPartitioner: %d" % key)
        return int(((key-self._low) / (self._high-self._low)) * self._bucket_count)

