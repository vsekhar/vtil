'''
Created on Jan 14, 2012

@author: vsekhar
'''

import unittest
import collections
import random
import os
import tempfile
from types import NotImplementedType

import vtil
from vtil import randomtools

class UtilTest(unittest.TestCase):
    def test_fixed_int(self):
        s = '0000000021415'
        self.assertEqual(s, vtil.fixed_int(int(s), len(s)))

class PartitionTest(unittest.TestCase):
    def test_partition(self):
        p = vtil.Partitioner(6)
        self.assertEqual(6, len(p))
        self.assertRaises(NotImplementedError, p, 'abc123')

    def isUniform(self, partitioner, delta=0.2, rfunc=None):
        def def_rfunc(): return randomtools.random_string(8)
        rfunc = rfunc or def_rfunc
        samples = 10000
        buckets = len(partitioner)

        expected_count = float(samples) / buckets

        c = collections.Counter((partitioner(rfunc()) for _ in xrange(samples)))
        for _, count in c.most_common():
            self.assertAlmostEqual(expected_count, count, delta=expected_count*delta)

    def test_hash_partitioner(self):
        h = vtil.HashPartitioner(6)
        self.assertEqual(len(h), 6)
        self.isUniform(h, 0.1)

    def test_string_partitioner(self):
        h = vtil.StringPartitioner(6)
        self.assertEqual(len(h), 6)
        self.isUniform(h, 0.2)
    
    def test_number_partitioner(self):
        h = vtil.NumberPartitioner(0, 1, 6)
        self.assertEqual(len(h), 6)
        self.isUniform(h, 0.1, rfunc = random.random)

class IndexedTest(unittest.TestCase):
    def test_indexed(self):
        from vtil.indexed import IndexedKVWriter, IndexedKVReader
        import random
        tf = tempfile.TemporaryFile()
        with IndexedKVWriter(tf, reverse=True) as writer:
            for _ in xrange(20):
                writer.write(random.random(), random.random())
        tf.seek(0)
        print "Descending order:"
        with IndexedKVReader(tf) as reader:
            reader.read_index()
            assert(len(reader) == 20)
            for k,v in reader:
                print k,v

class extsortedTest(unittest.TestCase):
    def test_extsorted(self):
        import sys
        import vtil.extsorted
        from vtil.iterator import pairwise
        file_size = 2 ** 20 # 1 MB
        file_goal = 5.5
        count = int((file_size * file_goal) / sys.getsizeof(random.random()))
        data = (random.random() for _ in xrange(count))
        sorted_data = vtil.extsorted.extsorted(data)
        s = set(a<b for a,b in pairwise(sorted_data))
        self.assertNotIn(False, s)
