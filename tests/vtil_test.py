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
        from vtil.iterator import pairwise
        import random
        tf = tempfile.TemporaryFile()
        with IndexedKVWriter(tf, reverse=True) as writer:
            for _ in xrange(20):
                writer.write(random.random(), random.random())
        tf.seek(0)
        for (k1,_),(k2,_) in pairwise(IndexedKVReader(tf)):
            self.assertGreaterEqual(k1, k2, 'key sort failed')
        tf.seek(0)
        self.assertEqual(20, len(iter(IndexedKVReader(tf))), 'length does not match')

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

class RangeReaderTest(unittest.TestCase):
    def test_rangereader(self):
        import random
        import StringIO
        from vtil.rangereader import RangeReader

        sio = StringIO.StringIO()
        [sio.write(random.random()) for _ in xrange(1000)]
        size = sio.tell()

        # soft end
        sio.seek(0)
        rr = RangeReader(sio, 20, 50)
        assert len(rr.read()) == size-20
        assert rr.eof()

        # hard end
        sio.seek(0)
        rr = RangeReader(sio, 20, 50, hard_end=True)
        assert len(rr.read()) == 50-20
        assert rr.eof()

        # rebase absolute
        sio.seek(0)
        rr1 = RangeReader(sio, 20, 50, rebase=False)
        rr2 = RangeReader(sio, 20, 50, rebase=True)
        rr1.seek(20)
        d1 = rr1.read()
        rr2.seek(0)
        d2 = rr2.read()
        assert d1 == d2
        assert rr1.eof() and rr2.eof()

        # rebase relative
        rr1.seek(20)
        rr1.seek(2, os.SEEK_CUR)
        d1 = rr1.read()
        rr2.seek(0)
        rr2.seek(2, os.SEEK_CUR)
        d2 = rr2.read()
        assert d1 == d2
        assert rr1.eof() and rr2.eof()

        # no end specified
        sio.seek(0)
        rr = RangeReader(sio, 20, rebase=True)
        assert len(rr.read()) == size-20

        # fixed quantity read
        rr.seek(0)
        assert len(rr.read(5)) == 5

        # seek past beginning
        sio.seek(0)
        rr = RangeReader(sio, 20, 50, rebase=False)
        try:
            rr.seek(0)
            assert False # should never happen
        except ValueError:
            pass
