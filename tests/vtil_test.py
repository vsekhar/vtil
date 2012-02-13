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
from vtil.iterator import pairwise

class UtilTest(unittest.TestCase):
    def test_fixed_int(self):
        s = '0000000021415'
        self.assertEqual(s, vtil.fixed_int(int(s), len(s)))

class SortingPipeTest(unittest.TestCase):
    def test_sortingpipe(self):
        from vtil.sortingpipe import sortingPipe
        iterations = 100

        forward = sortingPipe()
        reverse = sortingPipe(reverse=True)
        [forward.push(random.random()) for _ in xrange(iterations)]
        [reverse.push(random.random()) for _ in xrange(iterations)]
        s = set(a<=b for a,b in pairwise(forward))
        s = set(a>=b for a,b in pairwise(reverse))
        self.assertNotIn(False, forward)
        self.assertNotIn(False, reverse)

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
        from operator import itemgetter
        from vtil.extsorted import extsorted
        file_size = 2 ** 20 # 1 MB
        file_goal = 2.5
        prototype = (1, random.random())
        count = int((file_size * file_goal)) / sys.getsizeof(prototype)
        data = ((i, random.random()) for i in xrange(count))
        sorted_data = list(extsorted(data, key=itemgetter(1), reverse=True, max_mem=file_size))
        self.assertEqual(len(sorted_data), count)
        s = set(a[1]>=b[1] for a,b in pairwise(sorted_data))
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

class RecordReaderTest(unittest.TestCase):
    def test_recordreader(self):
        import random
        from StringIO import StringIO
        from vtil.records import RecordWriter, RecordReader

        stream = StringIO()
        data = [str(random.random()) for _ in xrange(3)]
        data.append('abc12#jeoht38#SoSooihetS#') # contains sentinel
        value_count = len(data)
        # TODO: add length/checksum
        count = len(data)
        for i in data:
            with RecordWriter(stream) as r:
                r.write(i)

        size = stream.tell()
        stream.seek(0, os.SEEK_SET)
        read_data = [s for s in RecordReader(stream)]
        self.assertEqual(len(read_data), count)
        self.assertEqual(data, read_data)

        # reading from the beginning gets all values
        stream.seek(0) 
        values = list(RecordReader(stream))
        self.assertEqual(value_count, len(values))

        # past the beginning gets fewer values
        last_count = len(values)
        for offset in xrange(1, size):
            stream.seek(offset, os.SEEK_SET)
            values = list(RecordReader(stream))
            self.assertTrue(len(values) == last_count or len(values) == last_count-1)
            last_count = min(last_count, len(values))
