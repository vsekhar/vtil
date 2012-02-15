'''
Created on Jan 14, 2012

@author: vsekhar
'''

import unittest
import random
import os
import tempfile
import sys
import random

from operator import itemgetter
from types import NotImplementedType
from cStringIO import StringIO

import vtil

from vtil import randomtools
from vtil.counter import Counter
from vtil.sorting import is_sorted
from vtil.extsorted import extsorted
from vtil.sortingpipe import sortingPipe
from vtil.indexed import IndexedKVWriter, IndexedKVReader
from vtil.rangereader import RangeReader
from vtil.records import RecordWriter, RecordReader
from vtil.randomtools import random_string

class UtilTest(unittest.TestCase):
    def test_fixed_int(self):
        s = '0000000021415'
        self.assertEqual(s, vtil.fixed_int(int(s), len(s)))

class SortingPipeTest(unittest.TestCase):
    def test_sortingpipe(self):
        iterations = 100

        forward = sortingPipe()
        reverse = sortingPipe(reverse=True)
        [forward.push(random.random()) for _ in xrange(iterations)]
        [reverse.push(random.random()) for _ in xrange(iterations)]
        self.assertTrue(is_sorted(forward))
        self.assertTrue(is_sorted(reverse, reverse=True))

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
        delta_count = expected_count * delta

        c = Counter((partitioner(rfunc()) for _ in xrange(samples)))
        for _, count in c.most_common():
            self.assertTrue(expected_count <= count+delta_count)
            self.assertTrue(expected_count >= count-delta_count)

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
        tf = tempfile.TemporaryFile()
        with IndexedKVWriter(tf, reverse=True) as writer:
            for _ in xrange(20):
                writer.write(random.random(), random.random())
        tf.seek(0)
        self.assertTrue(is_sorted(IndexedKVReader(tf), reverse=True, key=itemgetter(0)))
        tf.seek(0)
        self.assertEqual(20, len(iter(IndexedKVReader(tf))), 'length does not match')

class extsortedTest(unittest.TestCase):
    def test_extsorted_small(self):
        data = list(random.random() for _ in xrange(10))
        max_mem = sys.getsizeof(0) - 1 # too small for even one value
        self.assertTrue(is_sorted(extsorted(data, max_mem=max_mem)))

    def test_extsorted_all_at_once(self):
        data = list(random.random() for _ in xrange(10))
        max_mem = sys.getsizeof(data) * 2 # definitely big enough for whole list
        self.assertTrue(is_sorted(extsorted(data, max_mem=max_mem)))

    def test_extsorted_stress(self):
        file_size = 2 ** 20 # 1 MB
        file_count = 2.5
        typical_size = sys.getsizeof(1, random.random())
        count = int((file_size * file_count)) / typical_size
        data = ((i, random.random()) for i in xrange(count))
        sorted_data = list(extsorted(data, key=itemgetter(1), reverse=True, max_mem=file_size))
        self.assertEqual(len(sorted_data), count)
        self.assertTrue(is_sorted(sorted_data, key=itemgetter(1), reverse=True, expected_len=None))

class RangeReaderTest(unittest.TestCase):
    def test_rangereader(self):
        sio = StringIO()
        [sio.write(str(random.random())) for _ in xrange(1000)]
        size = sio.tell()

        # soft end, read all
        sio.seek(0)
        rr = RangeReader(sio, 20, 50)
        self.assertEqual(len(rr.read()), size-20)
        self.assertTrue(rr.eof())

        # soft end, read past end
        sio.seek(0)
        rr = RangeReader(sio, 20, 50)
        rr.seek(45)
        data = rr.read(25) # only 5 available
        self.assertEqual(len(data), 25)

        # hard end, read all
        sio.seek(0)
        rr = RangeReader(sio, 20, 50, hard_end=True)
        self.assertEqual(len(rr.read()), 50-20)
        self.assertTrue(rr.eof())

        # hard end, truncated range read
        sio.seek(0)
        rr = RangeReader(sio, 20, 50, hard_end=True)
        rr.seek(45)
        data = rr.read(25) # only 5 available
        self.assertEqual(len(data), 5)

        # rebase absolute
        sio.seek(0)
        rr1 = RangeReader(sio, 20, 50)
        rr2 = RangeReader(sio, 20, 50, rebase=True)
        rr1.seek(20)
        d1 = rr1.read()
        rr2.seek(0)
        d2 = rr2.read()
        self.assertEqual(d1, d2)
        self.assertTrue(rr1.eof() and rr2.eof())

        # rebase relative
        rr1.seek(20)
        rr1.seek(2, os.SEEK_CUR)
        d1 = rr1.read()
        rr2.seek(0)
        rr2.seek(2, os.SEEK_CUR)
        d2 = rr2.read()
        self.assertEqual(d1, d2)
        self.assertTrue(rr1.eof() and rr2.eof())

        # no end specified
        sio.seek(0)
        rr = RangeReader(sio, 20, rebase=True)
        self.assertEqual(len(rr.read()), size-20)

        # fixed quantity read
        rr.seek(0)
        self.assertEqual(len(rr.read(5)), 5)

        # absolute seek prior to beginning
        sio.seek(0)
        rr = RangeReader(sio, 20, 50, rebase=False)
        try:
            rr.seek(0)
            self.assertTrue(False) # should never happen
        except ValueError:
            pass

        # relative seek prior to beginning
        rr.seek(20)
        try:
            rr.seek(-1, os.SEEK_CUR)
            self.assertTrue(False) # should never happen
        except ValueError:
            pass

class RecordReaderTest(unittest.TestCase):
    def test_recordreader(self):
        stream = StringIO()
        data = [str(random.random()) for _ in xrange(2)]
        data.append('abc12#jeoht38#SoSooihetS#') # contains sentinel
        data.extend(random_string(8) for _ in xrange(2))
        value_count = len(data)
        for i in data:
            with RecordWriter(stream) as r:
                r.write(i)

        size = stream.tell()
        stream.seek(0)
        read_data = list(RecordReader(stream))

        print data
        stream.seek(0)
        print stream.read()
        print read_data
        self.assertEqual(len(data), len(read_data))
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
