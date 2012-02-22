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
import cPickle
import binascii

from operator import itemgetter
from types import NotImplementedType
from cStringIO import StringIO

import vtil

from vtil import randomtools
from vtil.counter import Counter
from vtil.sorting import is_sorted, sortingPipe, extsorted
from vtil.indexed import IndexedKVWriter, IndexedKVReader
from vtil.rangereader import RangeReader
from vtil.records import RecordWriter, RecordReader, SENTINEL
from vtil.transaction import TransactionReader, TransactionWriter
from vtil.randomtools import random_string
from vtil.partition import Partitioner, StringPartitioner, HashPartitioner, NumberPartitioner
from vtil.iterator import wrap_around, pairwise

class UtilTest(unittest.TestCase):
    def test_fixed_int(self):
        s = '0000000021415'
        self.assertEqual(s, vtil.fixed_int(int(s), len(s)))
        self.assertRaises(ValueError, vtil.fixed_int, int(s), 1)

class IteratorTest(unittest.TestCase):
    def test_wrap_around(self):
        l = xrange(7)
        self.assertEqual([0,1,2], list(wrap_around(l, 0, 3))) # start
        self.assertEqual([3,4,5], list(wrap_around(l, 3, 3))) # mid
        self.assertEqual([5,6,0], list(wrap_around(l, 5, 3))) # wrap

        self.assertEqual([6,0,1,2,3,4,5,6,0,1,2,3],
                         list(wrap_around(l, 6, 12))) # multi-wrap

        self.assertEqual(list(wrap_around(xrange(7), 4, 25)),
                         list(wrap_around(xrange(7), 18, 25))) # periodicity

    def test_pairwise(self):
        l = xrange(3)
        self.assertEqual([(0,1), (1,2)], [x for x in pairwise(l)])

class CounterTest(unittest.TestCase):
    def test_Counter(self):
        l = [1,1,2,3,5]
        c = Counter(l)
        self.assertEqual([(1,2), (2,1), (3,1), (5,1)], list(c.most_common()))

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
        p = Partitioner(6)
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
        h = HashPartitioner(6)
        self.assertEqual(len(h), 6)
        self.isUniform(h, 0.1)

    def test_string_partitioner(self):
        h = StringPartitioner(6)
        self.assertEqual(len(h), 6)
        self.isUniform(h, 0.2)
    
    def test_number_partitioner(self):
        h = NumberPartitioner(0, 1, 6)
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

        # empty range
        sio.seek(0)
        rr = RangeReader(sio, 20, 20, rebase=True, hard_end=True)
        self.assertEqual(rr.read(), '')

        # backwards range
        sio.seek(0)
        self.assertRaises(ValueError, RangeReader, sio, 20, 15)

class TransactionTest(unittest.TestCase):
    def test_transaction_writer(self):
        sio = StringIO()
        writer = TransactionWriter(sio)
        with writer:
            writer.write('123')
            writer.write('456')
            self.assertEqual(sio.getvalue(), '')
            self.assertEqual(writer.mem_use(), 6)
        self.assertEqual(sio.getvalue(), '')
        self.assertEqual(writer.mem_use(), 0)
        with writer:
            writer.write('78')
            self.assertEqual(writer.mem_use(), 2)
            writer.commit()
            self.assertEqual(writer.mem_use(), 0)
        self.assertEqual(sio.getvalue(), '78')
        self.assertEqual(writer.mem_use(), 0)
        with writer:
            writer.write('abc')
            writer.commit()
            self.assertEqual(sio.getvalue(), '78abc')
            writer.write('xyz')
            self.assertEqual(sio.getvalue(), '78abc')
            writer.commit()
            self.assertEqual(sio.getvalue(), '78abcxyz')
            self.assertEqual(writer.mem_use(), 0)
            writer.write('blah')
        writer.write('outoftransaction')
        self.assertEqual(sio.getvalue(), '78abcxyzoutoftransaction')

    def test_transaction_reader(self):
        sio = StringIO()
        sio.write('1234567890abcdefg')
        sio.seek(0)
        reader = TransactionReader(sio)

        # no commit, unwind read
        with reader:
            self.assertEqual(reader.read(2), '12')
            self.assertEqual(reader.read(5), '34567')
        self.assertEqual(reader.mem_use(), 7)

        # commit
        with reader:
            self.assertEqual(reader.read(2), '12')
            self.assertEqual(reader.mem_use(), 7)
            self.assertEqual(reader.read(5), '34567')
            self.assertEqual(reader.read(2), '89')
            reader.commit()
        self.assertEqual(reader.mem_use(), 0)

        # partial commit
        with reader:
            self.assertEqual(reader.read(3), '0ab')
            self.assertEqual(reader.mem_use(), 3)
            reader.commit(2)
            self.assertEqual(reader.mem_use(), 1) # just 'b' left
            self.assertEqual(reader.read(2), 'cd')
        with reader:
            self.assertEqual(reader.read(2), 'bc') # '0a' was committed
        self.assertEqual(reader.mem_use(), 3)
        with reader:
            self.assertEqual(reader.read(2), 'bc')
            self.assertEqual(reader.mem_use(), 3)
        self.assertEqual(reader.mem_use(), 3)

        # read to end
        with reader:
            self.assertEqual(reader.read(), 'bcdefg')
        self.assertEqual(reader.mem_use(), 6)
        with reader:
            self.assertEqual(reader.read(), 'bcdefg')
            reader.commit()
        self.assertEqual(reader.mem_use(), 0)

        # eof
        with reader:
            self.assertEqual(reader.read(3), '') # EOF
            self.assertEqual(reader.read(), '')

        # newline and read-line
        sio = StringIO()
        sio.write('abc123\nanotherline\nlastlinewith no eof')
        sio.seek(0)
        reader = TransactionReader(sio)
        with reader:
            self.assertEqual(reader.readline(), 'abc123\n')
            reader.commit()
        with reader:
            self.assertEqual(reader.readline(), 'anotherline\n')
        with reader:
            self.assertEqual(reader.readline(), 'anotherline\n')
            self.assertEqual(reader.readline(), 'lastlinewith no eof')
            reader.commit()
        with reader:
            self.assertEqual(reader.readline(), '')

class RecordReaderTest(unittest.TestCase):
    def test_recordreader(self):
        stream = StringIO()
        write_data = [cPickle.dumps(random.random()) for _ in xrange(2)]
        write_data.append('abc12#jeoht38#SoSooihetS#') # contains sentinel
        write_data.extend(random_string(8) for _ in xrange(2))
        for i in write_data:
            with RecordWriter(stream) as r:
                r.write(i) # write each obj as its own record

        size = stream.tell()

        # reading from the beginning gets all values
        stream.seek(0)
        read_data = list(RecordReader(stream))
        self.assertEqual(len(write_data), len(read_data))
        self.assertEqual(write_data, read_data)

        # past the beginning gets fewer values
        last_count = len(read_data)
        for offset in xrange(1, size):
            stream.seek(offset, os.SEEK_SET)
            values = list(RecordReader(stream))
            self.assertTrue(len(values) == last_count or len(values) == last_count-1)
            last_count = min(last_count, len(values))

        # truncated at both ends, shrinking window
        stream.seek(0)
        last_count = len(read_data)
        for offset in xrange(1, size/2):
            ranger = RangeReader(stream, start=offset, end=size-offset, rebase=True, hard_end=True)
            values = list(RecordReader(ranger))
            self.assertTrue(len(values) <= last_count)
            last_count = min(last_count, len(values))

    def test_bad_record(self):
        sio = StringIO()
        pre_data = [1,1,2,3,5]
        post_data = [8,13,21]
        for i in pre_data:
            with RecordWriter(sio) as r:
                r.write(str(i))
        sio.write(SENTINEL + cPickle.dumps(20)) # fake record
        sio.write(random_string(20))
        sio.write(cPickle.dumps(binascii.crc32(''))) # wrong CRC
        for i in post_data:
            with RecordWriter(sio) as r:
                r.write(str(i))
        sio.seek(0)
        read_data = [int(s) for s in RecordReader(sio)]
        self.assertEqual(read_data, [1,1,2,3,5,8,13,21])
