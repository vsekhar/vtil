'''
Read and write very large files.

Reader: Takes a file_obj intelligently seek()'s and read()'s it from another
thread to a buffer.

Writer: Writes/seeks to a local file
'''

import threading
import tempfile
import os
import Queue

import vtil

DEFAULT_CHUNK_SIZE = 2**20 * 64 # 64 MB per request

class Reader(object):
    def __init__(self, file_obj, file_size, chunksize=DEFAULT_CHUNK_SIZE, start=True):
        self._file_obj = file_obj
        self._file_size = file_size
        self._lock = threading.RLock()
        self._load_condition = threading.Condition(self._lock)
        self._stopped = False
        self._loc = 0
        self._chunk_size = chunksize
        chunk_count = self._file_size // self._chunk_size
        chunk_count += 1 if self._file_size % self._chunk_size else 0
        self._chunks = [None for _ in xrange(chunk_count)]
        self._load_thread = threading.Thread(target=self._load)
        if start:
            self._load_thread.start()

    def _chunk_loc(self):
        ' Returns (chunk_num, chunk_offset) for a given location in the larger file '
        adj_loc = min(self._loc, self._file_size)
        return adj_loc // self._chunk_size, adj_loc % self._chunk_size

    def _load_chunk(self, chunk_num):
        tf = tempfile.TemporaryFile()
        start_idx = chunk_num * self._chunk_size
        self._file_obj.seek(start_idx)
        tf.write(self._file_obj.read(self._chunk_size))
        with self._lock:
            self._chunks[chunk_num] = (tf, tf.tell()) # (tempfile, size)
            self._load_condition.notify()

    def _load(self):
        try:
            while not self._stopped:
                # check current chunk, load if needed
                if self._loc < self._file_size:
                    chunk_num, _ = self._chunk_loc()
                    chunk_and_size = self._chunks[chunk_num]
                    if chunk_and_size is None:
                        self._load_chunk(chunk_num)
    
                # load some other one
                chunk_num += 1
                try:
                    idx = (i for i,cs in vtil.wrap(enumerate(self._chunks), chunk_num, len(self._chunks))
                             if cs is None).next()
                except StopIteration:
                    # none left
                    return
                else:
                    self._load_chunk(idx)
        except KeyboardInterrupt:
            pass

    def seek(self, loc, rel=os.SEEK_SET):
        with self._lock:
            if rel == os.SEEK_CUR:
                self._loc += loc
            elif rel == os.SEEK_SET:
                self._loc = loc
            elif rel == os.SEEK_END:
                self._loc = self._file_size + loc

    def read(self, bytes_to_read=-1):
        ret = []
        with self._lock:
            while bytes_to_read > 0:
                if self._loc >= self._file_size: break # can't read past end of file
                chunk_num, chunk_offset = self._chunk_loc() # which chunk, and where in it to read?
                while not self._chunks[chunk_num]:
                    self._load_condition.wait() # wait for chunk if needed
                chunk, size = self._chunks[chunk_num]
                chunk.seek(chunk_offset, os.SEEK_SET)
                data = chunk.read(bytes_to_read)
                ret.append(data)
                bytes_to_read -= len(data)
                self._loc += len(data)
        return ''.join(ret)

    def start(self):
        self._load_thread.start()

    def join(self):
        self._load_thread.join()

    def stop(self):
        self._stopped = True

    def close(self):
        self.stop()
        self._file_obj.close()

class Writer(object):
    ' A file-like object in a temporary location that writes segments of itself in another thread as it is being written '
    def __init__(self, file_obj, chunksize=DEFAULT_CHUNK_SIZE, threads=3, start=True):
        self._file_obj = file_obj
        self._chunk_size = chunksize
        self._chunk_queue = Queue.Queue()
        self._cur_file = tempfile.TemporaryFile()
        self._save_thread = threading.Thread(target=self._save)
        self._save_event = threading.Event()
        if start:
            self._save_thread.start()

    def _save(self):
        while True:
            tf = self._chunk_queue.get(block=True)
            if not tf: break
            tf.seek(0, os.SEEK_SET)
            self._file_obj.write(tf.read())

    def _stash_chunk(self):
        self._chunk_queue.put(self._cur_file)
        self._cur_file = tempfile.TemporaryFile()
        self._save_event.set()

    def write(self, data):
        space = self._chunk_size - self._cur_file.tell()
        self._cur_file.write(data[:space])
        if self._cur_file.tell() == self._chunk_size:
            self._stash_chunk()
        elif self._cur_file.tell() > self._chunk_size:
            raise RuntimeError("bigfile.Writer problem")
        if len(data) >= space:
            self.write(data[space:])

    def start(self):
        self._save_thread.start()

    def join(self):
        self._save_thread.join()

    def close(self):
        if self._cur_file.tell():
            self._stash_chunk()
        self.stop()
        self.join()
        self._file_obj.close()

    def stop(self):
        self._chunk_queue.put(None)
        self._save_event.set()

if __name__ == '__main__':
    import time
    from contextlib import closing

    tf = tempfile.NamedTemporaryFile(delete=False)
    chunksize = (2**20) # 1MB

    # test writer
    with closing(Writer(tf, chunksize)) as writer:
        [writer.write('abc123\n') for _ in xrange(((2**20))+42)]
        size = tf.tell()
        print "size: %d" % size

    # test reader
    tf = open(tf.name, 'rb')
    with closing(Reader(tf, size, chunksize)) as reader:
        print 'first', reader.read(7)
        reader.seek(chunksize*23)
        print 'second', reader.read(7)

        time.sleep(2)
        print 'done sleeping'
        reader.seek(chunksize * 10)
        print 'third', reader.read(7)
        reader.seek(chunksize * 40)
        print 'fourth', reader.read(7)
