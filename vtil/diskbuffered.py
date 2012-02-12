'''
Created on Jan 11, 2012

@author: vsekhar
'''

import tempfile
import threading
import os
import socket

class DiskBufferedOutput(object):
    ''' Allows writing to a local disk file, then submits local file to a write
    function for upload/archive/etc. on close. '''

    def __init__(self, writefunc):
        if not callable(writefunc):
            raise ValueError("writefunc must be a callable taking one argument")
        self._writefunc = writefunc
        self._file = tempfile.TemporaryFile()
        
        # forward
        forwarded = ['read', 'readline', 'readlines', 'write', 'seek', 'tell']
        for f in forwarded: setattr(self, f, getattr(self._file, f))
    
    def __enter__(self): return self
    def __exit__(self, ex, et, tb):
        self.close()
        return False
    
    def close(self):
        self._file.flush()
        self._file.seek(0)
        self._writefunc(self._file)
        self._file.close()
    
class DiskBufferedInput(object):
    """ Reads file buffered to local disk """
    default_blocksize = 2**15 ## 1 meg

    def __init__(self, fileobj, blocksize=default_blocksize, wait=False):
        self._buffer_lock = threading.RLock()
        self._fileobj = fileobj
        self._buffer = tempfile.TemporaryFile()
        self._buffer_condition = threading.Condition(self._buffer_lock)
        self._load_thread = threading.Thread(target=self._load)
        self._load_stop = False        
        self._block_size = blocksize
        self._load_thread.start()
        if wait: # for debugging stuff that may not be threadsafe
            self.wait()

    def __enter__(self):
        return self
    
    def __exit__(self, et, ev, tb):
        self.close()
        return False
        
    def _end_pos(self):
        cur = self._buffer.tell()
        self._buffer.seek(0, os.SEEK_END)
        end = self._buffer.tell()
        self._buffer.seek(cur)
        return end
        
    def _append(self, block):
        pos = self._buffer.tell()
        self._buffer.seek(0, os.SEEK_END)
        self._buffer.write(block)
        self._buffer.seek(pos)
        return len(block)
    
    def _unread(self, count):
        pos = self._buffer.tell()
        if count > pos:
            raise ValueError("Cannot unread past beginning of buffer")
        self._buffer.seek(-count, os.SEEK_CUR)

    def _load(self):
        while not self._load_stop:
            with self._buffer_lock:
                try:
                    data = self._fileobj.read(self._block_size)
                except socket.error, (value, _):
                    if value == 11:
                        continue
                    else:
                        raise
                written = self._append(data)
                self._buffer_condition.notify()
                if not written:
                    break
            
    def read(self, n=-1):
        " Reads from local copy if possible, otherwise blocks until remote data arrives "
        with self._buffer_lock:
            while self._load_thread.is_alive() and (n == -1 or self.local_size() < n):
                self._buffer_condition.wait()            
            return self._buffer.read(n)
    
    def readline(self):
        " Read until newline is seen, blocking as needed (newline is swallowed) "
        read_count = 80
        ret = ''
        while True:
            data = self.read(read_count)
            if not data:
                return ret # EOF
            keep, sep, putback = data.partition('\n')
            ret += keep
            if putback:
                with self._buffer_lock:
                    self._unread(len(putback))
            if sep:
                return ret # newline found
            
    
    def local_size(self):
        ' Return amount of data from current location readable locally '
        with self._buffer_lock:
            cur = self._buffer.tell()
            self._buffer.seek(0, os.SEEK_END)
            end = self._buffer.tell()
            self._buffer.seek(cur)
            return end - cur
    
    def is_fully_loaded(self):
        ' Return boolean indicating if entire file is locally available '
        return not self._load_thread.is_alive()
    
    def seek(self, offset, whence=os.SEEK_SET):
        ' Seeks to desired location (blocking until buffer fills up to there) '
        with self._buffer_lock:
            
            # possibly wait if thread is running and data not yet available
            if self.is_fully_loaded:
                pass
            elif whence == os.SEEK_SET and offset > self._buffer.tell():
                while self.local_size() < offset - self._buffer.tell():
                    self._buffer_condition.wait()
            elif whence == os.SEEK_CUR and offset > 0:
                while self.local_size() < offset:
                    self.buffer_condition.wait()
            elif whence == os.SEEK_END and offset <= 0:
                while not self.is_fully_loaded():
                    self._buffer_condition.wait()

            # data now available, do the seek
            return self._buffer.seek(offset, whence)
    
    def tell(self):
        return self._buffer.tell()

    def wait(self):
        ' Block until remote file is fully loaded locally '
        self._load_thread.join()
            
    def close(self):
        " Close file and stop further loading (if any) "
        self._load_stop = True
        self._load_thread.join()
        self._fileobj.close()
