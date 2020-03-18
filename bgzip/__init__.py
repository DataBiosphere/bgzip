import io
import os
import gzip
import time
import struct
import threading
from math import floor, ceil
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import namedtuple

from . import bgzip_utils  # type: ignore


number_of_reader_threads = 4
number_of_writer_threads = 8


_profile_info = dict()
def profile(func):
    _profile_info[func.__name__] = dict(number_of_calls=0, total_duration=0)

    def wrapped(*args, **kwargs):
        start = time.time()
        res = func(*args, **kwargs)
        _profile_info[func.__name__]['total_duration'] += time.time() - start
        _profile_info[func.__name__]['number_of_calls'] += 1
        return res

    return wrapped


class BGZipReader(io.IOBase):
    chunk_size = 256 * 1024

    def __init__(self, fileobj):
        self.fileobj = fileobj
        self._input_data = bytes()
        self._buffer = bytearray()
        self._pos = 0

    def readable(self):
        return True

    def read(self, size=-1):
        while -1 == size or len(self._buffer) - self._pos < size:
            if self._pos:
                del self._buffer[:self._pos]
                self._pos = 0
            self._input_data += self.fileobj.read(self.chunk_size)
            if not self._input_data:
                break
            bytes_read = bgzip_utils.decompress_into(self._input_data,
                                                     self._buffer,
                                                     num_threads=number_of_reader_threads)
            self._input_data = self._input_data[bytes_read:]
        ret_val = self._buffer[self._pos:self._pos + size]
        self._pos += size
        return ret_val

    def readinto(self, buff):
        d = self.read(len(buff))
        buff[:len(d)] = d
        return len(d)


class Window:
    def __init__(self, start=0, end=0):
        self.start = start
        self.end = end


class BGZipReaderCircularBuff(io.IOBase):
    chunk_size = 1024 * 1024

    def __init__(self, fileobj):
        self.fileobj = fileobj
        self._input_data = bytes()
        self._buffer = memoryview(bytearray(1024 * 1024 * 1024 * 5))
        self._bytes_available = 0

        self._windows = []

    def readable(self):
        return True

    def read(self, size):
        """
        Return a view to mutable memory. View should be consumed before calling `read` again.
        """
        assert size > 0
        while self._bytes_available < size and self._bytes_available < len(self._buffer) / 2:
            if not self._windows:
                self._windows.append(Window())

            self._input_data += self.fileobj.read(self.chunk_size)
            if not self._input_data:
                break
            bytes_read, bytes_inflated = bgzip_utils.decompress_into_2(self._input_data,
                                                                       self._buffer,
                                                                       self._windows[-1].end,
                                                                       num_threads=number_of_reader_threads)
            if self._input_data and not bytes_inflated:
                self._windows.append(Window())
            else:
                self._bytes_available += bytes_inflated
                self._input_data = self._input_data[bytes_read:]
                self._windows[-1].end += bytes_inflated
                if self._windows[-1].end > len(self._buffer):
                    raise Exception("not good")
        start, end = self._windows[0].start, self._windows[0].end
        size = min(size, end - start)
        ret_val = self._buffer[start:start + size]
        self._windows[0].start += len(ret_val)
        self._bytes_available -= len(ret_val)
        if self._windows[0].start >= self._windows[0].end:
            self._windows.pop(0)
        return ret_val

    def readinto(self, buff):
        sz = len(buff)
        bytes_read = 0
        while bytes_read < sz:
            mv = self.read(sz - bytes_read)
            if not mv:
                break
            buff[bytes_read:bytes_read + len(mv)] = mv
            bytes_read += len(mv)
        return bytes_read


class BGZipWriter(io.IOBase):
    chunk_size = bgzip_utils.block_inflated_size
    batch_size = 20000
    block_metadata_size = bgzip_utils.block_metadata_size

    def __init__(self, fileobj):
        self.fileobj = fileobj
        self._input_buffer = bytearray()
        block_size = self.chunk_size + self.block_metadata_size
        self._scratch_buffers = [bytearray(block_size) for _ in range(self.batch_size)]

    def writable(self):
        return True

    def _deflate_and_write(self, data):
        bgzip_utils.compress_to_stream(data,
                                       self._scratch_buffers,
                                       self.fileobj,
                                       num_threads=number_of_writer_threads)

    def _compress(self, process_all_chunks=False):
        number_of_chunks = len(self._input_buffer) / self.chunk_size
        number_of_chunks = ceil(number_of_chunks) if process_all_chunks else floor(number_of_chunks)

        while number_of_chunks:
            batch = min(number_of_chunks, self.batch_size)
            if batch < self.batch_size and not process_all_chunks:
                break

            n = batch * self.chunk_size
            self._deflate_and_write(memoryview(self._input_buffer)[:n])
            self._input_buffer = self._input_buffer[n:]

            number_of_chunks -= batch

    def write(self, data):
        self._input_buffer.extend(data)
        if len(self._input_buffer) > self.batch_size * self.chunk_size:
            self._compress()

    def close(self):
        if self._input_buffer:
            self._compress(process_all_chunks=True)
        self.fileobj.flush()


def async_writer_wait_func(number_of_futures):
    if 20 < number_of_futures:
        raise Exception("bgzip writer can't keep up. Goodbye")


class AsyncBGZipWriter(BGZipWriter):
    def __init__(self, fileobj):
        super().__init__(fileobj)
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._futures = set()

    def _deflate_and_write(self, data):
        f = self._executor.submit(super()._deflate_and_write, data)
        f.add_done_callback(self._result)
        self._futures.add(f)

    def _result(self, future):
        future.result()
        self._futures.remove(future)
        async_writer_wait_func(len(self._futures))

    def close(self):
        if self._input_buffer:
            self._compress(process_all_chunks=True)
        self._executor.shutdown()
        try:
            self.fileobj.flush()
        except BlockingIOError:
            pass
