import io
import typing
import multiprocessing
from math import floor, ceil
from concurrent.futures import ThreadPoolExecutor, as_completed

from . import bgzip_utils  # type: ignore


available_cores = multiprocessing.cpu_count()


class BGZipReader(io.IOBase):
    def __init__(self, fileobj: typing.IO, num_threads: int=available_cores, raw_read_chunk_size: int=256 * 1024):
        self.fileobj = fileobj
        self._input_data = bytes()
        self._buffer = bytearray()
        self._pos = 0
        self.num_threads = num_threads
        self.raw_read_chunk_size = raw_read_chunk_size

    def readable(self):
        return True

    def read(self, size: int=-1):
        while -1 == size or len(self._buffer) - self._pos < size:
            if self._pos:
                del self._buffer[:self._pos]
                self._pos = 0
            self._input_data += self.fileobj.read(self.raw_read_chunk_size)
            if not self._input_data:
                break
            bytes_read = bgzip_utils.decompress_into(self._input_data,
                                                     self._buffer,
                                                     num_threads=self.num_threads)
            self._input_data = self._input_data[bytes_read:]
        ret_val = self._buffer[self._pos:self._pos + size]
        self._pos += size
        return ret_val

    def readinto(self, buff):
        d = self.read(len(buff))
        buff[:len(d)] = d
        return len(d)


class Window:
    def __init__(self, start: int=0, end: int=0):
        self.start = start
        self.end = end


class BGZipReaderPreAllocated(BGZipReader):
    """
    Inflate data into a pre-allocated buffer. The buffer size will not change, and should be large enough
    to hold at least twice the data of any call to `read`.
    """
    def __init__(self, fileobj, buf: bytearray, num_threads=available_cores, raw_read_chunk_size=256 * 1024):
        self.fileobj = fileobj
        self._input_data = bytes()
        self._scratch = memoryview(buf)
        self._bytes_available = 0
        self.raw_read_chunk_size = raw_read_chunk_size

        self.num_threads = num_threads

        self._windows = []  # type: ignore

    def readable(self):
        return True

    def _fetch_and_inflate(self, size: int):
        # TODO: prevent window overlap.
        #       does `self._bytes_available < len(self._scratch) / 2` do the trick?
        while self._bytes_available < size and self._bytes_available < len(self._scratch) / 2:
            if not self._windows:
                self._windows.append(Window())

            self._input_data += self.fileobj.read(self.raw_read_chunk_size)
            if not self._input_data:
                break
            bytes_read, bytes_inflated = bgzip_utils.decompress_into_2(self._input_data,
                                                                       self._scratch,
                                                                       self._windows[-1].end,
                                                                       num_threads=self.num_threads)
            if self._input_data and not bytes_inflated:
                self._windows.append(Window())
            else:
                self._bytes_available += bytes_inflated
                self._input_data = self._input_data[bytes_read:]
                self._windows[-1].end += bytes_inflated
                if self._windows[-1].end > len(self._scratch):
                    raise Exception("not good")

    def _read(self, size: int):
        start, end = self._windows[0].start, self._windows[0].end
        size = min(size, end - start)
        ret_val = self._scratch[start:start + size]
        self._windows[0].start += len(ret_val)
        self._bytes_available -= len(ret_val)
        if self._windows[0].start >= self._windows[0].end:
            self._windows.pop(0)
        return ret_val

    def read(self, size: int):  # type: ignore
        """
        Return a view to mutable memory. View should be consumed before calling `read` again.
        """
        assert size > 0
        self._fetch_and_inflate(size)
        return self._read(size)

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


class BGZipAsyncReaderPreAllocated(BGZipReaderPreAllocated):
    def __init__(self,
                 fileobj,
                 buf: bytearray,
                 num_threads: int=available_cores,
                 raw_read_chunk_size: int=256 * 1024,
                 read_buffer_size: int=1024 * 1024):
        super().__init__(fileobj, buf, num_threads, raw_read_chunk_size)
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._futures = list()  # type: ignore
        self._read_buffer_size = 1 * 1024 * 1024

    @property
    def future_size(self):
        return self._bytes_available + self._read_buffer_size * len(self._futures)

    def _fetch_and_inflate(self, size):
        f = self._executor.submit(super()._fetch_and_inflate, self._read_buffer_size)
        f.add_done_callback(self._finalize_future)
        self._futures.append(f)

    def _finalize_future(self, f):
        self._futures.remove(f)

    def _wait_for_futures(self):
        for _ in as_completed(self._futures[0:]):
            pass

    def read(self, size):
        if size <= self._bytes_available:
            pass
        elif size <= self.future_size:
            while size <= self.future_size:
                self._wait_for_futures()
        else:
            desired = size + self._read_buffer_size
            gap = size - self._bytes_available
            self._fetch_and_inflate(gap)
            self._fetch_and_inflate(desired - gap)
            self._wait_for_futures()
        return self._read(size)


class BGZipWriter(io.IOBase):
    def __init__(self, fileobj: typing.IO, batch_size: int=2000, num_threads: int=available_cores):
        self.fileobj = fileobj
        self.batch_size = batch_size
        self._input_buffer = bytearray()
        block_size = bgzip_utils.block_data_inflated_size + bgzip_utils.block_metadata_size
        self._scratch_buffers = [bytearray(block_size) for _ in range(self.batch_size)]
        self.num_threads = num_threads

    def writable(self):
        return True

    def _deflate_and_write(self, data):
        bgzip_utils.compress_to_stream(data,
                                       self._scratch_buffers,
                                       self.fileobj,
                                       num_threads=self.num_threads)

    def _compress(self, process_all_chunks=False):
        number_of_chunks = len(self._input_buffer) / bgzip_utils.block_data_inflated_size
        number_of_chunks = ceil(number_of_chunks) if process_all_chunks else floor(number_of_chunks)

        while number_of_chunks:
            batch = min(number_of_chunks, self.batch_size)
            if batch < self.batch_size and not process_all_chunks:
                break

            n = batch * bgzip_utils.block_data_inflated_size
            self._deflate_and_write(memoryview(self._input_buffer)[:n])
            self._input_buffer = self._input_buffer[n:]

            number_of_chunks -= batch

    def write(self, data):
        self._input_buffer.extend(data)
        if len(self._input_buffer) > self.batch_size * bgzip_utils.block_data_inflated_size:
            self._compress()

    def close(self):
        if self._input_buffer:
            self._compress(process_all_chunks=True)
        self.fileobj.flush()


def async_writer_wait_func(number_of_futures):
    if 20 < number_of_futures:
        raise Exception("bgzip writer can't keep up. Goodbye")


class AsyncBGZipWriter(BGZipWriter):
    def __init__(self, fileobj, *args, **kwargs):
        super().__init__(fileobj, *args, **kwargs)
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
