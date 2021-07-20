import io
import multiprocessing
from math import floor, ceil
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import IO, Optional

from bgzip import bgzip_utils  # type: ignore
from bgzip.bgzip_utils import BGZIPException, BGZIPMalformedHeaderException


available_cores = multiprocessing.cpu_count()

# samtools format specs:
# https://samtools.github.io/hts-specs/SAMv1.pdf
bgzip_eof = bytes.fromhex("1f8b08040000000000ff0600424302001b0003000000000000000000")

DEFAULT_DECOMPRESS_BUFFER_SZ = 1024 * 1024 * 50

class Window:
    def __init__(self, start: int=0, end: int=0):
        self.start = start
        self.end = end

class BGZipReader(io.IOBase):
    """
    Inflate data into a pre-allocated buffer. The buffer size will not change, and should be large enough
    to hold at least twice the data of any call to `read`.
    """
    def __init__(self,
                 fileobj: IO,
                 buffer_size: int=DEFAULT_DECOMPRESS_BUFFER_SZ,
                 num_threads=available_cores,
                 raw_read_chunk_size=256 * 1024):
        self.fileobj = fileobj
        self._input_data = bytes()
        self._scratch = memoryview(bytearray(buffer_size))
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
            bytes_read, bytes_inflated = bgzip_utils.inflate_into(self._input_data,
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

class BGZipWriter(io.IOBase):
    def __init__(self, fileobj: IO, batch_size: int=2000, num_threads: int=available_cores):
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
        self.fileobj.write(bgzip_eof)
        self.fileobj.flush()
