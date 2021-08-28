import io
from math import floor, ceil
from multiprocessing import cpu_count
from typing import Generator, IO, Iterable, List, Tuple

from bgzip import bgzip_utils as bgu  # type: ignore


# samtools format specs:
# https://samtools.github.io/hts-specs/SAMv1.pdf
bgzip_eof = bytes.fromhex("1f8b08040000000000ff0600424302001b0003000000000000000000")

DEFAULT_DECOMPRESS_BUFFER_SZ = 1024 * 1024 * 50

class BGZipReader(io.RawIOBase):
    """
    Inflate data into a pre-allocated buffer. The buffer size will not change, and should be large enough
    to hold at least twice the data of any call to `read`.
    """
    def __init__(self,
                 fileobj: IO,
                 buffer_size: int=DEFAULT_DECOMPRESS_BUFFER_SZ,
                 num_threads=cpu_count(),
                 raw_read_chunk_size=256 * 1024):
        self.fileobj = fileobj
        self._input_data = bytes()
        self._inflate_buf = memoryview(bytearray(buffer_size))
        self._start = self._stop = 0
        self.raw_read_chunk_size = raw_read_chunk_size
        self.num_threads = num_threads

    def readable(self) -> bool:
        return True

    def _fetch_and_inflate(self):
        while True:
            self._input_data += self.fileobj.read(self.raw_read_chunk_size)
            bytes_read, bytes_inflated = bgu.inflate_into(self._input_data,
                                                          self._inflate_buf[self._start:],
                                                          num_threads=self.num_threads)
            if self._input_data and not bytes_inflated:
                # Not enough space at end of buffer, reset indices
                assert self._start == self._stop, "Read error. Please contact bgzip maintainers."
                self._start = self._stop = 0
            else:
                self._input_data = self._input_data[bytes_read:]
                self._stop += bytes_inflated
                break

    def _read(self, requested_size: int) -> memoryview:
        if self._start == self._stop:
            self._fetch_and_inflate()
        size = min(requested_size, self._stop - self._start)
        out = self._inflate_buf[self._start:self._start + size]
        self._start += len(out)
        return out

    def read(self, size: int=-1) -> memoryview:
        """
        Return a view to mutable memory. View should be consumed before calling 'read' again.
        """
        if -1 == size:
            data = bytearray()
            while True:
                try:
                    d = self._read(1024 ** 3)
                    if not d:
                        break
                    data.extend(d)
                finally:
                    d.release()
            out = memoryview(data)
        else:
            out = self._read(size)
        return out

    def readinto(self, buff) -> int:
        sz = len(buff)
        bytes_read = 0
        while bytes_read < sz:
            mv = self.read(sz - bytes_read)
            if not mv:
                break
            buff[bytes_read:bytes_read + len(mv)] = mv
            bytes_read += len(mv)
        return bytes_read

    def __iter__(self) -> Generator[bytes, None, None]:
        if not hasattr(self, "_buffered"):
            self._buffered = io.BufferedReader(self)
        for line in self._buffered:
            yield line

    def close(self):
        super().close()
        if hasattr(self, "_buffered"):
            self._buffered.close()

class BGZipWriter(io.IOBase):
    def __init__(self, fileobj: IO, batch_size: int=2000, num_threads: int=cpu_count()):
        self.fileobj = fileobj
        self.batch_size = batch_size
        self._input_buffer = bytearray()
        self._deflate_buffers = gen_deflate_buffers(self.batch_size)
        self.num_threads = num_threads

    def writable(self):
        return True

    def _deflate_and_write(self, data):
        _, deflated_blocks = deflate_to_buffers(data, self._deflate_buffers, num_threads=self.num_threads)
        for block in deflated_blocks:
            self.fileobj.write(block)

    def _compress(self, process_all_chunks=False):
        number_of_chunks = len(self._input_buffer) / bgu.block_data_inflated_size
        number_of_chunks = ceil(number_of_chunks) if process_all_chunks else floor(number_of_chunks)

        while number_of_chunks:
            batch = min(number_of_chunks, self.batch_size)
            if batch < self.batch_size and not process_all_chunks:
                break

            n = batch * bgu.block_data_inflated_size
            self._deflate_and_write(memoryview(self._input_buffer)[:n])
            self._input_buffer = self._input_buffer[n:]

            number_of_chunks -= batch

    def write(self, data):
        self._input_buffer.extend(data)
        if len(self._input_buffer) > self.batch_size * bgu.block_data_inflated_size:
            self._compress()

    def close(self):
        if self._input_buffer:
            self._compress(process_all_chunks=True)
        self.fileobj.write(bgzip_eof)
        self.fileobj.flush()

def gen_deflate_buffers(number_of_buffers: int) -> List[bytearray]:
    # Include a kilobyte of padding for poorly compressible data
    max_deflated_block_size = bgu.block_data_inflated_size + bgu.block_metadata_size + 1024
    return [bytearray(max_deflated_block_size) for _ in range(number_of_buffers)]

def deflate_to_buffers(data: memoryview,
                       deflate_buffers: Iterable[bytearray],
                       num_threads: int=cpu_count()) -> Tuple[int, List[memoryview]]:
    deflated_sizes = bgu.deflate_to_buffers(data, deflate_buffers, num_threads)
    bytes_deflated = bgu.block_data_inflated_size * len(deflated_sizes)
    return bytes_deflated, [memoryview(buf)[:sz] for buf, sz in zip(deflate_buffers, deflated_sizes)]
