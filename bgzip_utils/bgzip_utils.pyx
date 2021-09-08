import io
import zlib
import struct
from math import ceil

from libc.stdlib cimport abort
from cython.parallel import prange

from czlib cimport *
from cpython_nogil cimport *


cdef enum:
    BLOCK_BATCH_SIZE = 300
    MAGIC_LENGTH = 4

block_batch_size = int(BLOCK_BATCH_SIZE)  # make BLOCK_BATCH_SIZE accessible in Python

cdef enum bgzip_err:
    BGZIP_CRC_MISMATCH = -8
    BGZIP_ZLIB_INITIALIZATION_ERROR
    BGZIP_BLOCK_SIZE_MISMATCH
    BGZIP_BLOCK_SIZE_NEGATIVE
    BGZIP_ZLIB_ERROR
    BGZIP_MALFORMED_HEADER
    BGZIP_INSUFFICIENT_BYTES
    BGZIP_ERROR
    BGZIP_OK

cdef const unsigned char * MAGIC = "\037\213\010\4"
# cdef Bytef * HEADER = b"\037\213\010\4\0\0\0\0\0\377\6\0\102\103\2\0"
cdef bytes HEADER = b"\037\213\010\4\0\0\0\0\0\377\6\0\102\103\2\0"

ctypedef block_header_s BlockHeader
cdef struct block_header_s:
    unsigned char magic[MAGIC_LENGTH]
    unsigned int mod_time
    unsigned char extra_flags
    unsigned char os_type
    unsigned short extra_len

ctypedef block_header_subfield_s BlockHeaderSubfield
cdef struct block_header_subfield_s:
    unsigned char id_[2]
    unsigned short length

ctypedef block_header_bgzip_subfield_s BlockHeaderBGZipSubfield
cdef struct block_header_bgzip_subfield_s:
    unsigned char id_[2]
    unsigned short length
    unsigned short block_size

ctypedef block_tailer_s BlockTailer
cdef struct block_tailer_s:
    unsigned int crc
    unsigned int inflated_size

ctypedef block_s Block
cdef struct block_s:
    unsigned int deflated_size
    unsigned int inflated_size
    unsigned int crc
    unsigned short block_size
    Bytef * next_in
    unsigned int available_in
    Bytef * next_out
    unsigned int avail_out

ctypedef bgzip_stream_s BGZipStream
cdef struct bgzip_stream_s:
    unsigned int available_in
    Bytef *next_in

class BGZIPException(Exception):
    pass

class BGZIPMalformedHeaderException(BGZIPException):
    pass

cdef bgzip_err inflate_block(Block * block) nogil:
    cdef z_stream zst
    cdef int err

    zst.zalloc = NULL
    zst.zfree = NULL
    zst.opaque = NULL
    zst.avail_in = block.deflated_size
    zst.avail_out = 1024 * 1024
    zst.next_in = block.next_in
    zst.next_out = block.next_out

    err = inflateInit2(&zst, -15)
    if Z_OK != err:
        return BGZIP_ZLIB_INITIALIZATION_ERROR
    err = inflate(&zst, Z_FINISH)
    if Z_STREAM_END == err:
        pass
    else:
        return BGZIP_ZLIB_ERROR
    inflateEnd(&zst)

    if block[0].inflated_size != zst.total_out:
        return BGZIP_BLOCK_SIZE_MISMATCH

    if block.crc != crc32(0, block.next_out, block.inflated_size):
        return BGZIP_CRC_MISMATCH

    return BGZIP_OK

    # Difference betwwen `compress` and `deflate`:
    # https://stackoverflow.com/questions/10166122/zlib-differences-between-the-deflate-and-compress-functions

cdef void * ref_and_advance(BGZipStream * rb, unsigned int member_size, bgzip_err *err) nogil:
    if rb.available_in  < member_size:
        err[0] = BGZIP_INSUFFICIENT_BYTES
        return NULL
    else:
        ret_val = rb.next_in
        rb.next_in += member_size
        rb.available_in  -= member_size
        err[0] = BGZIP_OK
        return ret_val

cdef bgzip_err read_block(Block * block, BGZipStream *src) nogil:
    cdef unsigned int i
    cdef bgzip_err err
    cdef BlockHeader * head
    cdef BlockTailer * tail
    cdef BlockHeaderSubfield * subfield
    cdef Bytef * subfield_data
    cdef unsigned int extra_len

    block.block_size = 0

    head = <BlockHeader *>ref_and_advance(src, sizeof(BlockHeader), &err)
    if err:
        return err

    for i in range(<unsigned int>MAGIC_LENGTH):
        if head.magic[i] != MAGIC[i]:
            return BGZIP_MALFORMED_HEADER

    extra_len = head.extra_len
    while extra_len > 0:
        subfield = <BlockHeaderSubfield *>ref_and_advance(src, sizeof(BlockHeaderSubfield), &err)
        if err:
            return err
        extra_len -= sizeof(BlockHeaderSubfield)

        subfield_data = <Bytef *>ref_and_advance(src, subfield.length, &err)
        if err:
            return err
        extra_len -= sizeof(subfield.length)

        if b"B" == subfield.id_[0] and b"C" == subfield.id_[1]:
            if subfield.length != 2:
                return BGZIP_MALFORMED_HEADER
            block.block_size = (<unsigned short *>subfield_data)[0]

    if 0 != extra_len:
        return BGZIP_BLOCK_SIZE_MISMATCH

    if 0 >= block.block_size:
        return BGZIP_BLOCK_SIZE_NEGATIVE

    block.next_in = src.next_in
    block.deflated_size = 1 + block.block_size - sizeof(BlockHeader) - head.extra_len - sizeof(BlockTailer)

    ref_and_advance(src, block.deflated_size, &err)
    if err:
        return err

    tail = <BlockTailer *>ref_and_advance(src, sizeof(BlockTailer), &err)
    if err:
        return err

    block.crc = tail.crc
    block.inflated_size = tail.inflated_size

cdef py_memoryview_to_buffer(object py_memoryview, Bytef ** buf):
    cdef PyObject * obj = <PyObject *>py_memoryview
    if PyMemoryView_Check(obj):
        # TODO: Check buffer is contiguous, has normal stride
        buf[0] = <Bytef *>(<Py_buffer *>PyMemoryView_GET_BUFFER(obj)).buf
        assert NULL != buf
    else:
        raise TypeError("'py_memoryview' must be a memoryview instance.")

def inflate_chunks(list py_src_mem_views, object dst_buff_obj, int num_threads):
    """
    Inflate bytes from `py_src_mem_views` into `dst_buff`
    """
    cdef int i, err
    cdef int bytes_read = 0, bytes_inflated = 0, number_of_source_chunks = 0, block_index = 0, chunk_index = 0
    cdef Bytef * out = NULL
    cdef Block blocks[BLOCK_BATCH_SIZE]
    cdef BGZipStream curr, src[BLOCK_BATCH_SIZE]

    for i, view in zip(range(BLOCK_BATCH_SIZE), py_src_mem_views):
        py_memoryview_to_buffer(view, &(src[i].next_in))
        src[i].available_in = len(view)
    number_of_source_chunks = i + 1

    py_memoryview_to_buffer(dst_buff_obj, &out)
    cdef unsigned int avail_out = PySequence_Size(<PyObject *>dst_buff_obj)

    with nogil:
        while chunk_index < number_of_source_chunks and block_index < BLOCK_BATCH_SIZE:
            if 0 == src[chunk_index].available_in:
                chunk_index += 1
                continue
            curr = src[chunk_index]
            err = read_block(&blocks[block_index], &src[chunk_index])
            if BGZIP_OK == err:
                pass
            elif BGZIP_INSUFFICIENT_BYTES == err:
                break
            elif BGZIP_MALFORMED_HEADER == err:
                raise BGZIPMalformedHeaderException("Block gzip magic not found in header.")
            else:
                raise BGZIPException("decompress 2 error")
            if avail_out < bytes_inflated + blocks[block_index].inflated_size:
                src[chunk_index] = curr
                break
            bytes_read += 1 + blocks[block_index].block_size
            bytes_inflated += blocks[block_index].inflated_size
            block_index += 1

        for i in range(block_index):
            blocks[i].next_out = out
            out += blocks[i].inflated_size

        for i in prange(block_index, num_threads=num_threads, schedule="dynamic"):
            inflate_block(&blocks[i])

    chunk_index = min(chunk_index, number_of_source_chunks - 1) 
    sz = src[chunk_index].available_in
    if sz:
        remaining_chunks = [py_src_mem_views[chunk_index][-sz:]]
    else:
        remaining_chunks = list()
    remaining_chunks.extend(py_src_mem_views[chunk_index + 1:])

    return {'bytes_read':       bytes_read,
            'bytes_inflated':   bytes_inflated,
            'remaining_chunks': remaining_chunks,
            'block_sizes':      [blocks[i].inflated_size for i in range(block_index)]}

cdef bgzip_err compress_block(Block * block) nogil:
    cdef z_stream zst
    cdef int err = 0
    cdef BlockHeader * head
    cdef BlockHeaderBGZipSubfield * head_subfield
    cdef BlockTailer * tail
    cdef int wbits = -15
    cdef int mem_level = 8

    head = <BlockHeader *>block.next_out
    block.next_out += sizeof(BlockHeader)

    head_subfield = <BlockHeaderBGZipSubfield *>block.next_out
    block.next_out += sizeof(BlockHeaderBGZipSubfield)

    zst.zalloc = NULL
    zst.zfree = NULL
    zst.opaque = NULL
    zst.next_in = block.next_in
    zst.avail_in = block.available_in
    zst.next_out = block.next_out
    zst.avail_out = 1024 * 1024
    err = deflateInit2(&zst, Z_BEST_COMPRESSION, Z_DEFLATED, wbits, mem_level, Z_DEFAULT_STRATEGY)
    if Z_OK != err:
        return BGZIP_ZLIB_ERROR
    err = deflate(&zst, Z_FINISH)
    if Z_STREAM_END != err:
        return BGZIP_ZLIB_ERROR
    deflateEnd(&zst)

    block.next_out += zst.total_out

    tail = <BlockTailer *>block.next_out

    for i in range(MAGIC_LENGTH):
        head.magic[i] = MAGIC[i]
    head.mod_time = 0
    head.extra_flags = 0
    head.os_type = b"\377"
    head.extra_len = sizeof(BlockHeaderBGZipSubfield)

    head_subfield.id_[0] = b"B"
    head_subfield.id_[1] = b"C"
    head_subfield.length = 2
    head_subfield.block_size = sizeof(BlockHeader) + sizeof(BlockHeaderBGZipSubfield) + zst.total_out + sizeof(BlockTailer) - 1

    tail.crc = crc32(0, block.next_in, block.inflated_size)
    tail.inflated_size = block.inflated_size

    block.block_size = 1 + head_subfield.block_size

    return BGZIP_OK

cdef unsigned int _block_data_inflated_size = 65280
cdef unsigned int _block_metadata_size = sizeof(BlockHeader) + sizeof(BlockHeaderBGZipSubfield) + sizeof(BlockTailer)
block_data_inflated_size = _block_data_inflated_size
block_metadata_size = _block_metadata_size

cdef void _get_buffer(PyObject * obj, Py_buffer * view):
    cdef int err

    err = PyObject_GetBuffer(obj, view, PyBUF_SIMPLE)
    if -1 == err:
        raise Exception()

def deflate_to_buffers(py_input_buff, list py_deflated_buffers, int num_threads):
    """
    Compress the data in `py_input_buff` and write it to `handle`.

    `deflated_buffers` should contain enough buffers to hold the number of blocks compressed. Each
    buffer should hold `_block_data_inflated_size + _block_metadata_size` bytes.
    """
    cdef int i, chunk_size
    cdef unsigned int bytes_available = len(py_input_buff)
    cdef int number_of_chunks = min(ceil(bytes_available / block_data_inflated_size),
                                    len(py_deflated_buffers))
    cdef Block blocks[BLOCK_BATCH_SIZE]
    cdef PyObject * deflated_buffers = <PyObject *>py_deflated_buffers
    cdef PyObject * compressed_chunk

    cdef Py_buffer input_view 
    _get_buffer(<PyObject *>py_input_buff, &input_view)

    with nogil:
        for i in range(number_of_chunks):
            compressed_chunk = <PyObject *>PyList_GetItem(deflated_buffers, i)

            if bytes_available >= _block_data_inflated_size:
                chunk_size = _block_data_inflated_size
            else:
                chunk_size = bytes_available

            bytes_available -= _block_data_inflated_size

            blocks[i].inflated_size = chunk_size
            blocks[i].next_in = <Bytef *>input_view.buf + (i * _block_data_inflated_size)
            blocks[i].available_in = chunk_size
            blocks[i].next_out = <Bytef *>PyByteArray_AS_STRING(compressed_chunk)
            blocks[i].avail_out = _block_data_inflated_size + _block_metadata_size

        for i in prange(number_of_chunks, num_threads=num_threads, schedule="dynamic"):
            if BGZIP_OK != compress_block(&blocks[i]):
                with gil:
                    raise BGZIPException()

    PyBuffer_Release(&input_view)

    return [blocks[i].block_size for i in range(number_of_chunks)]
