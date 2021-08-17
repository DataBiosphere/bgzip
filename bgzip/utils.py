import zlib
import struct
from ctypes import util as ctypes_util

import numba
import numpy as np


foo = ctypes_util.find_library("c")
print(foo)

MAGIC = b"\x1f\x8b\x08\x04"  # b"\037\213\010\4"
MAGIC_SIZE = len(MAGIC)

BOCK_HEADER = np.dtype([
    ("mod_time", "<i4"),
    ("flags", "<B"),
    ("os_type", "<B"),
    ("extra_len", "<u2")
])
BOCK_HEADER_SIZE = BOCK_HEADER.itemsize

HEADER_SUBFIELD = np.dtype([
    ("id", "<B", (2,)),
    ("length", "<u2")
])
HEADER_SUBFIELD_SIZE = HEADER_SUBFIELD.itemsize

HEADER_SUBFIELD_BLOCK_SIZE = np.dtype([
    ("block_size", "<u2")
])
HEADER_SUBFIELD_BLOCK_SIZE_SIZE = HEADER_SUBFIELD_BLOCK_SIZE.itemsize

BLOCK_TAILER = np.dtype([
    ("crc", "<u4"),
    ("inflated_size", "<u4"),
])
BLOCK_TAILER_SIZE = BLOCK_TAILER.itemsize

DATA_BLOCK = np.dtype([
    ("start", int),
    ("deflated_size", int),
    ("inflated_size", int),
    ("crc", int),
])

BLOCKS = np.empty((100000,), dtype=DATA_BLOCK)
    
def _inflate_blocks(src: memoryview, dst: memoryview, number_of_blocks: int) -> int:
    offset = 0
    for block in BLOCKS[:number_of_blocks]:
        deflated_data = src[block['start']: block['start'] + block['deflated_size']]
        inflated_data = zlib.decompress(deflated_data, wbits=-15)
        dst[offset: offset + len(inflated_data)] = inflated_data
        offset += len(inflated_data)
    return offset

@numba.jit(nopython=True)
def _read_blocks(src: memoryview):
    offset = 0
    for i in range(len(BLOCKS)):
        # TODO: figure out how to do this check in numba
        # assert MAGIC == src[offset: offset + MAGIC_SIZE], "gzip magic not found at beginning of buffer"
        offset += MAGIC_SIZE

        header = np.frombuffer(src[offset: offset + BOCK_HEADER_SIZE], BOCK_HEADER)[0]
        offset += BOCK_HEADER_SIZE

        header_subfield = np.frombuffer(src[offset: offset + HEADER_SUBFIELD_SIZE], HEADER_SUBFIELD)[0]
        offset += HEADER_SUBFIELD_SIZE

        subfield_data = src[offset: offset + header_subfield['length']]
        offset += header_subfield['length']

        assert 66 == header_subfield['id'][0] and 67 == header_subfield['id'][1], "Malformed header subfield"
        assert header['extra_len'] == HEADER_SUBFIELD_SIZE + header_subfield['length'], "Malformed header subfield length"

        if 2 == header_subfield['length']:
            block_size = np.frombuffer(subfield_data, HEADER_SUBFIELD_BLOCK_SIZE)[0]['block_size']

        block_start = offset

        deflated_size = 1 + block_size - BOCK_HEADER_SIZE - HEADER_SUBFIELD_SIZE - header['extra_len'] - BLOCK_TAILER_SIZE
        offset += deflated_size

        tailer = np.frombuffer(src[offset: offset + BLOCK_TAILER_SIZE], BLOCK_TAILER)[0]
        offset += BLOCK_TAILER_SIZE

        if not tailer['inflated_size']:
            break

        BLOCKS[i]['start'] = block_start
        BLOCKS[i]['deflated_size'] = deflated_size
        BLOCKS[i]['inflated_size'] = tailer['inflated_size']
        BLOCKS[i]['crc'] = tailer['crc']
    return i

def inflate(src: memoryview, dst: memoryview) -> int:
    number_of_blocks = _read_blocks(src)
    bytes_inflated = _inflate_blocks(src, dst, number_of_blocks)
    return bytes_inflated
