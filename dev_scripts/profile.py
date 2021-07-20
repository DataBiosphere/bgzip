#!/usr/bin/env python
import io
import os
import sys
import time
import gzip
from random import randint
from multiprocessing import cpu_count
from contextlib import AbstractContextManager

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import bgzip


VCF_FILEPATH = os.path.join(pkg_root, "dev_scripts", "partial.vcf.gz")
UNCOMPRESSED_LENGTH = 22242386 

class profile(AbstractContextManager):
    def __init__(self, name="default"):
        self.name = name

    def __enter__(self, *args, **kwargs):
        self.start = time.time()

    def __exit__(self, *args, **kwargs):
        self._print(time.time() - self.start)

    def _print(self, duration):
        print(f"{self.name} took {duration} seconds")

    def __call__(self, meth):
        def wrapper(*args, **kwargs):
            start = time.time()
            res = meth(*args, **kwargs)
            dur = time.time() - start
            self._print(dur)
            return res
        return wrapper

def profile_read():
    with open(VCF_FILEPATH, "rb") as raw:
        with profile("gzip read"):
            with gzip.GzipFile(fileobj=raw) as fh:
                data = fh.read()
            assert UNCOMPRESSED_LENGTH == len(data)

    for num_threads in range(1, 1 + cpu_count()):
        with open(VCF_FILEPATH, "rb") as raw:
            with profile(f"BGZipReader read (num_threads={num_threads})"):
                reader = bgzip.BGZipReader(raw, num_threads=num_threads)
                data = bytearray()
                while True:
                    d = reader.read(randint(1024 * 1024 * 1, 1024 * 1024 * 10))
                    if not d:
                        break
                    try:
                        data += d
                    finally:
                        d.release()
                assert UNCOMPRESSED_LENGTH == len(data)

def profile_iter_blocks():
    with open(VCF_FILEPATH, "rb") as raw:
        with profile("gzip read"):
            with gzip.GzipFile(fileobj=raw) as fh:
                data = fh.read()
            assert UNCOMPRESSED_LENGTH == len(data)

    for num_threads in range(1, 1 + cpu_count()):
        with open(VCF_FILEPATH, "rb") as raw:
            with profile(f"BGZipReader iter_blocks (num_threads={num_threads})"):
                data = bytearray()
                for d in bgzip.BGZipReader.iter_blocks(raw, num_threads=num_threads):
                    data.extend(d)
                assert UNCOMPRESSED_LENGTH == len(data)

def profile_write():
    with open(VCF_FILEPATH, "rb") as raw:
        with gzip.GzipFile(fileobj=raw) as fh:
            inflated_data = fh.read()

    with profile("gzip write"):
        with gzip.GzipFile(fileobj=io.BytesIO(), mode="w") as fh:
            fh.write(inflated_data)

    for num_threads in range(1, 1 + cpu_count()):
        with profile(f"bgzip write (num_threads={num_threads})"):
            with bgzip.BGZipWriter(io.BytesIO(), num_threads=num_threads) as writer:
                n = 987345
                writer.write(inflated_data[:n])
                writer.write(inflated_data[n:])

if __name__ == "__main__":
    profile_read()
    print()
    profile_iter_blocks()
    print()
    profile_write()
