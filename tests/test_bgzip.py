#!/usr/bin/env python
import io
import os
import sys
import time
import unittest
import gzip
from contextlib import AbstractContextManager

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import bgzip


class TestBGZipReader(unittest.TestCase):
    def test_read(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with gzip.GzipFile(fileobj=raw) as fh:
                expected_data = fh.read()

        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with bgzip.BGZipReader(raw) as fh:
                a = fh.read()

        self.assertEqual(a, expected_data[:-1])

    def test_read_into_better(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            a = bytearray()
            with bgzip.BGZipReaderCircularBuff(raw) as fh:
                while True:
                    data = fh.read(30 * 1024 * 1024)
                    if not data:
                        break
                    a.extend(data)

        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with gzip.GzipFile(fileobj=raw) as fh:
                b = fh.read()

        self.assertEqual(a, b)


class TestBGZipReaderCircularBuff(TestBGZipReader):
    def test_read(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with gzip.GzipFile(fileobj=raw) as fh:
                expected_data = fh.read()

        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with bgzip.BGZipReader(raw) as fh:
                a = bytearray()
                while True:
                    data = fh.read(1024 * 1024)
                    if not data:
                        break
                    a.extend(data)

        self.assertEqual(a, expected_data)


class TestBGZipWriter(unittest.TestCase):
    def test_write(self):
        self._test_write(bgzip.BGZipWriter)

    def test_async_write(self):
        self._test_write(bgzip.AsyncBGZipWriter)

    def _test_write(self, writer_class):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with gzip.GzipFile(fileobj=raw) as fh:
                inflated_data = fh.read()

        fh_out = io.BytesIO()
        with writer_class(fh_out) as writer:
            n = 987345
            writer.write(inflated_data[:n])
            writer.write(inflated_data[n:])

        fh_out.seek(0)
        with bgzip.BGZipReader(fh_out) as reader:
            reinflated_data = reader.read()
        self.assertEqual(inflated_data[:-1], reinflated_data)

    def test_pathalogical_write(self):
        fh = io.BytesIO()
        with bgzip.BGZipWriter(fh):
            fh.write(b"")


class TestProfileBGZip(unittest.TestCase):
    def test_profile_read(self):
        print()
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with profile("gzip read"):
                with gzip.GzipFile(fileobj=raw) as fh:
                    fh.read()

        for num_threads in range(1, 1 + bgzip.available_cores):
            with profile(f"bgzip read (num_threads={num_threads})"):
                with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
                    with bgzip.BGZipReader(raw, num_threads=num_threads) as fh:
                        fh.read()
        print()

    def test_profile_write(self):
        print()
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with gzip.GzipFile(fileobj=raw) as fh:
                inflated_data = fh.read()

        with profile("gzip write"):
            with gzip.GzipFile(fileobj=io.BytesIO(), mode="w") as fh:
                fh.write(inflated_data)

        for num_threads in range(1, 1 + bgzip.available_cores):
            with profile(f"bgzip write (num_threads={num_threads})"):
                with bgzip.BGZipWriter(io.BytesIO(), num_threads=num_threads) as writer:
                    n = 987345
                    writer.write(inflated_data[:n])
                    writer.write(inflated_data[n:])
        print()


class profile(AbstractContextManager):
    """
    Profile methods or code blocks with decorators or contexts, respectively.

    @profile("profiling my method")
    def my_method(*args, **kwargs):
       ...

    with profile("profiling my block"):
       ...
    """
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


if __name__ == '__main__':
    unittest.main()
