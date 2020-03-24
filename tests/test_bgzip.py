#!/usr/bin/env python
import io
import os
import sys
import time
import unittest
import gzip
from random import randint
from contextlib import AbstractContextManager

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import bgzip


class TestBGZipReader(unittest.TestCase):
    def _get_reader(self, handle):
        return bgzip.BGZipReader(handle)

    def test_read(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with gzip.GzipFile(fileobj=raw) as fh:
                expected_data = fh.read()

        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with self._get_reader(raw) as fh:
                a = bytearray()
                while True:
                    data = fh.read(randint(1024 * 1024 * 1, 1024 * 1024 * 10))
                    if not data:
                        break
                    a.extend(data)

        self.assertEqual(a, expected_data)

    def test_read_all(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with gzip.GzipFile(fileobj=raw) as fh:
                expected_data = fh.read()

        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with self._get_reader(raw) as fh:
                a = fh.read()

        self.assertEqual(a, expected_data[:-1])

    def test_read_into_better(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            a = bytearray()
            with self._get_reader(raw) as fh:
                while True:
                    data = fh.read(30 * 1024 * 1024)
                    if not data:
                        break
                    a.extend(data)

        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with gzip.GzipFile(fileobj=raw) as fh:
                b = fh.read()

        self.assertEqual(a, b)


class TestBGZipReaderPreAllocated(TestBGZipReader):
    reader_class = bgzip.BGZipReaderPreAllocated

    def _get_reader(self, handle):
        return self.reader_class(handle, memoryview(bytearray(1024 * 1024 * 50)))

    def test_read_all(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with self._get_reader(raw) as fh:
                with self.assertRaises(TypeError):
                    fh.read()

    def test_buffers(self):
        with self.subTest("Should be able to pass in bytearray"):
            self.reader_class(io.BytesIO(), bytearray(b"laskdf"))
        with self.subTest("Should be able to pass in memoryview to bytearray"):
            self.reader_class(io.BytesIO(), memoryview(bytearray(b"laskdf")))
        with self.subTest("Should NOT be able to pass in bytes"):
            with self.assertRaises(ValueError):
                self.reader_class(io.BytesIO(), b"laskdf")
        with self.subTest("Should NOT be able to pass in memoryview to bytes"):
            with self.assertRaises(ValueError):
                self.reader_class(io.BytesIO(), b"laskdf")
        with self.subTest("Should NOT be able to pass in non-bytes-like object"):
            with self.assertRaises(TypeError):
                self.reader_class(io.BytesIO(), 2)


class TestBGZipAsyncReaderPreAllocated(TestBGZipReaderPreAllocated):
    reader_class = bgzip.BGZipAsyncReaderPreAllocated


class TestBGZipWriter(unittest.TestCase):
    writer_class = bgzip.BGZipWriter

    def test_write(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with gzip.GzipFile(fileobj=raw) as fh:
                inflated_data = fh.read()

        fh_out = io.BytesIO()
        with self.writer_class(fh_out) as writer:
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


class TestAsyncBGZipWriter(TestBGZipWriter):
    writer_class = bgzip.AsyncBGZipWriter  # type: ignore


class TestProfileBGZip(unittest.TestCase):
    buf = memoryview(bytearray(1024 * 1024 * 50))

    def _get_reader_for_class(self, reader_class, handle, num_threads):
        if reader_class.__name__ == "BGZipReader":
            return bgzip.BGZipReader(handle, num_threads=num_threads)
        else:
            return reader_class(handle, self.buf, num_threads=num_threads)

    def test_profile_read(self):
        print()
        for reader_class in [bgzip.BGZipReader, bgzip.BGZipReaderPreAllocated, bgzip.BGZipAsyncReaderPreAllocated]:
            with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
                with profile("gzip read"):
                    with gzip.GzipFile(fileobj=raw) as fh:
                        fh.read()

            for num_threads in range(1, 1 + bgzip.available_cores):
                with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
                    reader = self._get_reader_for_class(reader_class, raw, num_threads)
                    with profile(f"{reader_class.__name__} read (num_threads={num_threads})"):
                        while True:
                            data = reader.read(randint(1024 * 1024 * 1, 1024 * 1024 * 10))
                            if not data:
                                break
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
