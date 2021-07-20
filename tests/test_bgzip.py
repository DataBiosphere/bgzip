#!/usr/bin/env python
import io
import os
import sys
import time
import gzip
import unittest
from random import randint
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
                a = bytearray()
                while True:
                    data = fh.read(randint(1024 * 1024 * 1, 1024 * 1024 * 10))
                    if not data:
                        break
                    try:
                        a.extend(data)
                    finally:
                        data.release()

        self.assertEqual(a, expected_data)

    def test_read_all(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with gzip.GzipFile(fileobj=raw) as fh:
                expected_data = fh.read()

        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with bgzip.BGZipReader(raw) as fh:
                data = bytearray()
                while True:
                    d = fh.read(1024 * 1024)
                    if not d:
                        break
                    try:
                        data += d
                    finally:
                        d.release()

        self.assertEqual(data, expected_data)

    def test_read_into_better(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            a = bytearray()
            with bgzip.BGZipReader(raw) as fh:
                while True:
                    data = fh.read(30 * 1024 * 1024)
                    if not data:
                        break
                    try:
                        a.extend(data)
                    finally:
                        data.release()

        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with gzip.GzipFile(fileobj=raw) as fh:
                b = fh.read()

        self.assertEqual(a, b)

    def test_buffers(self):
        with self.subTest("Should be able to pass in bytearray"):
            bgzip.BGZipReader(io.BytesIO(), bytearray(b"laskdf"))
        with self.subTest("Should be able to pass in memoryview to bytearray"):
            bgzip.BGZipReader(io.BytesIO(), memoryview(bytearray(b"laskdf")))
        with self.subTest("Should NOT be able to pass in bytes"):
            with self.assertRaises(ValueError):
                bgzip.BGZipReader(io.BytesIO(), b"laskdf")
        with self.subTest("Should NOT be able to pass in memoryview to bytes"):
            with self.assertRaises(ValueError):
                bgzip.BGZipReader(io.BytesIO(), b"laskdf")
        with self.subTest("Should NOT be able to pass in non-bytes-like object"):
            with self.assertRaises(TypeError):
                bgzip.BGZipReader(io.BytesIO(), 2)

class TestBGZipWriter(unittest.TestCase):
    def test_write(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with gzip.GzipFile(fileobj=raw) as fh:
                inflated_data = fh.read()

        fh_out = io.BytesIO()
        with bgzip.BGZipWriter(fh_out) as writer:
            n = 987345
            writer.write(inflated_data[:n])
            writer.write(inflated_data[n:])

        fh_out.seek(0)
        with bgzip.BGZipReader(fh_out) as reader:
            reinflated_data = bytearray()
            while True:
                d = reader.read(1024 * 1024)
                if d:
                    reinflated_data += d
                else:
                    break
        self.assertEqual(inflated_data, reinflated_data)
        self.assertTrue(fh_out.getvalue().endswith(bgzip.bgzip_eof))

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
            with profile(f"BGZipReader (num_threads={num_threads})"):
                with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
                    with bgzip.BGZipReader(raw, num_threads=num_threads) as reader:
                        while True:
                            data = reader.read(randint(1024 * 1024 * 1, 1024 * 1024 * 10))
                            if not data:
                                break

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
