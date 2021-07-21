#!/usr/bin/env python
import io
import os
import sys
import gzip
import unittest
from random import randint

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import bgzip


class TestBGZipReader(unittest.TestCase):
    def test_read(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with gzip.GzipFile(fileobj=raw) as fh:
                expected_data = fh.read()

        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with bgzip.BGZipReader(raw, 1024 * 1024 * 1) as fh:
                data = bytearray()
                while True:
                    d = fh.read(randint(1024 * 1, 1024 * 1024 * 1024))
                    if not d:
                        break
                    try:
                        data += d
                    finally:
                        d.release()

        self.assertEqual(expected_data, data)

    def test_empty(self):
        with bgzip.BGZipReader(io.BytesIO()) as fh:
            d = fh.read(1024)
            self.assertEqual(0, len(d))

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

    def test_read_into(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with gzip.GzipFile(fileobj=raw) as fh:
                expected_data = fh.read()

        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            data = bytearray()
            with bgzip.BGZipReader(raw) as fh:
                while True:
                    d = fh.read(30 * 1024 * 1024)
                    if not d:
                        break
                    try:
                        data += d
                    finally:
                        d.release()
        self.assertEqual(expected_data, data)

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

if __name__ == '__main__':
    unittest.main()
