#!/usr/bin/env python
import io
import os
import sys
import unittest
import gzip
from datetime import datetime
from unittest import mock

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import bgzip


class TestBGZip(unittest.TestCase):
    def test_read(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with bgzip.BGZipReader(raw) as fh:
                a = fh.read()

        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with gzip.GzipFile(fileobj=raw) as fh:
                b = fh.read()

        self.assertEqual(a, b[:-1])

    def test_write(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as fh:
            deflated_data = fh.read()

        with gzip.GzipFile(fileobj=io.BytesIO(deflated_data)) as fh:
            inflated_data = fh.read()

        fh_out = io.BytesIO()
        with bgzip.AsyncBGZipWriter(fh_out) as writer:
            n = 987345
            writer.write(inflated_data[:n])
            writer.write(inflated_data[n:])

        with gzip.GzipFile(fileobj=io.BytesIO(), mode="w") as fh:
            fh.write(inflated_data)

        fh_out.seek(0)
        with bgzip.BGZipReader(fh_out) as reader:
            reinflated_data = reader.read()
        self.assertEqual(inflated_data[:-1], reinflated_data)

        with open("out.vcf.gz", "wb") as fh:
            fh.write(fh_out.getvalue())

    def test_pathalogical_write(self):
        fh = io.BytesIO()
        with bgzip.BGZipWriter(fh):
            fh.write(b"")

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


if __name__ == '__main__':
    unittest.main()
