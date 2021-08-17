#!/usr/bin/env python
import io
import os
import sys
import gzip
import unittest
from random import randint

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from bgzip import utils


class TestUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            cls.src_data = raw.read()

        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with gzip.GzipFile(fileobj=raw) as fh:
                cls.expected_data = fh.read()

    def test_inflate(self):
        dst = memoryview(bytearray(1024 ** 2 * 100))
        bytes_inflated = utils.inflate(self.src_data, dst)
        self.assertEqual(self.expected_data, bytes(dst[:bytes_inflated]))

if __name__ == '__main__':
    unittest.main()
