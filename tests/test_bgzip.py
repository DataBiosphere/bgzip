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
    @classmethod
    def setUpClass(cls):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with gzip.GzipFile(fileobj=raw) as fh:
                cls.expected_data = fh.read()

    def test_read(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with bgzip.BGZipReader(raw, 1024 * 1024 * 1) as fh:
                data = bytearray()
                while True:
                    d = fh.read(randint(1024 * 1, 1024 * 1024 * 1024))
                    if not d:
                        break
                    data.extend(d)
                    d.release()
        self.assertEqual(self.expected_data, data)

    def test_empty(self):
        with bgzip.BGZipReader(io.BytesIO()) as fh:
            d = fh.read(1024)
            self.assertEqual(0, len(d))

    def test_read_all(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with bgzip.BGZipReader(raw) as fh:
                data = fh.read()
        self.assertEqual(data, self.expected_data)

    def test_read_into(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            data = bytearray()
            with bgzip.BGZipReader(raw) as fh:
                while True:
                    d = fh.read(30 * 1024 * 1024)
                    if not d:
                        break
                    data.extend(d)
                    d.release()
        self.assertEqual(self.expected_data, data)

    def test_iter(self):
        with self.subTest("iter byte lines"):
            data = b""
            with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
                with bgzip.BGZipReader(raw, 1024 * 1024 * 1) as fh:
                    for line in fh:
                        data += line
            self.assertEqual(self.expected_data, data)

        with self.subTest("iter text lines"):
            content = ""
            with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
                with bgzip.BGZipReader(raw, 1024 * 1024 * 1) as fh:
                    with io.TextIOWrapper(fh, "utf-8") as handle:
                        for line in handle:
                            content += line
            self.assertEqual(self.expected_data.decode("utf-8"), content)

class TestBGZipWriter(unittest.TestCase):
    def test_write(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with gzip.GzipFile(fileobj=raw) as fh:
                inflated_data = fh.read()

        deflate_buffers = bgzip.gen_deflate_buffers(3)
        deflated_with_buffers = bytes()
        data = memoryview(bytes(inflated_data))
        while data:
            bytes_deflated, deflated_blocks = bgzip.deflate_to_buffers(data, deflate_buffers, 2)
            data = data[bytes_deflated:]
            deflated_with_buffers += b"".join(deflated_blocks)
        deflated_with_buffers += bgzip.bgzip_eof

        fh_out = io.BytesIO()
        with bgzip.BGZipWriter(fh_out) as writer:
            n = 987345
            writer.write(inflated_data[:n])
            writer.write(inflated_data[n:])
        deflated_with_writer = fh_out.getvalue()

        self.assertEqual(deflated_with_buffers, deflated_with_writer)

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
        self.assertTrue(deflated_with_writer.endswith(bgzip.bgzip_eof))

    def test_write_random_data(self):
        inflated_data = os.urandom(1024 * 1024)
        with bgzip.BGZipWriter(io.BytesIO()) as writer:
            writer.write(inflated_data)

    def test_pathalogical_write(self):
        fh = io.BytesIO()
        with bgzip.BGZipWriter(fh):
            fh.write(b"")

if __name__ == '__main__':
    unittest.main()
