#!/usr/bin/env python
"""
Verify the results of bgzip are identicle to gzip for a block gzipped file.
"""
import os
import sys
import gzip

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import bgzip

filepath = sys.argv[1]

with open(filepath, "rb") as raw1:
    with open(filepath, "rb") as raw2:
        with gzip.GzipFile(fileobj=raw1) as gzip_reader:
            with bgzip.BGZipReader(raw2, num_threads=6) as bgzip_reader:
                while True:
                    a = bgzip_reader.read(1024 * 1024)
                    b = gzip_reader.read(len(a))
                    assert a == b
                    if not (a or b):
                        break
