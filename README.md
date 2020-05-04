# bgzip: block gzip streams
_bgzip_ provides streams for block gzip files.

Cython is used under the hood to bypass Python's GIL and provide fast, parallelized inflation/deflation.

```
with open("my_bgzipped_file.gz", "rb") as raw:
	with bgzip.Reader(raw) as fh:
		data = fh.read(number_of_bytes)

with open("my_bgzipped_file.gz", "wb") as raw:
	with bgzip.Writer(raw) as fh:
		fh.write(my_data)
```

## Installation

```
pip install bgzip
```

#### Requirements
bgzip requires [openmp](https://github.com/llvm/llvm-project/tree/master/openmp). On MacOS
it can be installed with:
```
brew install openmp
```

## Links
Project home page [GitHub](https://github.com/xbrianh/bgzip)  
Package distribution [PyPI](https://pypi.org/project/bgzip/)

### Bugs
Please report bugs, issues, feature requests, etc. on [GitHub](https://github.com/xbrianh/bgzip).

![](https://travis-ci.org/xbrianh/bgzip.svg?branch=master) ![](https://badge.fury.io/py/bgzip.svg)
