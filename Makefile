include common.mk

MODULES=bgzip tests

test: lint mypy bgzip_utils tests

lint:
	flake8 $(MODULES) *.py

mypy:
	mypy --ignore-missing-imports --no-strict-optional $(MODULES)

tests:
	PYTHONWARNINGS=ignore:ResourceWarning coverage run --source=bgzip \
		-m unittest discover --start-directory tests --top-level-directory . --verbose

version: bgzip/version.py

bgzip/version.py: setup.py
	echo "__version__ = '$$(python setup.py --version)'" > $@

clean:
	git clean -dfx

bgzip_utils.c: clean
	cython bgzip_utils/bgzip_utils.pyx

bgzip_utils: clean
	BUILD_WITH_CYTHON=1 python setup.py build_ext --inplace

build: clean version
	BUILD_WITH_CYTHON=1 python setup.py bdist_wheel

sdist: clean version bgzip_utils.c
	python setup.py sdist

install: build
	pip install --upgrade dist/*.whl

.PHONY: test lint mypy tests clean build sdist install
