.PHONY: test lint mypy tests clean build install
MODULES=bgzip tests

test: lint mypy tests

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
	-rm -rf build dist
	-rm -rf *.egg-info

build: version clean
	-rm -rf dist
	python setup.py build_ext --inplace
	python setup.py bdist_wheel

install: build
	pip install --upgrade dist/*.whl
