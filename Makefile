include common.mk

MODULES=bgzip tests

test: lint mypy tests

profile:
	python dev_scripts/profile.py

lint:
	flake8 $(MODULES) *.py

mypy:
	mypy --ignore-missing-imports $(MODULES)

tests:
	PYTHONWARNINGS=ignore:ResourceWarning coverage run --source=bgzip \
		-m unittest discover --start-directory tests --top-level-directory . --verbose

version: bgzip/version.py

bgzip/version.py: setup.py
	echo "__version__ = '$$(python setup.py --version)'" > $@

clean:
	git clean -dfx

build: clean version
	python setup.py bdist_wheel

sdist: clean version
	python setup.py sdist

install: build
	pip install --upgrade dist/*.whl

.PHONY: test profile lint mypy tests clean build sdist install
