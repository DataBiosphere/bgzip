import os
import glob
import subprocess
from setuptools import setup, find_packages


install_requires = [line.rstrip() for line in open(os.path.join(os.path.dirname(__file__), "requirements.txt"))]

with open("README.md") as fh:
    long_description = fh.read()

def get_version():
    filepath = os.path.join(os.path.dirname(__file__), "bgzip", "version.py")
    if os.path.isfile(filepath):
        # Pick up the version packaged into a wheel or source distribution
        with open(filepath) as fh:
            version = dict()
            exec(fh.read().strip(), version)
            return version['__version__']
    else:
        # Pick up version from git annotated tag (canonical version)
        p = subprocess.run(["git", "describe", "--tags", "--match", "v*.*.*"], stdout=subprocess.PIPE)
        p.check_returncode()
        out = p.stdout.decode("ascii").strip()
        if "-" in out:
            out = out.split("-", 1)[0]
        assert out.startswith("v")
        return out[1:]

setup(
    name='bgzip',
    version=get_version(),
    description='Utilities working with blocked gzip streams.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/xbrianh/bgzip.git',
    author='Brian Hannafious',
    author_email='bhannafi@ucsc.edu',
    license='MIT',
    packages=find_packages(exclude=['tests']),
    scripts=glob.glob('scripts/*'),
    zip_safe=False,
    install_requires=install_requires,
    platforms=['Posix', 'MacOS X'],
    test_suite='test',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7'
    ]
)
