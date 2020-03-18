import glob
import os
from setuptools import setup, find_packages
from Cython.Build import cythonize
from distutils.extension import Extension

install_requires = [line.rstrip() for line in open(os.path.join(os.path.dirname(__file__), "requirements.txt"))]

extensions = [
    Extension(
        name="bgzip.bgzip_utils",
        sources=["bgzip_utils/bgzip_utils.pyx"],
        libraries=["z"],
        extra_compile_args=['-O3', '-fopenmp'],
        extra_link_args=['-fopenmp'],
    )
]

setup(
    name='bgzip',
    version='0.0.0',
    description='Utilities working with blocked gzip streams.',
    url='https://github.com/xbrianh/bgzip.git',
    author='Brian Hannafious',
    author_email='bhannafi@ucsc.edu',
    license='MIT',
    packages=find_packages(exclude=['tests']),
    scripts=glob.glob('scripts/*'),
    zip_safe=False,
    install_requires=install_requires,
    platforms=['MacOS X', 'Posix'],
    test_suite='test',
    ext_modules=cythonize(extensions),
    classifiers=[
        'Intended Audience :: Bioinformatics developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7'
    ]
)
