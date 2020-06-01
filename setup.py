import os
import glob
import platform
from setuptools import setup, find_packages
from distutils.extension import Extension


install_requires = [line.rstrip() for line in open(os.path.join(os.path.dirname(__file__), "requirements.txt"))]

if "Darwin" == platform.system():
    extra_compile_args = ["-O3", "-Xpreprocessor", "-fopenmp"]
    extra_link_args = ["-lomp"]
else:
    extra_compile_args = ["-O3", "-fopenmp"]
    extra_link_args = ["-fopenmp"]

file_ext = "pyx" if os.environ.get("BUILD_WITH_CYTHON") else "c"
extensions = [
    Extension(
        name="bgzip.bgzip_utils",
        sources=[f"bgzip_utils/bgzip_utils.{file_ext}"],
        libraries=["z"],
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
    )
]

if os.environ.get("BUILD_WITH_CYTHON"):
    from Cython.Build import cythonize
    extensions = cythonize(extensions)

with open("README.md") as fh:
    long_description = fh.read()

setup(
    name='bgzip',
    version='0.3.2',
    description='Utilities working with blocked gzip streams.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/xbrianh/bgzip.git',
    author='Brian Hannafious',
    author_email='bhannafi@ucsc.edu',
    license='MIT',
    packages=find_packages(exclude=['tests']),
    ext_modules=extensions,
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
