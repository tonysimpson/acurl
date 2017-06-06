from __future__ import division, absolute_import, print_function

import os
import sys
from setuptools import setup
from setuptools.extension import Extension
from glob import glob


with open(os.path.join('acurl','version.py')) as f:
    exec(f.read())


# Building without nanoconfig
cpy_extension = Extension('_acurl',
                          sources=['src/acurl.c', 'src/ae/ae.c','src/ae/zmalloc.c'],
                          libraries=['curl']
                         )


install_requires = []


setup(
    name='acurl',
    version=__version__,
    packages=['acurl'],
    ext_modules=[cpy_extension],
    install_requires=install_requires,
    description='An async Curl library.',
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
    ],
    author='Tony Simpson',
    author_email='agjasimpson@gmail.com',
    license='MIT',
    test_suite="tests",
)
