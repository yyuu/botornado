#!/usr/bin/env python

from __future__ import with_statement
import contextlib
import re

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name='botornado',
    version='0.0.2',
    description='boto on tornado - an asynchronous Amazon Web Service (AWS) client',
    author='Yamashita, Yuu',
    author_email='yamashita@geishatokyo.com',
    url='https://github.com/yyuu/botornado',
    install_requires=[
#       "boto==2.2.2", # current version of botornado includes tested version of boto in source tree
        "tornado>=2.1.1",
    ],
    packages=find_packages(),
    test_suite='botornado.test',
    license='MIT',
    platforms="Posix; MacOS X; Windows",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Internet"
    ],
)

# vim:set ft=python sw=4 :
