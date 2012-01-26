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

install_requires = []
with contextlib.closing(open("packages.txt")) as fp:
  for s in fp:
    package = re.sub(r'#.*$', '', s.strip())
    if 0 < len(package):
      install_requires.append(package)

setup(
  name='botornado',
  version='0.0.2git',
  description='boto on tornado - an asynchronous Amazon Web Service (AWS) client',
  author='Yamashita, Yuu',
  author_email='yamashita@geishatokyo.com',
  url='https://github.com/yyuu/botornado',
  install_requires=install_requires,
  packages=find_packages(),
  package_data={
     'botornado': [],
  },
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

# vim:set ft=python :
