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
  version='0.0.1',
  description='botornado',
  author='Yamashita, Yuu',
  author_email='yamashita@geishatokyo.com',
  url='https://github.com/yyuu/botornado',
  install_requires=install_requires,
  packages=find_packages(),
  test_suite='botornado.test',
)

# vim:set ft=python :
