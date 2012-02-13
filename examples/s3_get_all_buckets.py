#!/usr/bin/env python

import os
import sys
import time
import tornado.ioloop
import tornado.options

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) # FIXME
import botornado.s3
from botornado.s3.connection import AsyncS3Connection

tornado.options.parse_command_line(sys.argv)

s3 = AsyncS3Connection(aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))

def get_all_buckets():
  def cb(response):
    print response
    sys.exit(0)
  s3.get_all_buckets(callback=cb)

ioloop = tornado.ioloop.IOLoop.instance()
ioloop.add_timeout(time.time(), get_all_buckets)
ioloop.start()

# vim:set ft=python :
