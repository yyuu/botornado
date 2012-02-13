#!/usr/bin/env python

import os
import sys
import time
import tornado.ioloop
import tornado.options

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) # FIXME
import botornado.sqs
from botornado.sqs.connection import AsyncSQSConnection

tornado.options.parse_command_line(sys.argv)

sqs = botornado.sqs.connect_to_region('ap-northeast-1',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))

def get_all_queues():
  def cb(response):
    print response
    sys.exit(0)
  sqs.get_all_queues(callback=cb)

ioloop = tornado.ioloop.IOLoop.instance()
ioloop.add_timeout(time.time(), get_all_queues())
ioloop.start()

# vim:set ft=python :
