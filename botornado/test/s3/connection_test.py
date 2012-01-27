#!/usr/bin/env python

import tornado.testing
import tornado.web

from botornado.s3 import *
from botornado.s3.connection import *

class S3Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def delete(self):
        self.write("deleted")
        self.finish()

    @tornado.web.asynchronous
    def head(self):
        self.write("headed")
        self.finish()

    @tornado.web.asynchronous
    def get(self):
        # http://docs.amazonwebservices.com/AmazonS3/2006-03-01/API/RESTServiceGET.html
        self.write("""<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://doc.s3.amazonaws.com/2006-03-01">
  <Owner>
    <ID>bcaf1ffd86f461ca5fb16fd081034f</ID>
    <DisplayName>webfile</DisplayName>
  </Owner>
  <Buckets>
    <Bucket>
      <Name>quotes</Name>
      <CreationDate>2006-02-03T16:45:09.000Z</CreationDate>
    </Bucket>
    <Bucket>
      <Name>samples</Name>
      <CreationDate>2006-02-03T16:41:58.000Z</CreationDate>
    </Bucket>
  </Buckets>
</ListAllMyBucketsResult>
""")
        self.finish()

    @tornado.web.asynchronous
    def post(self):
        self.write("posted")
        self.finish()

    @tornado.web.asynchronous
    def put(self):
        self.write("put")
        self.finish()

class AsyncHTTPClientStub(object):
    def __init__(self, async_test_case):
        self.async_test_case = async_test_case

    def fetch(self, path, callback=None, **kwargs):
        def fetched(response):
            self.async_test_case.stop()
            callback(response)
        self.async_test_case.http_client.fetch(path, callback=fetched, **kwargs)
        return self.async_test_case.wait()

class S3TestCase(tornado.testing.AsyncHTTPTestCase, tornado.testing.LogTrapTestCase):
    def get_app(self):
        return tornado.web.Application([(r'.*', S3Handler)])

    def setUp(self):
        super(S3TestCase, self).setUp()
        self.s3_client = AsyncS3Connection(aws_access_key_id='public', aws_secret_access_key='secret',
                                           host='127.0.0.1', port=self.get_http_port(),
                                           is_secure=False, http_client=AsyncHTTPClientStub(self))

    def test_get_all_buckets(self):
        def got(buckets):
            self.assertTrue(buckets)
            self.assertTrue(buckets, list)
            self.assertTrue(['quotes', 'samples'], map(lambda bucket: bucket.name, buckets))
        self.s3_client.get_all_buckets(callback=got)

if __name__ == '__main__':
    tornado.testing.main()

# vim:set ft=python sw=4 :
