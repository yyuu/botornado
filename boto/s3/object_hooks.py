# Copyright (c) 2006-2010 Mitch Garnaat http://garnaat.org/
# Copyright (c) 2010, Eucalyptus Systems, Inc.
# All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, 
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

import xml.sax
import urllib, base64
import time
import boto.utils
from boto.connection import AWSAuthConnection
from boto import handler
from boto.s3.bucket import Bucket
from boto.s3.key import Key
from boto.resultset import ResultSet
from boto.exception import BotoClientError

## connection.py
def get_all_buckets_hook(body):
    rs = ResultSet([('Bucket', self.bucket_class)])
    h = handler.XmlHandler(rs, self)
    xml.sax.parseString(body, h)
    return rs


## bucket.py
def _get_all(element_map, initial_query_string='',
             headers=None, **params):
    l = []
    for k, v in params.items():
        k = k.replace('_', '-')
        if  k == 'maxkeys':
            k = 'max-keys'
        if isinstance(v, unicode):
            v = v.encode('utf-8')
        if v is not None and v != '':
            l.append('%s=%s' % (urllib.quote(k), urllib.quote(str(v))))
    if len(l):
        s = initial_query_string + '&' + '&'.join(l)
    else:
        s = initial_query_string
    response = self.connection.make_request('GET', self.name,
                                            headers=headers,
                                            query_args=s)
    body = response.read()
    boto.log.debug(body)
    if response.status == 200:
        rs = ResultSet(element_map)
        h = handler.XmlHandler(rs, self)
        xml.sax.parseString(body, h)
        return rs
    else:
        raise self.connection.provider.storage_response_error(
            response.status, response.reason, body)

def get_all_hook(body):
    boto.log.debug(body)
    rs = ResultSet(element_map)
    h = handler.XmlHandler(rs, self)
    xml.sax.parseString(body, h)
    return rs


