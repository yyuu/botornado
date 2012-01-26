# Copyright (c) 2006-2010 Mitch Garnaat http://garnaat.org/
# Copyright (c) 2010 Google
# Copyright (c) 2008 rPath, Inc.
# Copyright (c) 2009 The Echo Nest Corporation
# Copyright (c) 2010, Eucalyptus Systems, Inc.
# Copyright (c) 2011, Nexenta Systems Inc.
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

#
# Parts of this code were copied or derived from sample code supplied by AWS.
# The following notice applies to that code.
#
#  This software code is made available "AS IS" without warranties of any
#  kind.  You may copy, display, modify and redistribute the software
#  code either by itself or as incorporated into your code; provided that
#  you do not remove any proprietary notices.  Your use of this software
#  code is at your own risk and you waive any claim against Amazon
#  Digital Services, Inc. or its affiliates with respect to your use of
#  this software code. (c) 2006 Amazon Digital Services, Inc. or its
#  affiliates.

"""
Handles basic connections to AWS
"""

from __future__ import with_statement
import base64
import errno
import httplib
import os
import Queue
import random
import re
import socket
import sys
import time
import urllib, urlparse
import xml.sax

import boto.auth
import boto.auth_handler
import boto
import boto.utils
import boto.handler
import boto.cacerts

from boto import config, UserAgent
from boto.exception import AWSConnectionError, BotoClientError, BotoServerError
from boto.provider import Provider
from boto.resultset import ResultSet

from boto.connection import *
import StringIO
import httplib
import mimetools
import tornado.httpclient
import tornado.httputil

class AsyncHTTPConnection(object):
    """
    a wrapper class to tornado.httpclient.AsyncHTTPClient
    """
    def __init__(self, host, port=None, strict=None, timeout=20.0, client=None):
        """
        """
        self.method = 'GET'
        self.host = host
        self.port = port
        self.path = '/'
        self.headers = []
        self.body = None
        self.timeout = timeout
        self.client = client if client else tornado.httpclient.AsyncHTTPClient()

    def __repr__(self):
        return '<AsyncHTTPConnection: %s>' % (repr(self.getrequest()))

    def request(self, method, path, body=None, headers=None):
        self.path = path
        self.method = method
        if body is not None:
            if hasattr(body,'read'): # file-like object
                self.body = body.read()
            else:
                self.body = body if body else None
        if headers is not None:
            self.headers += [ (k, headers[k]) for k in headers ]

    def getrequest(self, scheme='http'):
        url = urlparse.urlunsplit((scheme, self.host, self.path, '', ''))
        headers = tornado.httputil.HTTPHeaders()
        for (k,v) in self.headers:
            headers.add(k, v)
        request = tornado.httpclient.HTTPRequest(
            url, method=self.method, headers=headers, body=self.body,
            connect_timeout=self.timeout, request_timeout=self.timeout,
            validate_cert=False, # FIXME: disable validation since we could not validate S3 certs
        )
        return request

    def getresponse(self, callback=None):
        def fetched(tornado_response):
            if callable(callback):
                callback(AsyncHTTPResponse(tornado_response))
        self.client.fetch(self.getrequest(), callback=fetched)

    def set_debuglevel(self, level):
        pass

    def connect(self):
        pass

    def close(self):
        pass

    def putrequest(self, method, path, **kwargs):
        self.method = method
        self.path = path

    def putheader(self, header, argument):
    	  self.headers.append((header, argument))

    def endheaders(self):
        pass

    def send(self, data):
        self.body = data if data else None

class AsyncHTTPSConnection(AsyncHTTPConnection):
    def getrequest(self, scheme='https'):
        return AsyncHTTPConnection.getrequest(self, scheme=scheme)

class AsyncHTTPResponse(object):
    """
    a wrapper class to tornado.httpclient.HTTPResponse
    """
    def __init__(self, tornado_response):
        self.response = tornado_response
        self._msg = None
        self.version = 10
        self.status = self.response.code
        self.reason = self.response.error.message if self.response.error else ''

    def __repr__(self):
        return '<AsyncHTTPResponse: %s>' % (repr(self.response))

    def read(self, amt=None):
        return self.response.body

    def getheader(self, name, default=None):
        return self.response.headers.get(name, default)

    def getheaders(self):
        return map(lambda (k,v): (k,v), self.response.headers.get_all())

    def _get_msg(self):
        if self._msg is None:
            fp = StringIO.StringIO("\r\n".join(map(lambda (k,v): "%s: %s" % (k,v), self.response.headers.get_all())))
            self._msg = mimetools.Message(fp)
        return self._msg
    msg = property(_get_msg)

class AsyncConnection(object):
    def __init__(self, io_loop=None, max_clients=10):
        self._httpclient = tornado.httpclient.AsyncHTTPClient(io_loop=io_loop, max_clients=max_clients)

    def get_http_connection(self, host, is_secure):
        """
        Gets a connection from the pool for the named host.  Returns
        None if there is no connection that can be reused.
        """
        if is_secure:
            return AsyncHTTPSConnection(host, client=self._httpclient)
        else:
            return AsyncHTTPConnection(host, client=self._httpclient)

    def _mexe(self, request, sender=None, callback=None):
        boto.log.debug('Method: %s' % request.method)
        boto.log.debug('Path: %s' % request.path)
        boto.log.debug('Data: %s' % request.body)
        boto.log.debug('Headers: %s' % request.headers)
        boto.log.debug('Host: %s' % request.host)

        connection = self.get_http_connection(request.host, self.is_secure)
        request.authorize(connection=self)

        if callable(sender):
            sender(connection, request.method, request.path,
                   request.body, request.headers, callback)
        else:
            connection.request(request.method, request.path, request.body,
                               request.headers)
            connection.getresponse(callback=callback)

class AsyncAWSAuthConnection(AsyncConnection, boto.connection.AWSAuthConnection):
    def __init__(self, host, io_loop=None, max_clients=10, **kwargs):
        AsyncConnection.__init__(self, io_loop=io_loop, max_clients=max_clients)
        boto.connection.AWSAuthConnection.__init__(self, host, **kwargs)

    def make_request(self, method, path, headers=None, data='', host=None, auth_path=None, sender=None, callback=None, **kwargs):
        request = self.build_base_http_request(method, path, auth_path,
                                               {}, headers, data, host)
        self._mexe(request, sender=sender, callback=callback)

class AsyncAWSQueryConnection(AsyncConnection, boto.connection.AWSQueryConnection):
    def __init__(self, io_loop=None, max_clients=10, **kwargs):
        AsyncConnection.__init__(self, io_loop=io_loop, max_clients=max_clients)
        boto.connection.AWSQueryConnection.__init__(self, **kwargs)

    def make_request(self, action, params, path, verb, callback=None):
        request = self.build_base_http_request(verb, path, None,
                                               params, {}, '', self.server_name())
        if action:
            request.params['Action'] = action
        request.params['Version'] = self.APIVersion
        self._mexe(request, callback=callback)

    def get_list(self, action, params, markers, path='/',
                 parent=None, verb='GET', callback=None):
        if not parent:
            parent = self
        def list_got(response):
            body = response.read()
            boto.log.debug(body)
            if not body:
                boto.log.error('Null body %s' % body)
                raise self.ResponseError(response.status, response.reason, body)
            elif response.status == 200:
                rs = ResultSet(markers)
                h = boto.handler.XmlHandler(rs, parent)
                xml.sax.parseString(body, h)
                if callable(callback):
                    callback(rs)
            else:
                boto.log.error('%s %s' % (response.status, response.reason))
                boto.log.error('%s' % body)
                raise self.ResponseError(response.status, response.reason, body)
        self.make_request(action, params, path, verb, callback=list_got)

    def get_object(self, action, params, cls, path='/',
                   parent=None, verb='GET', callback=None):
        if not parent:
            parent = self
        def object_got(response):
            body = response.read()
            boto.log.debug(body)
            if not body:
                boto.log.error('Null body %s' % body)
                raise self.ResponseError(response.status, response.reason, body)
            elif response.status == 200:
                obj = cls(parent)
                h = boto.handler.XmlHandler(obj, parent)
                xml.sax.parseString(body, h)
                if callable(callback):
                    callback(obj)
            else:
                boto.log.error('%s %s' % (response.status, response.reason))
                boto.log.error('%s' % body)
                raise self.ResponseError(response.status, response.reason, body)
        self.make_request(action, params, path, verb, callback=object_got)

    def get_status(self, action, params, path='/', parent=None, verb='GET', callback=None):
        if not parent:
            parent = self
        def status_got(response):
            body = response.read()
            boto.log.debug(body)
            if not body:
                boto.log.error('Null body %s' % body)
                raise self.ResponseError(response.status, response.reason, body)
            elif response.status == 200:
                rs = ResultSet()
                h = boto.handler.XmlHandler(rs, parent)
                xml.sax.parseString(body, h)
                if callable(callback):
                    callback(rs)
            else:
                boto.log.error('%s %s' % (response.status, response.reason))
                boto.log.error('%s' % body)
                raise self.ResponseError(response.status, response.reason, body)
        self.make_request(action, params, path, verb, callback=status_got)
