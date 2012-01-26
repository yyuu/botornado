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

from boto.s3.connection import *
import botornado.connection
import botornado.s3.bucket
import botornado.s3.key

class AsyncS3Connection(botornado.connection.AsyncAWSAuthConnection, boto.s3.connection.S3Connection):
    def __init__(self, host=boto.s3.connection.S3Connection.DefaultHost,
                       calling_format=boto.s3.connection.SubdomainCallingFormat(),
                       bucket_class=botornado.s3.bucket.AsyncBucket, **kwargs):
        self.calling_format = calling_format
        self.bucket_class = bucket_class
        botornado.connection.AsyncAWSAuthConnection.__init__(self, host, **kwargs)

    def get_all_buckets(self, headers=None, callback=None):
        def got_all_buckets(response):
            body = response.read()
            if response.status > 300:
                raise self.provider.storage_response_error(
                    response.status, response.reason, body)
            rs = ResultSet([('Bucket', self.bucket_class)])
            h = handler.XmlHandler(rs, self)
            xml.sax.parseString(body, h)
            if callable(callback):
                callback(rs)
        self.make_request('GET', headers=headers, callback=got_all_buckets)

    def get_canonical_user_id(self, headers=None, callback=None):
        """
        Convenience method that returns the "CanonicalUserID" of the
        user who's credentials are associated with the connection.
        The only way to get this value is to do a GET request on the
        service which returns all buckets associated with the account.
        As part of that response, the canonical userid is returned.
        This method simply does all of that and then returns just the
        user id.

        :rtype: string
        :return: A string containing the canonical user id.
        """
        def got_canonical_user_id(rs):
            if callable(callback):
                callback(rs.ID)
        self.get_all_buckets(headers=headers, callback=got_canonical_user_id)


    def get_bucket(self, bucket_name, validate=True, headers=None, callback=None):
        bucket = self.bucket_class(connection=self, name=bucket_name)
        if validate:
            def got_bucket(response):
                if callable(callback):
                    callback(bucket)
            bucket.get_all_keys(headers, maxkeys=0, callback=got_bucket)
        else:
            if callable(callback):
                callback(bucket)

    def lookup(self, bucket_name, validate=True, headers=None, callback=None):
        def lookedup(bucket):
            if callable(callback):
                callback(bucket)
        self.get_bucket(bucket_name, validate, headers=headers, callabck=lookedup)

    def create_bucket(self, bucket_name, headers=None,
                      location=Location.DEFAULT, policy=None, callback=None):
        """
        Creates a new located bucket. By default it's in the USA. You can pass
        Location.EU to create an European bucket.

        :type bucket_name: string
        :param bucket_name: The name of the new bucket
        
        :type headers: dict
        :param headers: Additional headers to pass along with the request to AWS.

        :type location: :class:`boto.s3.connection.Location`
        :param location: The location of the new bucket
        
        :type policy: :class:`boto.s3.acl.CannedACLStrings`
        :param policy: A canned ACL policy that will be applied to the new key in S3.
             
        """
        check_lowercase_bucketname(bucket_name)

        if policy:
            if headers:
                headers[self.provider.acl_header] = policy
            else:
                headers = {self.provider.acl_header : policy}
        if location == Location.DEFAULT:
            data = ''
        else:
            data = '<CreateBucketConstraint><LocationConstraint>' + \
                    location + '</LocationConstraint></CreateBucketConstraint>'
        def bucket_created(response):
            body = response.read()
            if response.status == 409:
                raise self.provider.storage_create_error(
                    response.status, response.reason, body)
            if response.status == 200:
                if callable(callback):
                    callback(self.bucket_class(self, bucket_name))
            else:
                raise self.provider.storage_response_error(
                    response.status, response.reason, body)

        self.make_request('PUT', bucket_name, headers=headers,
                data=data, callback=bucket_created)

    def delete_bucket(self, bucket, headers=None, callback=None):
        def bucket_deleted(response):
            body = response.read()
            if response.status != 204:
                raise self.provider.storage_response_error(
                    response.status, response.reason, body)
            if callable(callback):
                callback(True)
        self.make_request('DELETE', bucket, headers=headers, callback=bucket_deleted)

    def make_request(self, method, bucket='', key='', headers=None, data='',
            query_args=None, sender=None, callback=None, **kwargs):
        if isinstance(bucket, self.bucket_class):
            bucket = bucket.name
        if isinstance(key, Key):
            key = key.name
        path = self.calling_format.build_path_base(bucket, key)
        boto.log.debug('path=%s' % path)
        auth_path = self.calling_format.build_auth_path(bucket, key)
        boto.log.debug('auth_path=%s' % auth_path)
        host = self.calling_format.build_host(self.server_name(), bucket)
        if query_args:
            path += '?' + query_args
            boto.log.debug('path=%s' % path)
            auth_path += '?' + query_args
            boto.log.debug('auth_path=%s' % auth_path)
        return botornado.connection.AsyncAWSAuthConnection.make_request(self, method, path, headers,
                data, host, auth_path, sender,
                callback=callback, **kwargs)

