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

import boto
from boto import handler
from boto.resultset import ResultSet
from boto.s3.acl import Policy, CannedACLStrings, Grant
from boto.s3.key import Key
from boto.s3.prefix import Prefix
from boto.s3.deletemarker import DeleteMarker
from boto.s3.multipart import MultiPartUpload
from boto.s3.multipart import CompleteMultiPartUpload
from boto.s3.bucketlistresultset import BucketListResultSet
from boto.s3.bucketlistresultset import VersionedBucketListResultSet
from boto.s3.bucketlistresultset import MultiPartUploadListResultSet
import boto.jsonresponse
import boto.utils
import xml.sax
import urllib
import re
from collections import defaultdict

from boto.s3.bucket import *
import botornado.s3.key

class AsyncBucket(Bucket):
    def __init__(self, connection=None, name=None, key_class=botornado.s3.key.AsyncKey, **kwargs):
        Bucket.__init__(self, connection=connection, name=name, key_class=key_class, **kwargs)

    def __repr__(self):
        return '<AsyncBucket: %s>' % self.name

    def lookup(self, key_name, headers=None, callback=None):
        """
        Deprecated: Please use get_key method.
        
        :type key_name: string
        :param key_name: The name of the key to retrieve
        
        :rtype: :class:`boto.s3.key.Key`
        :returns: A Key object from this bucket.
        """
        return self.get_key(key_name, headers=headers, callback=callback)
 
    def get_key(self, key_name, headers=None, version_id=None, callback=None):
        if version_id:
            query_args = 'versionId=%s' % version_id
        else:
            query_args = None
        def got_key(response):
            # Allow any success status (2xx) - for example this lets us
            # support Range gets, which return status 206:
            if response.status/100 == 2:
#               response.body
                k = self.key_class(self)
                provider = self.connection.provider
                k.metadata = boto.utils.get_aws_metadata(response.msg, provider)
                k.etag = response.getheader('etag')
                k.content_type = response.getheader('content-type')
                k.content_encoding = response.getheader('content-encoding')
                k.last_modified = response.getheader('last-modified')
                # the following machinations are a workaround to the fact that
                # apache/fastcgi omits the content-length header on HEAD
                # requests when the content-length is zero.
                # See http://goo.gl/0Tdax for more details.
                clen = response.getheader('content-length')
                if clen:
                    k.size = int(response.getheader('content-length'))
                else:
                    k.size = 0
                k.cache_control = response.getheader('cache-control')
                k.name = key_name
                k.handle_version_headers(response)
                k.handle_encryption_headers(response)
                if callable(callback):
                    callback(k)
            else:
                if response.status == 404:
#                   response.read()
                    if callable(callback):
                        callback(None)
                else:
                    raise self.connection.provider.storage_response_error(
                        response.status, response.reason, '')

        response = self.connection.make_request('HEAD', self.name, key_name,
                                                headers=headers,
                                                query_args=query_args, callback=got_key)

    def _get_all(self, element_map, initial_query_string='',
                 headers=None, callback=None, **params):
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
        def _got_all(response):
            body = response.read()
            boto.log.debug(body)
            if response.status == 200:
                rs = boto.resultset.ResultSet(element_map)
                h = boto.handler.XmlHandler(rs, self)
                xml.sax.parseString(body, h)
                if callable(callback):
                    callback(rs)
            else:
                raise self.connection.provider.storage_response(
                    response.status, response.reason, body)

        self.connection.make_request('GET', self.name,
                                     headers=headers,
                                     query_args=s, callback=_got_all)

    def get_all_keys(self, headers=None, callback=None, **params):
        """
        A lower-level method for listing contents of a bucket.
        This closely models the actual S3 API and requires you to manually
        handle the paging of results.  For a higher-level method
        that handles the details of paging for you, you can use the list method.
        
        :type max_keys: int
        :param max_keys: The maximum number of keys to retrieve
        
        :type prefix: string
        :param prefix: The prefix of the keys you want to retrieve
        
        :type marker: string
        :param marker: The "marker" of where you are in the result set
        
        :type delimiter: string 
        :param delimiter: If this optional, Unicode string parameter
                          is included with your request, then keys that
                          contain the same string between the prefix and
                          the first occurrence of the delimiter will be
                          rolled up into a single result element in the
                          CommonPrefixes collection. These rolled-up keys
                          are not returned elsewhere in the response.

        :rtype: ResultSet
        :return: The result from S3 listing the keys requested
        
        """
        return self._get_all([('Contents', self.key_class),
                              ('CommonPrefixes', boto.s3.prefix.Prefix)],
                              '', headers, callback=callback, **params)

    def delete_key(self, key_name, headers=None,
                   version_id=None, mfa_token=None, callback=None):
        """
        Deletes a key from the bucket.  If a version_id is provided,
        only that version of the key will be deleted.
        
        :type key_name: string
        :param key_name: The key name to delete

        :type version_id: string
        :param version_id: The version ID (optional)
        
        :type mfa_token: tuple or list of strings
        :param mfa_token: A tuple or list consisting of the serial number
                          from the MFA device and the current value of
                          the six-digit token associated with the device.
                          This value is required anytime you are
                          deleting versioned objects from a bucket
                          that has the MFADelete option on the bucket.
        """
        provider = self.connection.provider
        if version_id:
            query_args = 'versionId=%s' % version_id
        else:
            query_args = None
        if mfa_token:
            if not headers:
                headers = {}
            headers[provider.mfa_header] = ' '.join(mfa_token)
        def key_deleted(response):
            body = response.read()
            if response.status != 204:
                raise provider.storage_response_error(response.status,
                                                      response.reason, body)
            if callable(callback):
                callback(True)
        self.connection.make_request('DELETE', self.name, key_name,
                                     headers=headers,
                                     query_args=query_args, callback=key_deleted)
        
    def delete(self, headers=None, callback=None):
        return self.connection.delete_bucket(self.name, headers=headers, callback=callback)
