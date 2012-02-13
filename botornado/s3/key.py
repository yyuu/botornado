#!/usr/bin/env python

# Copyright (c) 2006,2007 Mitch Garnaat http://garnaat.org/
# Copyright (c) 2011, Nexenta Systems Inc.
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

import mimetypes
import os
import re
import rfc822
import StringIO
import base64
import boto.utils
from boto.exception import BotoClientError
from boto.provider import Provider
from boto.s3.user import User
from boto import UserAgent
try:
    from hashlib import md5
except ImportError:
    from md5 import md5

from boto.s3.key import *

class AsyncKey(Key):
    def __init__(self, bucket=None, name=None):
        Key.__init__(self, bucket=bucket, name=name)

    def __repr__(self):
        if self.bucket:
            return '<AsyncKey: %s,%s>' % (self.bucket.name, self.name)
        else:
            return '<AsyncKey: None,%s>' % self.name

    def open_read(self, headers=None, query_args=None,
                  override_num_retries=None, response_headers=None, callback=None):
        """
        Open this key for reading

        :type headers: dict
        :param headers: Headers to pass in the web request

        :type query_args: string
        :param query_args: Arguments to pass in the query string (ie, 'torrent')

        :type override_num_retries: int
        :param override_num_retries: If not None will override configured
                                     num_retries parameter for underlying GET.

        :type response_headers: dict
        :param response_headers: A dictionary containing HTTP headers/values
                                 that will override any headers associated with
                                 the stored object in the response.
                                 See http://goo.gl/EWOPb for details.
        """
        if self.resp == None:
            self.mode = 'r'

            provider = self.bucket.connection.provider
            def opened_read(response):
                self.resp = response
                if self.resp.status < 199 or self.resp.status > 299:
                    body = self.resp.read()
                    raise provider.storage_response_error(self.resp.status,
                                                          self.resp.reason, body)
                response_headers = self.resp.msg
                self.metadata = boto.utils.get_aws_metadata(response_headers,
                                                            provider)
                for name,value in response_headers.items():
                    # To get correct size for Range GETs, use Content-Range
                    # header if one was returned. If not, use Content-Length
                    # header.
                    if (name.lower() == 'content-length' and
                        'Content-Range' not in response_headers):
                        self.size = int(value)
                    elif name.lower() == 'content-range':
                        end_range = re.sub('.*/(.*)', '\\1', value)
                        self.size = int(end_range)
                    elif name.lower() == 'etag':
                        self.etag = value
                    elif name.lower() == 'content-type':
                        self.content_type = value
                    elif name.lower() == 'content-encoding':
                        self.content_encoding = value
                    elif name.lower() == 'last-modified':
                        self.last_modified = value
                    elif name.lower() == 'cache-control':
                        self.cache_control = value
                self.handle_version_headers(self.resp)
                self.handle_encryption_headers(self.resp)
                if callable(callback):
                    callback(response)
            self.bucket.connection.make_request(
                'GET', self.bucket.name, self.name, headers,
                query_args=query_args,
                override_num_retries=override_num_retries, callback=opened_read)

    def open(self, mode='r', headers=None, query_args=None,
             override_num_retries=None, callback=None):
        if mode == 'r':
            self.mode = 'r'
            self.open_read(headers=headers, query_args=query_args,
                           override_num_retries=override_num_retries, callback=callback)
        elif mode == 'w':
            self.mode = 'w'
            self.open_write(headers=headers,
                            override_num_retries=override_num_retries, callback=callback)
        else:
            raise BotoClientError('Invalid mode: %s' % mode)

    def next(self):
        """
        By providing a next method, the key object supports use as an iterator.
        For example, you can now say:

        for bytes in key:
            write bytes to a file or whatever

        All of the HTTP connection stuff is handled for you.
        """
        raise BotoClientError('Not Implemented')

    def read(self, size=0, callback=None):
        def _read(response):
            if size == 0:
                data = self.resp.read()
            else:
                data = self.resp.read(size)
            if not data:
                self.close()
            if callable(callback):
                callback(data)
        self.open_read(callback=_read)

    def exists(self, callback=None):
        """
        Returns True if the key exists

        :rtype: bool
        :return: Whether the key exists on S3
        """
        def existence_tested(response):
            if callable(callback):
                callback(bool(response))
        self.bucket.lookup(self.name, callback=existence_tested)

    def delete(self, callback=None):
        """
        Delete this key from S3
        """
        return self.bucket.delete_key(self.name, version_id=self.version_id, callback=callback)

    def send_file(self, fp, headers=None, cb=None, num_cb=10,
                  query_args=None, chunked_transfer=False, callback=None):
        """
        Upload a file to a key into a bucket on S3.

        :type fp: file
        :param fp: The file pointer to upload

        :type headers: dict
        :param headers: The headers to pass along with the PUT request

        :type cb: function
        :param cb: a callback function that will be called to report
                   progress on the upload.  The callback should accept
                   two integer parameters, the first representing the
                   number of bytes that have been successfully
                   transmitted to S3 and the second representing the
                   size of the to be transmitted object.

        :type num_cb: int
        :param num_cb: (optional) If a callback is specified with the cb
                       parameter this parameter determines the granularity
                       of the callback by defining the maximum number of
                       times the callback will be called during the file
                       transfer. Providing a negative integer will cause
                       your callback to be called with each buffer read.

        """
        provider = self.bucket.connection.provider

        def sender(http_conn, method, path, data, headers, sendback=None):
            http_conn.putrequest(method, path)
            for key in headers:
                http_conn.putheader(key, headers[key])
            http_conn.endheaders()
            if chunked_transfer:
                # MD5 for the stream has to be calculated on the fly, as
                # we don't know the size of the stream before hand.
                m = md5()
            else:
                fp.seek(0)

            save_debug = self.bucket.connection.debug
            self.bucket.connection.debug = 0
            # If the debuglevel < 3 we don't want to show connection
            # payload, so turn off HTTP connection-level debug output (to
            # be restored below).
            # Use the getattr approach to allow this to work in AppEngine.
            if getattr(http_conn, 'debuglevel', 0) < 3:
                http_conn.set_debuglevel(0)
            if cb:
                if chunked_transfer:
                    # For chunked Transfer, we call the cb for every 1MB
                    # of data transferred.
                    cb_count = (1024 * 1024)/self.BufferSize
                    self.size = 0
                elif num_cb > 2:
                    cb_count = self.size / self.BufferSize / (num_cb-2)
                elif num_cb < 0:
                    cb_count = -1
                else:
                    cb_count = 0
                i = total_bytes = 0
                cb(total_bytes, self.size)
            l = fp.read(self.BufferSize)
            while len(l) > 0:
                if chunked_transfer:
                    http_conn.send('%x;\r\n' % len(l))
                    http_conn.send(l)
                    http_conn.send('\r\n')
                else:
                    http_conn.send(l)
                if cb:
                    total_bytes += len(l)
                    i += 1
                    if i == cb_count or cb_count == -1:
                        cb(total_bytes, self.size)
                        i = 0
                if chunked_transfer:
                    m.update(l)
                l = fp.read(self.BufferSize)
            if chunked_transfer:
                http_conn.send('0\r\n')
                http_conn.send('\r\n')
                if cb:
                    self.size = total_bytes
                # Get the md5 which is calculated on the fly.
                self.md5 = m.hexdigest()
            else:
                fp.seek(0)
            if cb:
                cb(total_bytes, self.size)
            def sender_sent(response):
                body = response.read()
                http_conn.set_debuglevel(save_debug)
                self.bucket.connection.debug = save_debug
                if ((response.status == 500 or response.status == 503 or
                        response.getheader('location')) and not chunked_transfer):
                    # we'll try again.
                    if callable(sendback):
                        sendback(response)
                elif response.status >= 200 and response.status <= 299:
                    self.etag = response.getheader('etag')
                    if self.etag != '"%s"'  % self.md5:
                        raise provider.storage_data_error(
                            'ETag from S3 did not match computed MD5')
                    if callable(sendback):
                        sendback(response)
                else:
                    raise provider.storage_response_error(
                        response.status, response.reason, body)
            http_conn.getresponse(callback=sender_sent)

        if not headers:
            headers = {}
        else:
            headers = headers.copy()
        headers['User-Agent'] = UserAgent
        if self.base64md5:
            headers['Content-MD5'] = self.base64md5
        if self.storage_class != 'STANDARD':
            headers[provider.storage_class_header] = self.storage_class
        if headers.has_key('Content-Encoding'):
            self.content_encoding = headers['Content-Encoding']
        if headers.has_key('Content-Type'):
            self.content_type = headers['Content-Type']
        elif self.path:
            self.content_type = mimetypes.guess_type(self.path)[0]
            if self.content_type == None:
                self.content_type = self.DefaultContentType
            headers['Content-Type'] = self.content_type
        else:
            headers['Content-Type'] = self.content_type
        if not chunked_transfer:
            headers['Content-Length'] = str(self.size)
#       headers['Expect'] = '100-Continue'
        headers = boto.utils.merge_meta(headers, self.metadata, provider)
        def file_sent(resp):
            self.handle_version_headers(resp, force=True)
            if callable(callback):
                callback(resp)
        self.bucket.connection.make_request('PUT', self.bucket.name,
                                            self.name, headers,
                                            sender=sender,
                                            query_args=query_args, callback=file_sent)

    def set_contents_from_stream(self, fp, headers=None, replace=True,
                                 cb=None, num_cb=10, policy=None,
                                 reduced_redundancy=False, query_args=None, callback=None):
        """
        Store an object using the name of the Key object as the key in
        cloud and the contents of the data stream pointed to by 'fp' as
        the contents.
        The stream object is not seekable and total size is not known.
        This has the implication that we can't specify the Content-Size and
        Content-MD5 in the header. So for huge uploads, the delay in calculating
        MD5 is avoided but with a penalty of inability to verify the integrity
        of the uploaded data.

        :type fp: file
        :param fp: the file whose contents are to be uploaded

        :type headers: dict
        :param headers: additional HTTP headers to be sent with the PUT request.

        :type replace: bool
        :param replace: If this parameter is False, the method will first check
            to see if an object exists in the bucket with the same key. If it
            does, it won't overwrite it. The default value is True which will
            overwrite the object.

        :type cb: function
        :param cb: a callback function that will be called to report
            progress on the upload. The callback should accept two integer
            parameters, the first representing the number of bytes that have
            been successfully transmitted to GS and the second representing the
            total number of bytes that need to be transmitted.

        :type num_cb: int
        :param num_cb: (optional) If a callback is specified with the cb
            parameter, this parameter determines the granularity of the callback
            by defining the maximum number of times the callback will be called
            during the file transfer.

        :type policy: :class:`boto.gs.acl.CannedACLStrings`
        :param policy: A canned ACL policy that will be applied to the new key
            in GS.

        :type reduced_redundancy: bool
        :param reduced_redundancy: If True, this will set the storage
                                   class of the new Key to be
                                   REDUCED_REDUNDANCY. The Reduced Redundancy
                                   Storage (RRS) feature of S3, provides lower
                                   redundancy at lower storage cost.
        """

        provider = self.bucket.connection.provider
        if not provider.supports_chunked_transfer():
            raise BotoClientError('%s does not support chunked transfer'
                % provider.get_provider_name())

        # Name of the Object should be specified explicitly for Streams.
        if not self.name or self.name == '':
            raise BotoClientError('Cannot determine the destination '
                                'object name for the given stream')

        if headers is None:
            headers = {}
        if policy:
            headers[provider.acl_header] = policy

        # Set the Transfer Encoding for Streams.
        headers['Transfer-Encoding'] = 'chunked'

        if reduced_redundancy:
            self.storage_class = 'REDUCED_REDUNDANCY'
            if provider.storage_class_header:
                headers[provider.storage_class_header] = self.storage_class

        if self.bucket != None:
            if not replace:
                def existence_tested(k):
                    if k:
                        if callable(callback):
                            callback(False)
                    else:
                        self.send_file(fp, headers, cb, num_cb, query_args,
                                                        chunked_transfer=True, callback=callback)
                self.bucket.lookup(self.name, callback=existence_tested)
                return
            self.send_file(fp, headers, cb, num_cb, query_args,
                                            chunked_transfer=True, callback=callback)

    def set_contents_from_file(self, fp, headers=None, replace=True,
                               cb=None, num_cb=10, policy=None, md5=None,
                               reduced_redundancy=False, query_args=None,
                               encrypt_key=False, callback=None):
        """
        Store an object in S3 using the name of the Key object as the
        key in S3 and the contents of the file pointed to by 'fp' as the
        contents.

        :type fp: file
        :param fp: the file whose contents to upload

        :type headers: dict
        :param headers: Additional HTTP headers that will be sent with
                        the PUT request.

        :type replace: bool
        :param replace: If this parameter is False, the method
                        will first check to see if an object exists in the
                        bucket with the same key.  If it does, it won't
                        overwrite it.  The default value is True which will
                        overwrite the object.

        :type cb: function
        :param cb: a callback function that will be called to report
                   progress on the upload.  The callback should accept
                   two integer parameters, the first representing the
                   number of bytes that have been successfully
                   transmitted to S3 and the second representing the
                   size of the to be transmitted object.

        :type cb: int
        :param num_cb: (optional) If a callback is specified with the cb
                       parameter this parameter determines the granularity
                       of the callback by defining the maximum number of
                       times the callback will be called during the
                       file transfer.

        :type policy: :class:`boto.s3.acl.CannedACLStrings`
        :param policy: A canned ACL policy that will be applied to the
                       new key in S3.

        :type md5: A tuple containing the hexdigest version of the MD5
                   checksum of the file as the first element and the
                   Base64-encoded version of the plain checksum as the
                   second element.  This is the same format returned by
                   the compute_md5 method.
        :param md5: If you need to compute the MD5 for any reason prior
                    to upload, it's silly to have to do it twice so this
                    param, if present, will be used as the MD5 values of
                    the file.  Otherwise, the checksum will be computed.

        :type reduced_redundancy: bool
        :param reduced_redundancy: If True, this will set the storage
                                   class of the new Key to be
                                   REDUCED_REDUNDANCY. The Reduced Redundancy
                                   Storage (RRS) feature of S3, provides lower
                                   redundancy at lower storage cost.

        :type encrypt_key: bool
        :param encrypt_key: If True, the new copy of the object will
                            be encrypted on the server-side by S3 and
                            will be stored in an encrypted form while
                            at rest in S3.
        """
        provider = self.bucket.connection.provider
        if headers is None:
            headers = {}
        if policy:
            headers[provider.acl_header] = policy
        if encrypt_key:
            headers[provider.server_side_encryption_header] = 'AES256'

        if reduced_redundancy:
            self.storage_class = 'REDUCED_REDUNDANCY'
            if provider.storage_class_header:
                headers[provider.storage_class_header] = self.storage_class
                # TODO - What if provider doesn't support reduced reduncancy?
                # What if different providers provide different classes?
        if hasattr(fp, 'name'):
            self.path = fp.name
        if self.bucket != None:
            if not md5:
                md5 = self.compute_md5(fp)
            else:
                # even if md5 is provided, still need to set size of content
                fp.seek(0, 2)
                self.size = fp.tell()
                fp.seek(0)
            self.md5 = md5[0]
            self.base64md5 = md5[1]
            if self.name == None:
                self.name = self.md5
            if not replace:
                def existence_tested(k):
                    if k:
                        if callable(callback):
                            callback(False)
                    else:
                        self.send_file(fp, headers, cb, num_cb, query_args, callback=callback)
                self.bucket.lookup(self.name, callback=existence_tested)
                return
            self.send_file(fp, headers, cb, num_cb, query_args, callback=callback)

    def set_contents_from_filename(self, filename, headers=None, replace=True,
                                   cb=None, num_cb=10, policy=None, md5=None,
                                   reduced_redundancy=False,
                                   encrypt_key=False):
        """
        Store an object in S3 using the name of the Key object as the
        key in S3 and the contents of the file named by 'filename'.
        See set_contents_from_file method for details about the
        parameters.

        :type filename: string
        :param filename: The name of the file that you want to put onto S3

        :type headers: dict
        :param headers: Additional headers to pass along with the
                        request to AWS.

        :type replace: bool
        :param replace: If True, replaces the contents of the file
                        if it already exists.

        :type cb: function
        :param cb: a callback function that will be called to report
                   progress on the upload.  The callback should accept
                   two integer parameters, the first representing the
                   number of bytes that have been successfully
                   transmitted to S3 and the second representing the
                   size of the to be transmitted object.

        :type cb: int
        :param num_cb: (optional) If a callback is specified with
                       the cb parameter this parameter determines the
                       granularity of the callback by defining
                       the maximum number of times the callback will
                       be called during the file transfer.

        :type policy: :class:`boto.s3.acl.CannedACLStrings`
        :param policy: A canned ACL policy that will be applied to the
                       new key in S3.

        :type md5: A tuple containing the hexdigest version of the MD5
                   checksum of the file as the first element and the
                   Base64-encoded version of the plain checksum as the
                   second element.  This is the same format returned by
                   the compute_md5 method.
        :param md5: If you need to compute the MD5 for any reason prior
                    to upload, it's silly to have to do it twice so this
                    param, if present, will be used as the MD5 values
                    of the file.  Otherwise, the checksum will be computed.

        :type reduced_redundancy: bool
        :param reduced_redundancy: If True, this will set the storage
                                   class of the new Key to be
                                   REDUCED_REDUNDANCY. The Reduced Redundancy
                                   Storage (RRS) feature of S3, provides lower
                                   redundancy at lower storage cost.
        :type encrypt_key: bool
        :param encrypt_key: If True, the new copy of the object will
                            be encrypted on the server-side by S3 and
                            will be stored in an encrypted form while
                            at rest in S3.
        """
        fp = open(filename, 'rb')
        def _set_contents_from_filename(response):
            fp.close()
            if callable(callback):
                callback(response)
        self.set_contents_from_file(fp, headers, replace, cb, num_cb,
                                    policy, md5, reduced_redundancy,
                                    encrypt_key=encrypt_key, callback=_set_contents_from_filename)

    def set_contents_from_string(self, s, headers=None, replace=True,
                                 cb=None, num_cb=10, policy=None, md5=None,
                                 reduced_redundancy=False,
                                 encrypt_key=False, callback=None):
        """
        Store an object in S3 using the name of the Key object as the
        key in S3 and the string 's' as the contents.
        See set_contents_from_file method for details about the
        parameters.

        :type headers: dict
        :param headers: Additional headers to pass along with the
                        request to AWS.

        :type replace: bool
        :param replace: If True, replaces the contents of the file if
                        it already exists.

        :type cb: function
        :param cb: a callback function that will be called to report
                   progress on the upload.  The callback should accept
                   two integer parameters, the first representing the
                   number of bytes that have been successfully
                   transmitted to S3 and the second representing the
                   size of the to be transmitted object.

        :type cb: int
        :param num_cb: (optional) If a callback is specified with
                       the cb parameter this parameter determines the
                       granularity of the callback by defining
                       the maximum number of times the callback will
                       be called during the file transfer.

        :type policy: :class:`boto.s3.acl.CannedACLStrings`
        :param policy: A canned ACL policy that will be applied to the
                       new key in S3.

        :type md5: A tuple containing the hexdigest version of the MD5
                   checksum of the file as the first element and the
                   Base64-encoded version of the plain checksum as the
                   second element.  This is the same format returned by
                   the compute_md5 method.
        :param md5: If you need to compute the MD5 for any reason prior
                    to upload, it's silly to have to do it twice so this
                    param, if present, will be used as the MD5 values
                    of the file.  Otherwise, the checksum will be computed.

        :type reduced_redundancy: bool
        :param reduced_redundancy: If True, this will set the storage
                                   class of the new Key to be
                                   REDUCED_REDUNDANCY. The Reduced Redundancy
                                   Storage (RRS) feature of S3, provides lower
                                   redundancy at lower storage cost.
        :type encrypt_key: bool
        :param encrypt_key: If True, the new copy of the object will
                            be encrypted on the server-side by S3 and
                            will be stored in an encrypted form while
                            at rest in S3.
        """
        if isinstance(s, unicode):
            s = s.encode("utf-8")
        fp = StringIO.StringIO(s)
        def _set_contents_from_string(response):
            fp.close()
            if callable(callback):
                callback(response)
        self.set_contents_from_file(fp, headers, replace, cb, num_cb,
                                    policy, md5, reduced_redundancy,
                                    encrypt_key=encrypt_key, callback=_set_contents_from_string)

    def get_file(self, fp, headers=None, cb=None, num_cb=10,
                 torrent=False, version_id=None, override_num_retries=None,
                 response_headers=None, callback=None):
        """
        Retrieves a file from an S3 Key

        :type fp: file
        :param fp: File pointer to put the data into

        :type headers: string
        :param: headers to send when retrieving the files

        :type cb: function
        :param cb: a callback function that will be called to report
                   progress on the upload.  The callback should accept
                   two integer parameters, the first representing the
                   number of bytes that have been successfully
                   transmitted to S3 and the second representing the
                   size of the to be transmitted object.

        :type cb: int
        :param num_cb: (optional) If a callback is specified with
                       the cb parameter this parameter determines the
                       granularity of the callback by defining
                       the maximum number of times the callback will
                       be called during the file transfer.

        :type torrent: bool
        :param torrent: Flag for whether to get a torrent for the file

        :type override_num_retries: int
        :param override_num_retries: If not None will override configured
                                     num_retries parameter for underlying GET.

        :type response_headers: dict
        :param response_headers: A dictionary containing HTTP headers/values
                                 that will override any headers associated with
                                 the stored object in the response.
                                 See http://goo.gl/EWOPb for details.
        """
        if cb:
            if num_cb > 2:
                cb_count = self.size / self.BufferSize / (num_cb-2)
            elif num_cb < 0:
                cb_count = -1
            else:
                cb_count = 0
            i = total_bytes = 0
            cb(total_bytes, self.size)
        save_debug = self.bucket.connection.debug
        if self.bucket.connection.debug == 1:
            self.bucket.connection.debug = 0

        query_args = []
        if torrent:
            query_args.append('torrent')
        # If a version_id is passed in, use that.  If not, check to see
        # if the Key object has an explicit version_id and, if so, use that.
        # Otherwise, don't pass a version_id query param.
        if version_id is None:
            version_id = self.version_id
        if version_id:
            query_args.append('versionId=%s' % version_id)
        if response_headers:
            for key in response_headers:
                query_args.append('%s=%s' % (key, response_headers[key]))
        query_args = '&'.join(query_args)
        def file_got(response):
            body = self.resp.read()
            fp.write(body)
            if cb:
                cb(total_bytes, self.size)
            self.close()
            self.bucket.connection.debug = save_debug
            if callable(callback):
                callback(response)
        self.open('r', headers, query_args=query_args,
                  override_num_retries=override_num_retries, callback=file_got)

    def get_contents_to_file(self, fp, headers=None,
                             cb=None, num_cb=10,
                             torrent=False,
                             version_id=None,
                             res_download_handler=None,
                             response_headers=None, callback=None):
        """
        Retrieve an object from S3 using the name of the Key object as the
        key in S3.  Write the contents of the object to the file pointed
        to by 'fp'.

        :type fp: File -like object
        :param fp:

        :type headers: dict
        :param headers: additional HTTP headers that will be sent with
                        the GET request.

        :type cb: function
        :param cb: a callback function that will be called to report
                   progress on the upload.  The callback should accept
                   two integer parameters, the first representing the
                   number of bytes that have been successfully
                   transmitted to S3 and the second representing the
                   size of the to be transmitted object.

        :type cb: int
        :param num_cb: (optional) If a callback is specified with
                       the cb parameter this parameter determines the
                       granularity of the callback by defining
                       the maximum number of times the callback will
                       be called during the file transfer.

        :type torrent: bool
        :param torrent: If True, returns the contents of a torrent
                        file as a string.

        :type res_upload_handler: ResumableDownloadHandler
        :param res_download_handler: If provided, this handler will
                                     perform the download.

        :type response_headers: dict
        :param response_headers: A dictionary containing HTTP headers/values
                                 that will override any headers associated with
                                 the stored object in the response.
                                 See http://goo.gl/EWOPb for details.
        """
        if self.bucket != None:
            if res_download_handler:
                res_download_handler.get_file(self, fp, headers, cb, num_cb,
                                              torrent=torrent,
                                              version_id=version_id, callback=callback)
            else:
                self.get_file(fp, headers, cb, num_cb, torrent=torrent,
                              version_id=version_id,
                              response_headers=response_headers, callback=callback)

    def get_contents_to_filename(self, filename, headers=None,
                                 cb=None, num_cb=10,
                                 torrent=False,
                                 version_id=None,
                                 res_download_handler=None,
                                 response_headers=None, callback=None):
        """
        Retrieve an object from S3 using the name of the Key object as the
        key in S3.  Store contents of the object to a file named by 'filename'.
        See get_contents_to_file method for details about the
        parameters.

        :type filename: string
        :param filename: The filename of where to put the file contents

        :type headers: dict
        :param headers: Any additional headers to send in the request

        :type cb: function
        :param cb: a callback function that will be called to report
                   progress on the upload.  The callback should accept
                   two integer parameters, the first representing the
                   number of bytes that have been successfully
                   transmitted to S3 and the second representing the
                   size of the to be transmitted object.

        :type cb: int
        :param num_cb: (optional) If a callback is specified with
                       the cb parameter this parameter determines the
                       granularity of the callback by defining
                       the maximum number of times the callback will
                       be called during the file transfer.

        :type torrent: bool
        :param torrent: If True, returns the contents of a torrent file
                        as a string.

        :type res_upload_handler: ResumableDownloadHandler
        :param res_download_handler: If provided, this handler will
                                     perform the download.

        :type response_headers: dict
        :param response_headers: A dictionary containing HTTP headers/values
                                 that will override any headers associated with
                                 the stored object in the response.
                                 See http://goo.gl/EWOPb for details.
        """
        fp = open(filename, 'wb')
        def got_contents_to_filename(response):
            fp.close()
            # if last_modified date was sent from s3, try to set file's timestamp
            if self.last_modified != None:
                try:
                    modified_tuple = rfc822.parsedate_tz(self.last_modified)
                    modified_stamp = int(rfc822.mktime_tz(modified_tuple))
                    os.utime(fp.name, (modified_stamp, modified_stamp))
                except Exception: pass
            if callable(callback):
                callback(response)
        self.get_contents_to_file(fp, headers, cb, num_cb, torrent=torrent,
                                  version_id=version_id,
                                  res_download_handler=res_download_handler,
                                  response_headers=response_headers, callback=got_contents_to_filename)

    def get_contents_as_string(self, headers=None,
                               cb=None, num_cb=10,
                               torrent=False,
                               version_id=None,
                               response_headers=None, callback=None):
        """
        Retrieve an object from S3 using the name of the Key object as the
        key in S3.  Return the contents of the object as a string.
        See get_contents_to_file method for details about the
        parameters.

        :type headers: dict
        :param headers: Any additional headers to send in the request

        :type cb: function
        :param cb: a callback function that will be called to report
                   progress on the upload.  The callback should accept
                   two integer parameters, the first representing the
                   number of bytes that have been successfully
                   transmitted to S3 and the second representing the
                   size of the to be transmitted object.

        :type cb: int
        :param num_cb: (optional) If a callback is specified with
                       the cb parameter this parameter determines the
                       granularity of the callback by defining
                       the maximum number of times the callback will
                       be called during the file transfer.

        :type torrent: bool
        :param torrent: If True, returns the contents of a torrent file
                        as a string.

        :type response_headers: dict
        :param response_headers: A dictionary containing HTTP headers/values
                                 that will override any headers associated with
                                 the stored object in the response.
                                 See http://goo.gl/EWOPb for details.

        :rtype: string
        :returns: The contents of the file as a string
        """
        fp = StringIO.StringIO()
        def got_contents_as_string(response):
            if callable(callback):
                callback(fp.getvalue())
        self.get_contents_to_file(fp, headers, cb, num_cb, torrent=torrent,
                                  version_id=version_id,
                                  response_headers=response_headers, callback=got_contents_as_string)

# vim:set ft=python sw=4 :
