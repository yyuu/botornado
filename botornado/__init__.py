# Copyright (c) 2006-2011 Mitch Garnaat http://garnaat.org/
# Copyright (c) 2010-2011, Eucalyptus Systems, Inc.
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
from boto.pyami.config import Config, BotoConfigLocations
from boto.storage_uri import BucketStorageUri, FileStorageUri
import boto.plugin
import os, re, sys
import logging
import logging.config
from boto.exception import InvalidUriError

from boto import *

def connect_sqs(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.sqs.connection.SQSConnection`
    :return: A connection to Amazon's SQS
    """
    from botornado.sqs.connection import AsyncSQSConnection
    return AsyncSQSConnection(aws_access_key_id, aws_secret_access_key, **kwargs)

def connect_s3(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.s3.connection.S3Connection`
    :return: A connection to Amazon's S3
    """
    from botornado.s3.connection import AsyncS3Connection
    return AsyncS3Connection(aws_access_key_id, aws_secret_access_key, **kwargs)

def connect_gs(gs_access_key_id=None, gs_secret_access_key=None, **kwargs):
    """
    @type gs_access_key_id: string
    @param gs_access_key_id: Your Google Cloud Storage Access Key ID

    @type gs_secret_access_key: string
    @param gs_secret_access_key: Your Google Cloud Storage Secret Access Key

    @rtype: L{GSConnection<boto.gs.connection.GSConnection>}
    @return: A connection to Google's Storage service
    """
    raise BotoClientError('Not Implemented')

def connect_ec2(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.ec2.connection.EC2Connection`
    :return: A connection to Amazon's EC2
    """
    raise BotoClientError('Not Implemented')

def connect_elb(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.ec2.elb.ELBConnection`
    :return: A connection to Amazon's Load Balancing Service
    """
    raise BotoClientError('Not Implemented')

def connect_autoscale(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.ec2.autoscale.AutoScaleConnection`
    :return: A connection to Amazon's Auto Scaling Service
    """
    raise BotoClientError('Not Implemented')

def connect_cloudwatch(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.ec2.cloudwatch.CloudWatchConnection`
    :return: A connection to Amazon's EC2 Monitoring service
    """
    raise BotoClientError('Not Implemented')

def connect_sdb(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.sdb.connection.SDBConnection`
    :return: A connection to Amazon's SDB
    """
    raise BotoClientError('Not Implemented')

def connect_fps(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.fps.connection.FPSConnection`
    :return: A connection to FPS
    """
    raise BotoClientError('Not Implemented')

def connect_mturk(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.mturk.connection.MTurkConnection`
    :return: A connection to MTurk
    """
    raise BotoClientError('Not Implemented')

def connect_cloudfront(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.fps.connection.FPSConnection`
    :return: A connection to FPS
    """
    raise BotoClientError('Not Implemented')

def connect_vpc(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.vpc.VPCConnection`
    :return: A connection to VPC
    """
    raise BotoClientError('Not Implemented')

def connect_rds(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.rds.RDSConnection`
    :return: A connection to RDS
    """
    raise BotoClientError('Not Implemented')

def connect_emr(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.emr.EmrConnection`
    :return: A connection to Elastic mapreduce
    """
    raise BotoClientError('Not Implemented')

def connect_sns(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.sns.SNSConnection`
    :return: A connection to Amazon's SNS
    """
    raise BotoClientError('Not Implemented')


def connect_iam(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.iam.IAMConnection`
    :return: A connection to Amazon's IAM
    """
    raise BotoClientError('Not Implemented')

def connect_route53(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.dns.Route53Connection`
    :return: A connection to Amazon's Route53 DNS Service
    """
    raise BotoClientError('Not Implemented')

def connect_euca(host=None, aws_access_key_id=None, aws_secret_access_key=None,
                 port=8773, path='/services/Eucalyptus', is_secure=False,
                 **kwargs):
    """
    Connect to a Eucalyptus service.

    :type host: string
    :param host: the host name or ip address of the Eucalyptus server

    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.ec2.connection.EC2Connection`
    :return: A connection to Eucalyptus server
    """
    raise BotoClientError('Not Implemented')

def connect_walrus(host=None, aws_access_key_id=None, aws_secret_access_key=None,
                   port=8773, path='/services/Walrus', is_secure=False,
                   **kwargs):
    """
    Connect to a Walrus service.

    :type host: string
    :param host: the host name or ip address of the Walrus server

    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.s3.connection.S3Connection`
    :return: A connection to Walrus
    """
    raise BotoClientError('Not Implemented')

def connect_ses(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.ses.SESConnection`
    :return: A connection to Amazon's SES
    """
    raise BotoClientError('Not Implemented')

def connect_sts(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.sts.STSConnection`
    :return: A connection to Amazon's STS
    """
    raise BotoClientError('Not Implemented')

def connect_ia(ia_access_key_id=None, ia_secret_access_key=None,
               is_secure=False, **kwargs):
    """
    Connect to the Internet Archive via their S3-like API.

    :type ia_access_key_id: string
    :param ia_access_key_id: Your IA Access Key ID.  This will also look in your
                             boto config file for an entry in the Credentials
                             section called "ia_access_key_id"

    :type ia_secret_access_key: string
    :param ia_secret_access_key: Your IA Secret Access Key.  This will also
                                 look in your boto config file for an entry
                                 in the Credentials section called
                                 "ia_secret_access_key"

    :rtype: :class:`boto.s3.connection.S3Connection`
    :return: A connection to the Internet Archive
    """
    raise BotoClientError('Not Implemented')
