# Copyright (c) 2006-2009 Mitch Garnaat http://garnaat.org/
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

"""
Represents an SQS Queue
"""

import urlparse
from boto.sqs.message import Message

from boto.sqs.queue import *

class AsyncQueue(Queue):

    def __init__(self, connection=None, url=None, message_class=Message):
        self.connection = connection
        self.url = url
        self.message_class = message_class
        self.visibility_timeout = None

    def get_attributes(self, attributes='All', callback=None):
        """
        Retrieves attributes about this queue object and returns
        them in an Attribute instance (subclass of a Dictionary).

        :type attributes: string
        :param attributes: String containing one of:
                           ApproximateNumberOfMessages,
                           ApproximateNumberOfMessagesNotVisible,
                           VisibilityTimeout,
                           CreatedTimestamp,
                           LastModifiedTimestamp,
                           Policy
        :rtype: Attribute object
        :return: An Attribute object which is a mapping type holding the
                 requested name/value pairs
        """
        return self.connection.get_queue_attributes(self, attributes, callback=callback)

    def set_attribute(self, attribute, value, callback=None):
        """
        Set a new value for an attribute of the Queue.
        
        :type attribute: String
        :param attribute: The name of the attribute you want to set.  The
                           only valid value at this time is: VisibilityTimeout
        :type value: int
        :param value: The new value for the attribute.
                      For VisibilityTimeout the value must be an
                      integer number of seconds from 0 to 86400.

        :rtype: bool
        :return: True if successful, otherwise False.
        """
        return self.connection.set_queue_attribute(self, attribute, value, callback=callback)

    def get_timeout(self, callback=None):
        """
        Get the visibility timeout for the queue.
        
        :rtype: int
        :return: The number of seconds as an integer.
        """
        def got_timeout(a):
            if callable(callback):
                callback(int(a['VisibilityTimeout']))
        self.get_attributes('VisibilityTimeout', callback=got_timeout)

    def set_timeout(self, visibility_timeout, callback=None):
        """
        Set the visibility timeout for the queue.

        :type visibility_timeout: int
        :param visibility_timeout: The desired timeout in seconds
        """
        def _set_timeout(retval):
            if retval:
              self.visibility_timeout = visibility_timeout
            if callable(callback):
                callback(retval)
        retval = self.set_attribute('VisibilityTimeout', visibility_timeout, callback=_set_timeout)

    def add_permission(self, label, aws_account_id, action_name, callback=None):
        """
        Add a permission to a queue.

        :type label: str or unicode
        :param label: A unique identification of the permission you are setting.
                      Maximum of 80 characters ``[0-9a-zA-Z_-]``
                      Example, AliceSendMessage

        :type aws_account_id: str or unicode
        :param principal_id: The AWS account number of the principal who will be given
                             permission.  The principal must have an AWS account, but
                             does not need to be signed up for Amazon SQS. For information
                             about locating the AWS account identification.

        :type action_name: str or unicode
        :param action_name: The action.  Valid choices are:
                            \*|SendMessage|ReceiveMessage|DeleteMessage|
                            ChangeMessageVisibility|GetQueueAttributes

        :rtype: bool
        :return: True if successful, False otherwise.

        """
        return self.connection.add_permission(self, label, aws_account_id, action_name, callback=callback)

    def remove_permission(self, label, callback=None):
        """
        Remove a permission from a queue.

        :type label: str or unicode
        :param label: The unique label associated with the permission being removed.

        :rtype: bool
        :return: True if successful, False otherwise.
        """
        return self.connection.remove_permission(self, label, callback=callback)

    def read(self, visibility_timeout=None, callback=None):
        """
        Read a single message from the queue.
        
        :type visibility_timeout: int
        :param visibility_timeout: The timeout for this message in seconds

        :rtype: :class:`boto.sqs.message.Message`
        :return: A single message or None if queue is empty
        """
        def _read(rs):
            if callable(callback):
                callback(rs[0] if len(rs) == 1 else None)
        self.get_messages(1, visibility_timeout, callback=callback)

    def write(self, message, callback=None):
        """
        Add a single message to the queue.

        :type message: Message
        :param message: The message to be written to the queue

        :rtype: :class:`boto.sqs.message.Message`
        :return: The :class:`boto.sqs.message.Message` object that was written.
        """
        def wrote(new_msg):
            message.id = new_msg.id
            message.md5 = new_msg.md5
            if callable(callback):
                callback(message)
        self.connection.send_message(self, message.get_body_encoded(), callback=wrote)

    # get a variable number of messages, returns a list of messages
    def get_messages(self, num_messages=1, visibility_timeout=None,
                     attributes=None, callback=None):
        """
        Get a variable number of messages.

        :type num_messages: int
        :param num_messages: The maximum number of messages to read from the queue.
        
        :type visibility_timeout: int
        :param visibility_timeout: The VisibilityTimeout for the messages read.

        :type attributes: str
        :param attributes: The name of additional attribute to return with response
                           or All if you want all attributes.  The default is to
                           return no additional attributes.  Valid values:
                           All
                           SenderId
                           SentTimestamp
                           ApproximateReceiveCount
                           ApproximateFirstReceiveTimestamp
                           
        :rtype: list
        :return: A list of :class:`boto.sqs.message.Message` objects.
        """
        return self.connection.receive_message(self, number_messages=num_messages,
                                               visibility_timeout=visibility_timeout,
                                               attributes=attributes, callback=callback)

    def delete_message(self, message, callback=None):
        """
        Delete a message from the queue.

        :type message: :class:`boto.sqs.message.Message`
        :param message: The :class:`boto.sqs.message.Message` object to delete.

        :rtype: bool
        :return: True if successful, False otherwise
        """
        return self.connection.delete_message(self, message, callback=callback)

    def delete(self, callback=None):
        """
        Delete the queue.
        """
        return self.connection.delete_queue(self, callback=callback)

    def clear(self, page_size=10, vtimeout=10, callback=None):
        """Utility function to remove all messages from a queue"""
        raise BotoClientError('Not Implemented')

    def count(self, page_size=10, vtimeout=10, callback=None):
        """
        Utility function to count the number of messages in a queue.
        Note: This function now calls GetQueueAttributes to obtain
        an 'approximate' count of the number of messages in a queue.
        """
        def counted(a):
            if callable(callback):
                callback(int(a['ApproximateNumberOfMessages']))
        self.get_attributes('ApproximateNumberOfMessages', callback=counted)

    def count_slow(self, page_size=10, vtimeout=10, callback=None):
        """
        Deprecated.  This is the old 'count' method that actually counts
        the messages by reading them all.  This gives an accurate count but
        is very slow for queues with non-trivial number of messasges.
        Instead, use get_attribute('ApproximateNumberOfMessages') to take
        advantage of the new SQS capability.  This is retained only for
        the unit tests.
        """
        raise BotoClientError('Not Implemented')

    def dump(self, file_name, page_size=10, vtimeout=10, sep='\n', callback=None):
        """Utility function to dump the messages in a queue to a file
        NOTE: Page size must be < 10 else SQS errors"""
        raise BotoClientError('Not Implemented')

    def save_to_file(self, fp, sep='\n', callback=None):
        """
        Read all messages from the queue and persist them to file-like object.
        Messages are written to the file and the 'sep' string is written
        in between messages.  Messages are deleted from the queue after
        being written to the file.
        Returns the number of messages saved.
        """
        raise BotoClientError('Not Implemented')

    def save_to_filename(self, file_name, sep='\n', callback=None):
        """
        Read all messages from the queue and persist them to local file.
        Messages are written to the file and the 'sep' string is written
        in between messages.  Messages are deleted from the queue after
        being written to the file.
        Returns the number of messages saved.
        """
        raise BotoClientError('Not Implemented')

    # for backwards compatibility
    save = save_to_filename

    def save_to_s3(self, bucket):
        """
        Read all messages from the queue and persist them to S3.
        Messages are stored in the S3 bucket using a naming scheme of::
        
            <queue_id>/<message_id>
        
        Messages are deleted from the queue after being saved to S3.
        Returns the number of messages saved.
        """
        raise BotoClientError('Not Implemented')

    def load_from_s3(self, bucket, prefix=None):
        """
        Load messages previously saved to S3.
        """
        raise BotoClientError('Not Implemented')

    def load_from_file(self, fp, sep='\n'):
        """Utility function to load messages from a file-like object to a queue"""
        raise BotoClientError('Not Implemented')

    def load_from_filename(self, file_name, sep='\n'):
        """Utility function to load messages from a local filename to a queue"""
        raise BotoClientError('Not Implemented')

    # for backward compatibility
    load = load_from_filename

# vim:set ft=python sw=4 :
