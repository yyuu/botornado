# Copyright (c) 2006,2007 Mitch Garnaat http://garnaat.org/
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
SQS Message

A Message represents the data stored in an SQS queue.  The rules for what is allowed within an SQS
Message are here:

    http://docs.amazonwebservices.com/AWSSimpleQueueService/2008-01-01/SQSDeveloperGuide/Query_QuerySendMessage.html

So, at it's simplest level a Message just needs to allow a developer to store bytes in it and get the bytes
back out.  However, to allow messages to have richer semantics, the Message class must support the 
following interfaces:

The constructor for the Message class must accept a keyword parameter "queue" which is an instance of a
boto Queue object and represents the queue that the message will be stored in.  The default value for
this parameter is None.

The constructor for the Message class must accept a keyword parameter "body" which represents the
content or body of the message.  The format of this parameter will depend on the behavior of the
particular Message subclass.  For example, if the Message subclass provides dictionary-like behavior to the
user the body passed to the constructor should be a dict-like object that can be used to populate
the initial state of the message.

The Message class must provide an encode method that accepts a value of the same type as the body
parameter of the constructor and returns a string of characters that are able to be stored in an
SQS message body (see rules above).

The Message class must provide a decode method that accepts a string of characters that can be
stored (and probably were stored!) in an SQS message and return an object of a type that is consistent
with the "body" parameter accepted on the class constructor.

The Message class must provide a __len__ method that will return the size of the encoded message
that would be stored in SQS based on the current state of the Message object.

The Message class must provide a get_body method that will return the body of the message in the
same format accepted in the constructor of the class.

The Message class must provide a set_body method that accepts a message body in the same format
accepted by the constructor of the class.  This method should alter to the internal state of the
Message object to reflect the state represented in the message body parameter.

The Message class must provide a get_body_encoded method that returns the current body of the message
in the format in which it would be stored in SQS.
"""

import base64
import StringIO
from boto.sqs.attributes import Attributes
from boto.exception import SQSDecodeError

from boto.sqs.message import *

class AsyncRawMessage(RawMessage):
    """
    Base class for SQS messages.  RawMessage does not encode the message
    in any way.  Whatever you store in the body of the message is what
    will be written to SQS and whatever is returned from SQS is stored
    directly into the body of the message.
    """
    def delete(self, callback=None):
        if self.queue:
            return self.queue.delete_message(self, callback=callback)

    def change_visibility(self, visibility_timeout, callback=None):
        if self.queue:
            self.queue.connection.change_message_visibility(self.queue,
                                                            self.receipt_handle,
                                                            visibility_timeout, callback=callback)
     
class AsyncMessage(AsyncRawMessage, Message):
    """
    The default Message class used for SQS queues.  This class automatically
    encodes/decodes the message body using Base64 encoding to avoid any
    illegal characters in the message body.  See:

    http://developer.amazonwebservices.com/connect/thread.jspa?messageID=49680%EC%88%90

    for details on why this is a good idea.  The encode/decode is meant to
    be transparent to the end-user.
    """

class AsyncMHMessage(AsyncMessage, MHMessage):
    """
    The MHMessage class provides a message that provides RFC821-like
    headers like this:

    HeaderName: HeaderValue

    The encoding/decoding of this is handled automatically and after
    the message body has been read, the message instance can be treated
    like a mapping object, i.e. m['HeaderName'] would return 'HeaderValue'.
    """

class AsyncEncodedMHMessage(AsyncMHMessage, EncodedMHMessage):
    """
    The EncodedMHMessage class provides a message that provides RFC821-like
    headers like this:

    HeaderName: HeaderValue

    This variation encodes/decodes the body of the message in base64 automatically.
    The message instance can be treated like a mapping object,
    i.e. m['HeaderName'] would return 'HeaderValue'.
    """
    
