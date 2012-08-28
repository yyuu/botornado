# botornado

## Overview

An asynchronous AWS client on Tornado.

This is a dirty work to move boto onto Tornado ioloop.
Only SQS and S3 have been implemented as of 2012/Jan/26.


## Requirements

* boto (https://github.com/boto/boto)
* tornado (https://github.com/facebook/tornado)


## Samples

almost all options are same with boto other than callback.

sample S3 client.

    import os, sys
    from botornado.s3.connection import AsyncS3Connection
    client = AsyncS3Connection(aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                               aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
    # getting all your buckets
    def cb1(buckets):
        print 'your buckets:', buckets
    client.get_all_buckets(callback=cb1)

sample SQS client.

    import os, sys
    import botornado.sqs
    client = botornado.sqs.connect_to_region('ap-northeast-1',
                                             aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                                             aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
    def cb2(queues):
        print 'your queues:', queues
    client.get_all_queues(callback=cb2)


## License

MIT


## Author

* Copyright (C) 2011 Yamashita, Yuu <yamashita@geishatokyo.com>
* Copyright (C) 2011 Geisha Tokyo Entertainment, Inc.
