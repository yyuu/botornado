#!/usr/bin/env python

import boto.sqs
import boto.regioninfo
import botornado.sqs.connection

def regions():
    def f(r):
        return boto.regioninfo.RegionInfo(connection=r.connection, name=r.name, endpoint=r.endpoint,
                                          connection_cls=botornado.sqs.connection.AsyncSQSConnection)
    return map(f, boto.sqs.regions())

def connect_to_region(region_name, **kwargs):
    for region in regions():
        if region.name == region_name:
            return region.connect(**kwargs)
    return None

# vim:set ft=python sw=4 :
