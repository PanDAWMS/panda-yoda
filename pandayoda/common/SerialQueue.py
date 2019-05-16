#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)

from pandayoda.common.yoda_multiprocessing import Queue
import serializer
# from Queue import Queue,Full,Empty


class SerialQueue(Queue):
    pass


class SerialQueue2(Queue, object):
    """ custom version of Queue class which takes python objects and serializes them into strings """
    def put(self, message, block=True, timeout=None):
        smsg = serializer.serialize(message)
        super(SerialQueue, self).put(smsg, block=block, timeout=timeout)

    def get(self,block=True, timeout=None):
        smsg = super(SerialQueue, self).get(block=block, timeout=timeout)
        return serializer.deserialize(smsg)
