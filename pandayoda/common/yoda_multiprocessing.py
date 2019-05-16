#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# - Taylor Childers (john.taylor.childers@cern.ch)

import os
from multiprocessing import *

# running inside the Athena release, at least for 21.0.15,
# the python version can be strange. While it reports Python 2.7
# some behaviors are like 2.6.
# Especially in multiprocessing module
# This file tries to masks these differences by overridding the
# module objects if the environment variable 'RUN_YODA_IN_ATHENA'
# is set.


if 'RUN_YODA_IN_ATHENA' in os.environ:
    print('Running with custom multiprocessing module')
    from multiprocessing import synchronize
    # for the Event object, in 2.6 the Event.wait(timeout=None) function
    # always returns None
    # for Python 2.7+ it returns True if the internal flag is set
    # it returns False otherwise. However the Python version 2.7
    # that comes with Athena has the 2.6 behavior

    class MyEvent(synchronize.Event):
        # overload the wait to behave like the 2.7+ version
        def wait(self, timeout=None):
            super(MyEvent, self).wait(timeout)
            return self.is_set()

    # override multiprocessing Event class with my version
    Event = MyEvent
else:
    print('Running with standard multiprocessing module')
