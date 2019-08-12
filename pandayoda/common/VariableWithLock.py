# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)

from pandayoda.common.yoda_multiprocessing import Event, Value
import logging
import ctypes
logger = logging.getLogger(__name__)


def get_array_type(var):
    if type(var) is int:
        return 'i'
    elif type(var) is float:
        return 'f'
    elif type(var) is long:
        return 'I'
    elif type(var) is str:
        if len(var) == 1:
            return 'c'
        else:
            return ctypes.c_char_p
    else:
        logger.error('invalid type object %s', str(var))


class VariableWithLock:
    def __init__(self, initial):

        self.value = Value(get_array_type(initial), initial, lock=True)

        self.is_set_flag = Event()

    def set(self, value):
        self.value.get_lock().acquire()
        self.value.value = value
        self.is_set_flag.set()
        self.value.get_lock().release()

    def get(self):
        return self.value.value

    def wait(self, timeout=None):
        self.is_set_flag.wait(timeout)

    def clear(self):
        self.is_set_flag.clear()
