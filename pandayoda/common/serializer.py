#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - ..

import json,logging
logger = logging.getLogger(__name__)


def serialize(msg, pretty_print=False):
    try:
        if pretty_print:
            return json.dumps(msg, indent=2, sort_keys=True)
        else:
            return json.dumps(msg)
    except Exception:
        logger.exception('failed to serialize the message: %s', msg)
        raise


def deserialize(msg):
    try:
        return json.loads(msg)
    except Exception:
        logger.exception('failed to deserialize the message: %s', msg)
        raise
