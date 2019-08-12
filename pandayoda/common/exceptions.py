# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)


class MessengerJobAlreadyRequested(Exception):
    pass


class MessengerEventRangesAlreadyRequested(Exception):
    pass


class MessengerConfigError(Exception):
    pass


class MessengerFailedToParse(Exception):
    pass
