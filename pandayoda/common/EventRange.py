#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)


class EventRange:
    """ An Event Service Event Range:
        {"eventRangeID": "8848710-3005316503-6391858827-3-10", "LFN":"EVNT.06402143._012906.pool.root.1", "lastEvent": 3, "startEvent": 3, "scope": "mc15_13TeV", "GUID": "63A015D3-789D-E74D-BAA9-9F95DB068EE9"}
    """

    READY = 'READY'
    ASSIGNED = 'ASSIGNED'
    COMPLETED = 'COMPLETED'

    # order matters in this list, see EventRangeList.__add__ function for reason why
    STATES = [COMPLETED, ASSIGNED, READY]

    def __init__(self, eventrange=None):
        """ Event Range representation, can pass optional dictionary as described above for filling object """
        # the event range ID, format = "8848710-3005316503-6391858827-3-10"
        self.id = ''
        # filename in which events reside
        self.lfn = ''
        # filename scope
        self.scope = ''
        # start of event range
        self.startEvent = -1
        # end of event range
        self.lastEvent = -1
        # GUID for this event range
        self.GUID = ''

        # state for tracking processed event ranges
        self.state = EventRange.READY

        if eventrange:
            self.fill_from_dict(eventrange)

    def nevents(self):
        """ return the number of events in this range """
        if self.startEvent == -1 or self.lastEvent == -1:
            return 0

        return self.lastEvent - self.startEvent + 1

    def fill_from_dict(self, eventrange):
        self.id = eventrange['eventRangeID']
        self.lfn = eventrange['LFN']
        self.scope = eventrange['scope']
        self.startEvent = eventrange['startEvent']
        self.lastEvent = eventrange['lastEvent']
        self.GUID = eventrange['GUID']

    def get_dict(self):
        return {'eventRangeID': self.id, 'LFN': self.lfn, 'scope':self.scope,
                'startEvent': self.startEvent, 'lastEvent': self.lastEvent,
                'GUID': self.GUID}

    def set_assigned(self):
        self.state = EventRange.ASSIGNED

    def set_completed(self):
        self.state = EventRange.COMPLETED


# testing this thread
if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s|%(process)s|%(levelname)s|%(name)s|%(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)
    logger.info('Start test of EventRange')

    # import argparse
    # oparser = argparse.ArgumentParser()
    # oparser.add_argument('-l','--jobWorkingDir', dest="jobWorkingDir", default=None, help="Job's working directory.",required=True)
    # args = oparser.parse_args()

    logger.info('test object creation')
    er = EventRange()

    logger.info('test fill_from_dict ')
    d = {"eventRangeID": "8848710-3005316503-6391858827-3-10",
         "LFN": "EVNT.06402143._012906.pool.root.1",
         "lastEvent": 3,
         "startEvent": 3,
         "scope": "mc15_13TeV",
         "GUID": "63A015D3-789D-E74D-BAA9-9F95DB068EE9"}
    er.fill_from_dict(d)

    logger.info('test get dict')
    d = er.get_dict()
    logger.info('  output = %s', d)
