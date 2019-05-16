#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)

import json
import Database

eventRangeList = [
    {'eventRangeID': '1-2-3',
     'startEvent': 0,
     'lastEvent': 9,
     'LFN': 'NTUP_SUSY.01272447._000001.root.2',
     'GUID': 'c58cc417-f369-44d8-81b4-72a76c1f2b79',
     'scope': 'mc12_8TeV'},
    {'eventRangeID': '4-5-6',
     'startEvent': 10,
     'lastEvent': 19,
     'LFN': 'NTUP_SUSY.01272447._000001.root.2',
     'GUID': 'c58cc417-f369-44d8-81b4-72a76c1f2b79',
     'scope': 'mc12_8TeV'},
    ]


db = Database.Backend()

db.setupEventTable(None, eventRangeList)

while True:
    tmpListS = db.getEventRanges(1)
    tmpList = json.loads(tmpListS)
    print len(tmpList), "ranges"
    if tmpList == []:
        break
    for tmpItem in tmpList:
        print tmpItem
        print "update", tmpItem["eventRangeID"]
        db.updateEventRange(tmpItem["eventRangeID"], "finished")
db.dumpUpdates()
