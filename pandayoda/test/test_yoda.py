import sys
import json
import pickle
from mpi4py import MPI
from pandayoda.yodacore import Interaction
from pandayoda.yodacore import Yoda
from pandayoda.yodacore import Logger

import logging
logging.basicConfig(level=logging.DEBUG)

er = [
    {'eventRangeID':'1-2-3',
     'startEvent':0,
     'lastEvent':9,
     'LFN':'NTUP_SUSY.01272447._000001.root.2',
     'GUID':'c58cc417-f369-44d8-81b4-72a76c1f2b79',
     'scope':'mc12_8TeV'},
    {'eventRangeID':'4-5-6',
     'startEvent':10,
     'lastEvent':19,
     'LFN':'NTUP_SUSY.01272447._000001.root.2',
     'GUID':'c58cc417-f369-44d8-81b4-72a76c1f2b79',
     'scope':'mc12_8TeV'},
    ]

job = {'PandaID':'123',
       'jobsetID':'567',
       }

f = open('job_pickle.txt','w')
pickle.dump(job,f)
f.close()

f = open('eventranges_pickle.txt','w')
pickle.dump(er,f)
f.close()

# get logger
tmpLog = Logger.Logger()


comm = MPI.COMM_WORLD
mpirank = comm.Get_rank()

if mpirank==0:
    yoda = Yoda.Yoda()
    yoda.run()
else:
    snd = Interaction.Requester()
    tmpLog.debug("rank{0} sending req".format(mpirank))
    tmpStat,jobData = snd.sendRequest('getJob',{'siteName':'TEST'})
    while True:
        tmpStat,res = snd.sendRequest('getEventRanges',{'pandaID':jobData['PandaID'],
                                                        'jobsetID':jobData['jobsetID']})
        eventRangesStr = res['eventRanges'][0]
        eventRanges = json.loads(eventRangesStr)
        tmpLog.debug("rank{0} got {1} ranges".format(mpirank,len(eventRanges)))
        if eventRanges == []:
            res = snd.sendRequest('updateJob',{'jobID':jobData['PandaID'],
                                               'state':'finished'})
            break
        else:
            for eventRange in eventRanges:
                tmpLog.debug("update rangeID={0} ranges".format(eventRange['eventRangeID']))
                snd.sendRequest('updateEventRange',{"eventRangeID":eventRange['eventRangeID'],
                                                    'eventStatus':"finished"})
    tmpLog.info('rank{0} done'.format(mpirank))
