import sys
import json
import pickle
from mpi4py import MPI
from pandayoda.yodacore import Interaction
from pandayoda.yodacore import Yoda

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
       }

f = open('job_pickle.txt','w')
pickle.dump(job,f)
f.close()

f = open('eventranges_pickle.txt','w')
pickle.dump(er,f)
f.close()


comm = MPI.COMM_WORLD
mpirank = comm.Get_rank()

if mpirank==0:
    yoda = Yoda.Yoda()
    yoda.run()
    print "yoda done"
else:
    snd = Interaction.Requester()
    print mpirank,"sending req"
    tmpStat,res = snd.sendRequest('getJob',{1:2,3:4,'rank':mpirank})
    while True:
        tmpStat,res = snd.sendRequest('getEventRanges',{})
        eventRangesStr = res['eventRanges'][0]
        eventRanges = json.loads(eventRangesStr)
        print mpirank,"go {0} ranges".format(len(eventRanges))
        if eventRanges == []:
            res = snd.sendRequest('updateJob',{'jobStatus':'finished'})
            break
        else:
            for eventRange in eventRanges:
                print mpirank,"update rangeID={0} ranges".format(eventRange['eventRangeID'])
                snd.sendRequest('updateEventRange',{"eventRangeID":eventRange['eventRangeID'],
                                                    'eventStatus':"finished"})
    print mpirank,"done"
