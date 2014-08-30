import sys
import pickle
import Interaction,Database,Logger

# main Yoda class
class Yoda:
    
    # constructor
    def __init__(self):
        # communication channel
        self.comm = Interaction.Receiver()
        # database backend
        self.db = Database.Backend()



    # load job
    def loadJob(self):
        try:
            # load job
            tmpFile = open('job_pickle.txt')
            self.job = pickle.load(tmpFile)
            tmpFile.close()
            return True,None
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to load job with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg



    # make event table
    def makeEventTable(self):
        try:
            # load event ranges
            tmpFile = open('eventranges_pickle.txt')
            eventRangeList = pickle.load(tmpFile)
            tmpFile.close()
            # setup database
            self.db.setupEventTable(self.job,eventRangeList)
            return True,None
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed to make event table with {0}:{1}'.format(errtype.__name__,errvalue)
            return False,errMsg
        


    # get job
    def getJob(self,params):
        self.comm.returnResponse(self.job)



    # update job
    def updateJob(self,params):
        # final heartbeat
        if params['jobStatus'] in ['finished','failed']:
            self.comm.decrementNumRank()
        # make response
        res = {'StatusCode':0,
               'command':'NULL'}
        # return
        self.comm.returnResponse(res)
        

    # get event ranges
    def getEventRanges(self,params):
        # number of event ranges
        if 'nRanges' in params:
            nRanges = params['nRanges']
        else:
            nRanges = 1
        # get event ranges from DB
        eventRanges = self.db.getEventRanges(nRanges)
        # make response
        res = {'StatusCode':0,
               'eventRanges':eventRanges}
        # return response
        self.comm.returnResponse(res)
        # dump updated records
        self.db.dumpUpdates()



    # update event range
    def updateEventRange(self,params):
        # extract parameters
        eventRangeID = params['eventRangeID']
        eventStatus = params['eventStatus']
        # update database
        self.db.updateEventRange(eventRangeID,eventStatus)
        # make response
        res = {'StatusCode':0}
        # return
        self.comm.returnResponse(res)
        # dump updated records
        self.db.dumpUpdates()



    # main
    def run(self):
        # get logger
        tmpLog = Logger.Logger()
        # load job
        tmpStat,tmpOut = self.loadJob()
        if not tmpStat:
            tmpLog.error(tmpOut)
            sys.exit(1)
        # make event table
        tmpStat,tmpOut = self.makeEventTable()
        if not tmpStat:
            tmpLog.error(tmpOut)
            sys.exit(1)
        # main loop
        while self.comm.activeRanks():
            # get request
            tmpStat,method,params = self.comm.receiveRequest()
            if not tmpStat:
                sys.exit(1)
            # execute
            if hasattr(self,method):
                methodObj = getattr(self,method)
                apply(methodObj,[params])
            else:
                tmpLog.error('unknown method={0} was requested'.format(method))
        # final dump
        self.db.dumpUpdates(True)
                
