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
        # logger
        self.tmpLog = Logger.Logger()



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
        self.tmpLog.debug('res={0}'.format(str(res)))
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
        self.tmpLog.debug('res={0}'.format(str(res)))
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
        self.tmpLog.debug('res={0}'.format(str(res)))
        self.comm.returnResponse(res)
        # dump updated records
        self.db.dumpUpdates()



    # main
    def run(self):
        # get logger
        self.tmpLog.info('start')
        # load job
        self.tmpLog.info('loading job')
        tmpStat,tmpOut = self.loadJob()
        if not tmpStat:
            self.tmpLog.error(tmpOut)
            sys.exit(1)
        # make event table
        self.tmpLog.info('making EventTable')
        tmpStat,tmpOut = self.makeEventTable()
        if not tmpStat:
            self.tmpLog.error(tmpOut)
            sys.exit(1)
        # main loop
        while self.comm.activeRanks():
            # get request
            tmpStat,method,params = self.comm.receiveRequest()
            if not tmpStat:
                self.tmpLog.error(method)
                sys.exit(1)
            # execute
            self.tmpLog.debug('rank={0} method={1} param={2}'.format(self.comm.getRequesterRank(),
                                                                method,str(params)))
            if hasattr(self,method):
                methodObj = getattr(self,method)
                apply(methodObj,[params])
            else:
                self.tmpLog.error('unknown method={0} was requested from rank={1} '.format(method,
                                                                                      self.comm.getRequesterRank()))
        # final dump
        self.tmpLog.info('final dumping')
        self.db.dumpUpdates(True)
        self.tmpLog.info('done')
