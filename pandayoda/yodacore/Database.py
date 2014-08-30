import os
import json
import sqlite3
import datetime


# database class
class Backend:
  
    # constructor
    def __init__(self):
        # database file name
        self.dsFileName = './events_sqlite.db'
        # timestamp when dumping updates 
        self.dumpedTime = None



    # setup table
    def setupEventTable(self,job,eventRangeList):
        # delete file just in case
        try:
            os.remove(self.dsFileName)
        except:
            pass
        # make connection
        self.conn = sqlite3.connect(self.dsFileName)
        # make cursor
        self.cur = self.conn.cursor()
        # make event table
        sqlM  = "CREATE TABLE JEDI_Events("
        sqlM += "eventRangeID text,"
        sqlM += "startEvent integer,"
        sqlM += "lastEvent integer,"
        sqlM += "LFN text,"
        sqlM += "GUID text,"
        sqlM += "scope text,"
        sqlM += "status text,"
        sqlM += "todump integer,"
        sqlM  = sqlM[:-1]
        sqlM += ")"
        self.cur.execute(sqlM)
        # insert event ranges
        sqlI  = "INSERT INTO JEDI_Events ("
        sqlI += "eventRangeID,"
        sqlI += "startEvent,"
        sqlI += "lastEvent,"
        sqlI += "LFN,"
        sqlI += "GUID,"
        sqlI += "scope,"
        sqlI += "status,"
        sqlI += "todump,"
        sqlI  = sqlI[:-1]
        sqlI += ") "
        sqlI += "VALUES("
        sqlI += ":eventRangeID,"
        sqlI += ":startEvent,"
        sqlI += ":lastEvent,"
        sqlI += ":LFN,"
        sqlI += ":GUID,"
        sqlI += ":scope,"
        sqlI += ":status,"
        sqlI += ":todump,"
        sqlI  = sqlI[:-1]
        sqlI += ")"
        for tmpDict in eventRangeList:
            tmpDict['status'] = 'ready'
            tmpDict['todump'] = 0
            self.cur.execute(sqlI,tmpDict)
        # return
        return



    # get event ranges
    def getEventRanges(self,nRanges):
        # sql to get event range
        sqlI  = "SELECT "
        sqlI += "eventRangeID,"
        sqlI += "startEvent,"
        sqlI += "lastEvent,"
        sqlI += "LFN,"
        sqlI += "GUID,"
        sqlI += "scope,"
        sqlI  = sqlI[:-1]
        sqlI += " FROM JEDI_Events WHERE status=:status ORDER BY rowid "
        # sql to update event range
        sqlU  = "UPDATE JEDI_Events SET status=:status WHERE eventRangeID=:eventRangeID "
        # get event ranges
        varMap = {}
        varMap['status'] = 'ready'
        self.cur.execute(sqlI,varMap)
        retRanges = []
        for i in range(nRanges):
            # get one row
            tmpRet = self.cur.fetchone()
            if tmpRet == None:
                break
            eventRangeID,startEvent,lastEvent,LFN,GUID,scope = tmpRet
            tmpDict = {}
            tmpDict['eventRangeID'] = eventRangeID
            tmpDict['startEvent']   = startEvent
            tmpDict['lastEvent']    = lastEvent
            tmpDict['LFN']          = LFN
            tmpDict['GUID']         = GUID
            tmpDict['scope']        = scope
            # update status
            varMap = {}
            varMap['eventRangeID'] = eventRangeID
            varMap['status'] = 'running'
            self.cur.execute(sqlU,varMap)
            # append
            retRanges.append(tmpDict)
        # return list
        return json.dumps(retRanges)



    # update event range
    def updateEventRange(self,eventRangeID,eventStatus):
        sql = "UPDATE JEDI_Events SET status=:status,todump=:todump WHERE eventRangeID=:eventRangeID "
        varMap = {}
        varMap['eventRangeID'] = eventRangeID
        varMap['status']       = eventStatus
        varMap['todump']       = 1
        self.cur.execute(sql,varMap)
        return



    # dump updated records
    def dumpUpdates(self,forceDump=False):
        timeNow = datetime.datetime.utcnow()
        # forced or first dump or enough interval
        if forceDump or self.dumpedTime == None or \
                timeNow-self.dumpedTime > datetime.timedelta(seconds=600):
            # get event ranges to be dumped
            sql = "SELECT eventRangeID,status FROM JEDI_Events WHERE todump=:todump "
            varMap = {}
            varMap['todump'] = 1
            self.cur.execute(sql,varMap)
            # dump
            res = self.cur.fetchall()
            if len(res) > 0:
                outFileName = timeNow.strftime("%Y-%m-%d-%H-%M-%S") + '.dump'
                outFile = open(outFileName,'w')
                for eventRangeID,status in res:
                    outFile.write('{0} {1}\n'.format(eventRangeID,status))
                outFile.close()
            # update timestamp
            self.dumpedTime = timeNow
        # return
        return
 
