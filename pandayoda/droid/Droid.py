import logging,threading,os,time
from mpi4py import MPI
from pandayoda.common import yoda_droid_messenger as ydmess,SerialQueue
from pandayoda.droid import JobManager,JobComm,FileManager,YodaComm
logger = logging.getLogger(__name__)


class Droid(threading.Thread):
   ''' 1 Droid runs per node of a parallel job and launches the AthenaMP process '''
   def __init__(self,jobWorkingDir,
                     outputDir,
                     config,
                     loopTimeout = 30,
               ):
      ''' jobWorkingDir: the directory where the panda job should run
          outputDir: the directory where output files should be copied 
          config: the ConfigParser handle for yoda 
          loopTimeout: the number of seconds used to control the loop execution interval '''

      # call Thread constructor
      super(Droid,self).__init__()

      self.jobWorkingDir   = jobWorkingDir
      self.outputDir       = outputDir
      self.config          = config
      self.loopTimeout     = loopTimeout

      self.rank            = MPI.COMM_WORLD.Get_rank()
      self.prelog          = 'Rank %03i:' % self.rank


      self.exit            = threading.Event()

   def stop(self):
      ''' this function can be called by outside threads to cause the JobManager thread to exit'''
      self.exit.set()

   # this runs when 'droid_instance.start()' is called
   def run(self):
      ''' this is the function called when the user runs droid_instance.start() '''
      logger.info('%s Droid Thread starting',self.prelog)

      # create queues for subthreads to send messages out
      queues = {}
      queues['JobManager']       = SerialQueue.SerialQueue()
      queues['JobComm']          = SerialQueue.SerialQueue()
      queues['FileManager']      = SerialQueue.SerialQueue()
      queues['YodaComm']         = SerialQueue.SerialQueue()
      queues['Droid']            = SerialQueue.SerialQueue()
      
      # a dictionary of subthreads
      subthreads = {}
      
      # create job manager thread
      subthreads['JobManager']   = JobManager.JobManager(queues,self.config,loopTimeout=self.loopTimeout)
      subthreads['JobManager'].start()

      # create job comm thread
      subthreads['JobComm']      = JobComm.JobComm(queues,loopTimeout=self.loopTimeout)
      subthreads['JobComm'].start()

      # create file manager thread
      subthreads['FileManager']  = FileManager.FileManager(queues,self.outputDir,loopTimeout=self.loopTimeout)
      subthreads['FileManager'].start()

      # create yoda comm thread
      subthreads['YodaComm']     = YodaComm.YodaComm(queues,self.outputDir,loopTimeout=self.loopTimeout)
      subthreads['YodaComm'].start()

      # begin while loop to monitor subthreads
      while not self.exit.isSet():
         logger.debug('%s droid start loop',self.prelog)
         # check the status of each subthread
         keys = subthreads.keys()
         for name in keys:
            thread = subthreads[name]
            # if the thread is not alive, throw an error
            if not thread.isAlive():
               logger.warning('%s %s is no longer running.',self.prelog,name)
               del subthreads[name]
            #else:
               #logger.debug('%s %s is running.',self.prelog,name)

         if len(subthreads) == 0:
            logger.info('%s no subthreads remaining, exiting',self.prelog)
            break
         time.sleep(self.loopTimeout)

      # send the exit signal to all subthreads
      logger.info('%s sending exit signal to subthreads',self.prelog)
      for name,thread in subthreads.iteritems():
         thread.stop()

      # wait for sub threads to exit
      logger.info('%s waiting for subthreads to join',self.prelog)
      for name,thread in subthreads.iteritems():
         thread.join()

      logger.info('%s droid exiting',self.prelog)




# testing this thread
if __name__ == '__main__':
   try:
      logging.basicConfig(level=logging.DEBUG,
            format='%(asctime)s|%(process)s|%(levelname)s|%(name)s|%(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')
      logging.info('Start test of Droid')
      import time
      import argparse
      import importlib
      import ConfigParser
      from pandayoda.common import serializer,MessageTypes
      oparser = argparse.ArgumentParser()
      oparser.add_argument('-y','--yoda-config', dest='yoda_config', help="The Yoda Config file where some configuration information is set.",default='yoda.cfg')
      oparser.add_argument('-w','--jobWorkingDir',dest='jobWorkingDir',help='Where to run the job',required=True)

      args = oparser.parse_args()

      if not os.path.exists(args.jobWorkingDir):
         logger.error('working directory does not exist: %s',args.jobWorkingDir)
         sys.exit(-1)

      os.chdir(args.jobWorkingDir)

      config = ConfigParser.ConfigParser()
      config.read(args.yoda_config)


      droid = Droid(args.jobWorkingDir,args.jobWorkingDir,config,loopTimeout=3)

      droid.start()

      req = None
      jobn = 0
      evtrgn = 0
      while droid.join(timeout=5) is None:
         if not droid.isAlive(): break

         if req is None:
            # get message from droid
            req = ydmess.get_droid_message()

         if req is not None:
            # check if message has been received
            status = MPI.Status()
            flag,data = req.test(status=status)
            if flag:
               # message received
               source_rank = status.Get_source()
               logger.info('received message from droid rank %d: %s',source_rank,data)
               req = None

               if data['type'] == MessageTypes.REQUEST_JOB:
                  logger.info('received request for job from rank %d',source_rank)
                  jobn += 1
                  job = serializer.deserialize(open(args.jobWorkingDir +'/pandajob.json').read())
                  job['PandaID'] = job['PandaID'] + str(jobn)
                  logger.info('sending job to droid rank %d',source_rank)
                  ydmess.send_droid_new_job(job,source_rank)
               elif data['type'] == MessageTypes.REQUEST_EVENT_RANGES:
                  logger.info('received request for event range from rank %d',source_rank)
                  jobwise_evtrgs = serializer.deserialize(open(args.jobWorkingDir + '/JobsEventRanges.json').read())
                  evtrgs = jobwise_evtrgs[jobwise_evtrgs.keys()[0]]
                  if evtrgn < len(evtrgs):
                     logger.info('sending event range to droid rank: %d',source_rank)
                     ydmess.send_droid_new_eventranges([evtrgs[evtrgn]],source_rank)
                     evtrgn += 1
                  else:
                     logger.info('sending no more event range message to droid rank: %d',source_rank)
                     ydmess.send_droid_no_eventranges_left(source_rank)
                     evtrgn += 1


            else:
               continue
   except:
      droid.stop()

      droid.join()

   logger.info('Droid test exiting')


      