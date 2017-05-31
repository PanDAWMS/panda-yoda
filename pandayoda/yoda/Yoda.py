import logging,threading
import WorkManager,DroidComm,HarvesterComm
from pandayoda.common import yoda_droid_messenger as ydm
logger = logging.getLogger(__name__)


class Yoda(threading.Thread):
   def __init__(self,jobWorkingDir,config
                     outputDir = None,
               ):
      # call Thread constructor
      super(Yoda,self).__init__()

      # working directory for the jobs
      self.jobWorkingDir   = jobWorkingDir

      # output directory where event output is placed
      self.outputDir       = outputDir
      if outputDir is None:
         self.outputDir    = jobWorkingDir

      # configuration of Yoda
      self.config          = config

      # this is used to trigger the thread exit
      self.exit = threading.Event()

   def stop(self):
      ''' this function can be called by outside threads to cause the Yoda thread to exit'''
      self.exit.set()



   # this runs when 'yoda_instance.start()' is called
   def run(self):
      ''' this function is executed as the subthread. '''
      logger.info('Yoda Thread starting')

      # create queues for subthreads to send messages out
      queues = {}
      queues['WorkManager']      = SerialQueue.SerialQueue()
      
      # a dictionary of subthreads
      subthreads = {}
      
      # create droid comm thread
      subthreads['WorkManager']  = DroidComm.DroidComm(queues,loopTimeout=self.loopTimeout)
      subthreads['WorkManager'].start()


      # start message loop
      while not self.exit.isSet():
         logger.debug('start loop')
         # check for message from Droids

         # check the status of each subthread
         keys = subthreads.keys()
         for name in keys:
            thread = subthreads[name]
            # if the thread is not alive, throw an error
            if not thread.isAlive():
               logger.warning('%s is no longer running.',name)
               del subthreads[name]
            #else:
               #logger.debug('%s %s is running.',self.prelog,name)

         if len(subthreads) == 0:
            logger.info('no subthreads remaining, exiting')
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

      logger.info('Yoda is exiting')


      




      