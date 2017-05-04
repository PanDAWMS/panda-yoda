import logging,threading,SerialQueue,os,time
from mpi4py import MPI
import yoda_droid_messenger as ydmess
import JobManager,JobComm,FileManager,YodaComm
logger = logging.getLogger(__name__)


class Droid(threading.Thread):
   def __init__(self,jobWorkingDir,
                     messenger,
                     outputDir,
               ):
      # call Thread constructor
      super(Droid,self).__init__()

      self.jobWorkingDir   = jobWorkingDir
      self.outputDir       = outputDir

      self.messenger       = messenger
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
      subthreads['JobManager']   = JobManager.JobManager(queues)
      subthreads['JobManager'].start()

      # create job comm thread
      subthreads['JobComm']      = JobComm.JobComm(queues)
      subthreads['JobComm'].start()

      # create file manager thread
      subthreads['FileManager']  = FileManager.FileManager(queues,self.outputDir)
      subthreads['FileManager'].start()

      # create yoda comm thread
      subthreads['YodaComm']     = YodaComm.YodaComm(queues,self.outputDir)
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
            else:
               logger.debug('%s %s is running.',self.prelog,name)

         if len(subthreads) == 0:
            logger.info('%s no subthreads remaining, exiting',self.prelog)
            break
         time.sleep(2)

      # send the exit signal to all subthreads
      logger.info('%s sending exit signal to subthreads',self.prelog)
      for name,thread in subthreads.iteritems():
         thread.exit()

      # wait for sub threads to exit
      logger.info('%s waiting for subthreads to join',self.prelog)
      for name,thread in subthreads.iteritems():
         thread.join()

      logger.info('%s droid exiting',self.prelog)




# testing this thread
if __name__ == '__main__':
   logging.basicConfig(level=logging.DEBUG,
         format='%(asctime)s|%(process)s|%(levelname)s|%(name)s|%(message)s',
         datefmt='%Y-%m-%d %H:%M:%S')
   logging.info('Start test of Droid')
   import time
   import argparse
   import importlib
   oparser = argparse.ArgumentParser()
   oparser.add_argument('-y','--yoda-config', dest='yoda_config', help="The Yoda Config file where some configuration information is set.",default='yoda.cfg')
   oparser.add_argument('-w','--jobWorkingDir',dest='jobWorkingDir',help='Where to run the job',required=True)

   args = oparser.parse_args()

   if not os.path.exists(args.jobWorkingDir):
      logger.error('working directory does not exist: %s',args.jobWorkingDir)
      sys.exit(-1)

   os.chdir(args.jobWorkingDir)

   # create the messenger, which is a plugin
   # try to import the module specified by the parameter 'messenger_plugin'
   # if it is not in the PYTHONPATH this will fail
   messenger_plugin        = 'shared_file_messenger'
   try:
      messenger = importlib.import_module(messenger_plugin)
   except ImportError:
      logger.exception('Failed to import messenger_plugin: %s',messenger_plugin)
      raise
   # now seteup messenger
   messenger.setup(args.yoda_config)


   droid = Droid(args.jobWorkingDir,messenger,args.jobWorkingDir)

   droid.start()

   while droid.join(timeout=5) is None:
      if not droid.isAlive(): break



   logger.info('Droid test exiting')


      