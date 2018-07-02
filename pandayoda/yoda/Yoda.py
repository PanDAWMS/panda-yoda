import logging,threading,time,os,datetime
import WorkManager,FileManager
from pandayoda.common import SerialQueue,MessageTypes,MPIService
logger = logging.getLogger(__name__)

config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]


class Yoda(threading.Thread):
   def __init__(self,config):
      ''' config: configuration of Yoda
      '''
      # call Thread constructor
      super(Yoda,self).__init__()

      # configuration of Yoda
      self.config             = config

      # keep track of if the wallclock has expired
      self.wallclock_expired = threading.Event()

      # this is used to trigger the thread exit
      self.exit               = threading.Event()


   def stop(self):
      ''' this function can be called by outside threads to cause the Yoda thread to exit'''
      self.exit.set()

   # this runs when 'yoda_instance.start()' is called
   def run(self):
      ''' this is the function called when the user runs yoda_instance.start() '''
      try:
         self.subrun()
      except Exception:
         logger.exception('Yoda failed with uncaught exception')
         MPIService.MPI.COMM_WORLD.Abort()


   def subrun(self):
      ''' this function is the business logic, but wrapped in exception '''
      logger.info('Yoda Thread starting')
      logger.debug('config_section: %s',config_section)
      logger.debug('cwd: %s',os.getcwd())

      # keep track of starting path
      top_working_path = os.getcwd()

      # read config
      self.read_config()

      # create queues for subthreads to send messages out
      self.queues = {
         'Yoda':SerialQueue.SerialQueue(),
         'WorkManager':SerialQueue.SerialQueue(),
         'FileManager':SerialQueue.SerialQueue(),
         'MPIService':SerialQueue.SerialQueue(),
      }

      # create forwarding map for Yoda:
      forwarding_map = {
         MessageTypes.REQUEST_JOB: ['WorkManager'],
         MessageTypes.REQUEST_EVENT_RANGES: ['WorkManager'],
         MessageTypes.OUTPUT_FILE: ['FileManager'],
         MessageTypes.DROID_HAS_EXITED: ['Yoda'],
      }

      # initialize the MPIService with the queues
      MPIService.mpiService.initialize(self.config,self.queues,forwarding_map)

      # a list of ranks that have exited
      self.exited_droids = []
      
      # a dictionary of subthreads
      subthreads = {}
      
      # create WorkManager thread
      subthreads['WorkManager']  = WorkManager.WorkManager(self.config,self.queues)
      subthreads['WorkManager'].start()

      # create FileManager thread
      subthreads['FileManager']  = FileManager.FileManager(self.config,self.queues,top_working_path)
      subthreads['FileManager'].start()

      # start message loop
      while not self.exit.isSet():
         logger.debug('start loop')
         
         # process incoming messages from other threads or ranks
         self.process_incoming_messages()

         # check if all droids have exited
         if len(self.exited_droids) >= (MPIService.nranks - 1):
            logger.info('all droids have exited, exiting yoda')
            self.stop()
            break


         # check the status of each subthread
         logger.debug('checking all threads still alive')
         keys = subthreads.keys()
         for name in keys:
            thread = subthreads[name]
            # if the thread is not alive, throw an error
            if not thread.isAlive():
               logger.warning('%s is no longer running.',name)
               del subthreads[name]
               if name == 'WorkManager':
                  self.stop()
                  continue
            # else:
            # logger.debug('%s %s is running.',self.prelog,name)

         if len(subthreads) == 0:
            logger.info('no subthreads remaining, exiting')
            self.stop()
            break

         if self.queues['Yoda'].empty():
            logger.debug('sleeping %s',self.loop_timeout)
            self.exit.wait(timeout=self.loop_timeout)
      
      # send the exit signal to all droid ranks
      logger.info('sending exit signal to droid ranks')
      for ranknum in range(1,MPIService.nranks):
         if self.wallclock_expired.isSet():
            self.queues['MPIService'].put({'type':MessageTypes.WALLCLOCK_EXPIRING,'destination_rank':ranknum})
         else:
            self.queues['MPIService'].put({'type':MessageTypes.DROID_EXIT,'destination_rank':ranknum})

      # send the exit signal to all subthreads
      logger.info('sending exit signal to subthreads')
      for name,thread in subthreads.iteritems():
         thread.stop()

      # wait for sub threads to exit
      logger.info('waiting for subthreads to join')
      for name,thread in subthreads.iteritems():
         thread.join()


      while not MPIService.mpiService.message_queue_empty():
         logger.info('waiting for MPIService to send exit messages to Droid, sleep for %s',self.loop_timeout)
         time.sleep(self.loop_timeout)

      logger.info('Yoda is exiting')


   

   def read_config(self):

      # read yoda log level:
      if self.config.has_option(config_section,'loglevel'):
         self.loglevel = self.config.get(config_section,'loglevel')
         logger.info('%s loglevel: %s',config_section,self.loglevel)
         logger.setLevel(logging.getLevelName(self.loglevel))
      else:
         logger.warning('no "loglevel" in "%s" section of config file, keeping default',config_section)
         
      
      # read yoda loop timeout:
      if self.config.has_option(config_section,'loop_timeout'):
         self.loop_timeout = self.config.getfloat(config_section,'loop_timeout')
      else:
         logger.error('must specify "loop_timeout" in "%s" section of config file',config_section)
         return
      logger.info('%s loop_timeout: %d',config_section,self.loop_timeout)

      
      


   def process_incoming_messages(self):

      while not self.queues['Yoda'].empty():

         qmsg = self.queues['Yoda'].get(block=False)

         # process message
         
         logger.debug('received message: %s',qmsg)
         
         if qmsg['type'] == MessageTypes.DROID_HAS_EXITED:
            logger.debug(' droid rank %d has exited',qmsg['source_rank'])
            self.exited_droids.append(qmsg['source_rank'])
            logger.debug('%s droid ranks have exited',len(self.exited_droids))
         else:
            logger.error(' could not interpret message: %s',qmsg)


      