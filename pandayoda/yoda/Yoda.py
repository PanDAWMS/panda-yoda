import logging,time,os,importlib
import WorkManager,FileManager
from pandayoda.common import MessageTypes,MPIService
from pandayoda.common.yoda_multiprocessing import Process,Event
logger = logging.getLogger(__name__)

config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]


class Yoda(Process):
   def __init__(self,queues,config):
      ''' config: configuration of Yoda
      '''
      # call Thread constructor
      super(Yoda,self).__init__()

      # message queues
      self.queues             = queues

      # config settings
      self.config             = config

      # keep track of if the wallclock has expired
      self.wallclock_expired  = Event()

      # this is used to trigger the thread exit
      self.exit               = Event()


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
         raise

   def subrun(self):
      ''' this function is the business logic, but wrapped in exception '''

      self.read_config()

      # set logging level
      logger.info('Yoda Thread starting')
      logger.info('loglevel:                    %s',self.loglevel)
      logger.info('loop_timeout:                %s',self.loop_timeout)
      top_working_path  = os.getcwd()
      logger.debug('cwd:                        %s',top_working_path)

      # setup harvester messenger to share with FileManager and WorkManager
      logger.debug('setup harvester messenger')
      harvester_messenger = self.get_harvester_messenger()
      harvester_messenger.setup(self.config)
      # wait for setup to complete
      harvester_messenger.sfm_har_config_done.wait()

      # a list of ranks that have exited
      self.exited_droids = []
      
      # a dictionary of subthreads
      subthreads = {}
      
      # create WorkManager thread
      subthreads['WorkManager']  = WorkManager.WorkManager(self.config,self.queues,harvester_messenger)
      subthreads['WorkManager'].start()

      # create FileManager thread
      subthreads['FileManager']  = FileManager.FileManager(self.config,self.queues,top_working_path,harvester_messenger)
      subthreads['FileManager'].start()

      # start message loop
      while not self.exit.is_set():
         logger.debug('start loop')
         
         # process incoming messages from other threads or ranks
         self.process_incoming_messages()

         # check if all droids have exited
         if len(self.exited_droids) >= (MPIService.mpiworldsize.get() - 1):
            logger.info('all droids have exited, exiting yoda')
            self.stop()
            break


         # check the status of each subthread
         logger.debug('checking all threads still alive')
         keys = subthreads.keys()
         for name in keys:
            thread = subthreads[name]
            # if the thread is not alive, throw an error
            if not thread.is_alive():
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
      for ranknum in range(1,MPIService.mpiworldsize.get()):
         if ranknum not in self.exited_droids:
            if self.wallclock_expired.is_set():
               self.queues['MPIService'].put({'type':MessageTypes.WALLCLOCK_EXPIRING,'destination_rank':ranknum})
            else:
               self.queues['MPIService'].put({'type':MessageTypes.DROID_EXIT,'destination_rank':ranknum})

      # send the exit signal to all subthreads
      for name,thread in subthreads.iteritems():
         logger.info('sending exit signal to %s',name)
         thread.stop()

      # wait for sub threads to exit
      for name,thread in subthreads.iteritems():
         logger.info('waiting for %s to join',name)
         thread.join()
         logger.info('%s has joined',name)


      while not self.queues['MPIService'].empty():
         logger.info('waiting for MPIService to send exit messages to Droid, sleep for %s',self.loop_timeout)
         time.sleep(self.loop_timeout)

      logger.info('Yoda is exiting')

   
   def read_config(self):

      if config_section in self.config:
         # read log level:
         if 'loglevel' in self.config[config_section]:
            self.loglevel = self.config[config_section]['loglevel']
            logger.info('%s loglevel: %s',config_section,self.loglevel)
            logger.setLevel(logging.getLevelName(self.loglevel))
         else:
            logger.warning('no "loglevel" in "%s" section of config file, keeping default',config_section)

         # read loop timeout:
         if 'loop_timeout' in self.config[config_section]:
            self.loop_timeout = int(self.config[config_section]['loop_timeout'])
            logger.info('%s loop_timeout: %s',config_section,self.loop_timeout)
         else:
            logger.warning('no "loop_timeout" in "%s" section of config file, keeping default %s',config_section,self.loop_timeout)

         # messenger_plugin_module
         if 'messenger_plugin_module' in self.config[config_section]:
            self.messenger_plugin_module = self.config[config_section]['messenger_plugin_module']
         else:
            raise Exception('Failed to retrieve "messenger_plugin_module" from config file section %s' % config_section)
      else:
         raise Exception('no %s section in the configuration' % config_section)
      

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


   def get_harvester_messenger(self):
      # try to import the module specified in the config
      # if it is not in the PYTHONPATH this will fail
      try:
         return importlib.import_module(self.messenger_plugin_module)
      except ImportError:
         logger.exception('Failed to import messenger_plugin: %s',self.messenger_plugin_module)
         raise

      