import logging,os,time,socket,multiprocessing,platform,Queue
from pandayoda.common import MessageTypes,MPIService,StatefulService
from pandayoda.droid import TransformManager,JobComm
logger = logging.getLogger(__name__)


config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]


class Droid(StatefulService.StatefulService):
   ''' 1 Droid runs per node of a parallel job and launches the AthenaMP process '''

   CREATED           = 'CREATED'
   REQUEST_JOB       = 'REQUEST_JOB'
   WAITING_FOR_JOB   = 'WAITING_FOR_JOB'
   JOB_RECEIVED      = 'JOB_RECEIVED'
   MONITORING        = 'MONITORING'
   TRANSFORM_EXITED  = 'TRANSFORM_EXITED'
   EXITING           = 'EXITING'
   EXITED            = 'EXITED'

   STATES = [CREATED,REQUEST_JOB,WAITING_FOR_JOB,JOB_RECEIVED,MONITORING,TRANSFORM_EXITED,EXITING,EXITED]

   def __init__(self,queues,config,rank):
      ''' config: the ConfigParser handle for yoda '''

      # call Thread constructor
      super(Droid,self).__init__()

      # message queues
      self.queues = queues

      # configuration of Yoda
      self.config = config

      # rank number
      self.rank   = rank

      self.set_state(Droid.CREATED)


   # this runs when 'droid_instance.start()' is called
   def run(self):
      ''' this is the function called when the user runs droid_instance.start() '''

      try:
         self.subrun()
      except Exception:
         logger.exception('Droid failed with uncaught exception')
         from mpi4py import MPI
         MPI.COMM_WORLD.Abort()
         logger.info('Droid is exiting')


   def subrun(self):
      ''' this function is the business logic, but wrapped in exception '''
      
      logger.info('Droid Thread starting')
      logger.debug('config_section:             %s',config_section)
   
      logger.info('Droid running on hostname:   %s',socket.gethostname())
      logger.info('Droid node has               %d cpus',multiprocessing.cpu_count())
      logger.info('Droid uname:                 %s',','.join(platform.uname()))
      logger.info('Droid processor:             %s',platform.processor())


      # read in config variables inside the thread run function to avoid
      # duplicating objects in memory across threads
      self.read_config()


      # create custom droid working directory
      droid_working_path = os.path.join(os.getcwd(),self.working_path)
      try:
         os.makedirs(droid_working_path,0775)
      except OSError,e:
         if 'File exists' in str(e):
            pass
         else:
            logger.exception('exception raised while trying to mkdirs %s',droid_working_path)
            raise
      except Exception,e:
         logger.exception('exception raised while trying to mkdirs %s',droid_working_path)
         raise

      
      # a dictionary of subthreads
      self.subthreads = {}
      
      # create job comm thread
      self.subthreads['JobComm']      = JobComm.JobComm(self.config,self.queues,droid_working_path,self.yampl_socket_name)
      self.subthreads['JobComm'].start()

      # begin in the REQUEST_JOB state
      self.set_state(Droid.REQUEST_JOB)
      
      logger.debug('cwd: %s',os.getcwd())

      # begin while loop
      while not self.exit.is_set():
         state = self.get_state()
         logger.debug('droid start loop, state = %s',state)

         ###############################################
         # Request a job definition from Yoda
         #########
         if self.get_state() == Droid.REQUEST_JOB:
            logger.info('requesting a job')
            # request a job, place message on queue to MPIService
            self.request_job(self.queues)

            # wait for MPIService to get message
            while not self.queues['MPIService'].empty():
               time.sleep(1)

            # set MPIService to be MPI focused, it should currently be message queue focused
            # but right after the job request is sent, it will wake up, and convert to MPI focused
            # MPIService.mpiService.set_mpi_blocking()

            # change state
            self.set_state(Droid.WAITING_FOR_JOB)


         ###############################################
         # Waiting for a job definition from Yoda
         #########
         elif self.get_state() == Droid.WAITING_FOR_JOB:
            # check if message was received and was correct type
            
            try:
               logger.info('waiting for job, blocking on queue for %s',self.loop_timeout)
               qmsg = self.queues['Droid'].get(block=True,timeout=self.loop_timeout)
            except Queue.Empty:
               logger.debug('no message on queue')
            else:
               if qmsg['type'] == MessageTypes.NEW_JOB:
                  if 'job' in qmsg:
                     logger.debug('job received')
                     new_job_msg = qmsg
                     self.set_state(Droid.JOB_RECEIVED)
                  else:
                     logger.error('received NEW_JOB message but it did not contain a job description key, requesting new job, message = %s',qmsg)
                     self.set_state(Droid.REQUEST_JOB)
                  # set MPIService to be Queue focused
                  # MPIService.mpiService.set_queue_blocking()
               else:
                  logger.error('message type was not NEW_JOB, faied parsing: %s',qmsg)
            

         ###############################################
         # Job received
         #########
         elif self.get_state() == Droid.JOB_RECEIVED:
            logger.info('job received, launching transform')
            # launch TransformManager to run job
            self.subthreads['transform'] = TransformManager.TransformManager(new_job_msg['job'],
                                                                             self.config,
                                                                             self.queues,
                                                                             droid_working_path,
                                                                             os.getcwd(),
                                                                             self.yampl_socket_name)
            self.subthreads['transform'].start()

            # transition to monitoring state
            self.set_state(Droid.MONITORING)

            # set MPIService to be balanced
            # MPIService.mpiService.set_balanced()




         ###############################################
         # Monitoring a job
         #    in this state, Droid should monitor the
         #  job subprocess and jobComm object to ensure
         #  they are running. It should also respond to
         #  queue messages if needed.
         #########
         elif self.get_state() == Droid.MONITORING:

            if self.subthreads['transform'].is_alive():
               # if JobComm is not alive, we have a problem
               if not self.subthreads['JobComm'].is_alive():
                  # check to see if JobComm exited properly or if there is no more work.
                  if self.subthreads['JobComm'].no_more_work():
                     logger.info('no more work, triggering exit')
                     # JobComm received no more work message from Harvester so exiting
                     self.stop()
                  else:
                     # log error
                     logger.error('JobComm thread exited, but the not sure why')
                     self.stop()
               else:
                  # sleep for a bit while blocking on incoming messages
                  logger.info('transform running, block for %s on message queue',self.loop_timeout)
                  try:
                     qmsg = self.queues['Droid'].get(block=True,timeout=self.loop_timeout)
                     if qmsg['type'] == MessageTypes.WALLCLOCK_EXPIRING:
                        logger.info('received WALLCLOCK_EXPIRING message from Yoda, exiting.')
                        # stop Droid and it will kill all subthreads,etc.
                        self.stop()
                     elif qmsg['type'] == MessageTypes.NO_MORE_EVENT_RANGES:
                        logger.info('received NO_MORE_EVENT_RANGES, exiting')
                        self.stop()
                     else:
                        logger.error('received unexpected message: %s',qmsg)
                  except Queue.Empty:
                     logger.debug('no message received while in monitoring wait')


            else:
               logger.info('transform exited')
               self.set_state(Droid.TRANSFORM_EXITED)


         ###############################################
         # The transform exited
         #########
         elif self.get_state() == Droid.TRANSFORM_EXITED:
            # join the process
            self.subthreads['transform'].join()

            # transform has exited
            if self.subthreads['transform'].in_state(self.subthreads['transform'].FINISHED):
               # log transform output
               logger.info('transform exited with return code: %s',self.subthreads['transform'].get_returncode())
            else:
               # log transform error
               logger.error('transform exited but is not in FINISHED state, returncode = %s',
                            self.subthreads['transform'].get_returncode())

            # trigger exit
            self.stop()

            # in the future you might imagine requesting another job here

      # set exit state
      self.set_state(self.EXITING)

      # send the exit signal to all subthreads
      for name,thread in self.subthreads.iteritems():
         logger.info('sending exit signal to %s',name)
      
         thread.stop()

      # wait for sub threads to exit
      for name,thread in self.subthreads.iteritems():
         logger.info('waiting for %s to join',name)
         thread.join()
         logger.info('%s has joined',name)

      # send yoda message that Droid has exited
      logger.info('droid notifying yoda that it has exited')
      self.queues['MPIService'].put({'type':MessageTypes.DROID_HAS_EXITED,'destination_rank':0})

      self.set_state(self.EXITED)
      logger.info('droid exited')


   def read_config(self):

      if config_section in self.config:
         # read log level:
         if 'loglevel' in self.config[config_section]:
            self.loglevel = self.config[config_section]['loglevel']
            logger.info('%s loglevel: %s',config_section,self.loglevel)
            logger.setLevel(logging.getLevelName(self.loglevel))
         else:
            logger.warning('no "loglevel" in "%s" section of config file, keeping default',config_section)

         # read droid loop timeout:
         if 'loop_timeout' in self.config[config_section]:
            self.loop_timeout = int(self.config[config_section]['loop_timeout'])
            logger.info('%s loop_timeout: %s',config_section,self.loop_timeout)
         else:
            logger.warning('no "loop_timeout" in "%s" section of config file, keeping default %s',config_section,self.loop_timeout)


         # read droid yampl_socket_name:
         if 'yampl_socket_name' in self.config[config_section]:
            self.yampl_socket_name = self.config[config_section]['yampl_socket_name']
            self.yampl_socket_name = self.yampl_socket_name.format(rank=MPIService.mpirank.get())
            logger.info('%s yampl_socket_name: %s',config_section,self.yampl_socket_name)
         else:
            self.yampl_socket_name = 'EventServiceDroid_r{rank}'.format(rank=self.rank)
            logger.warning('no "yampl_socket_name" in "%s" section of config file, keeping default %s',config_section,self.yampl_socket_name)


         # read droid working_path:
         if 'working_path' in self.config[config_section]:
            self.working_path = self.config[config_section]['working_path']
            self.working_path = self.working_path.format(rank=MPIService.mpirank.get())
            logger.info('%s working_path: %s',config_section,self.working_path)
         else:
            self.working_path = 'droid_rank_{rank}'.format(rank=self.rank)
            logger.warning('no "working_path" in "%s" section of config file, keeping default %s',config_section,self.working_path)

      else:
         raise Exception('no %s section in the configuration' % config_section)

   def request_job(self,queues):
      qmsg = {'type': MessageTypes.REQUEST_JOB,'destination_rank':0}
      queues['MPIService'].put(qmsg)

   def get_queue_message(self,queues,block=False,timeout=None):
      try:
         qmsg = queues['Droid'].get(block=block,timeout=timeout)
         return qmsg
      except Queue.Empty:
         logger.debug('no messages for Droid')
         return None
