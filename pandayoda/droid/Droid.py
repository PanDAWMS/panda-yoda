import logging,os,time,socket,multiprocessing,platform
from pandayoda.common import SerialQueue,MessageTypes,MPIService,StatefulService
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

   STATES=[CREATED,REQUEST_JOB,WAITING_FOR_JOB,JOB_RECEIVED,MONITORING,TRANSFORM_EXITED,EXITING]

   def __init__(self,config):
      ''' config: the ConfigParser handle for yoda '''

      # call Thread constructor
      super(Droid,self).__init__()

      # configuration of Yoda
      self.config = config

      self.set_state(Droid.CREATED)


   # this runs when 'droid_instance.start()' is called
   def run(self):
      ''' this is the function called when the user runs droid_instance.start() '''
      
      if MPIService.rank == 0:
         logger.info('Droid Thread starting')
         logger.debug('config_section: %s',config_section)
      
         logger.info('Droid running on hostname: %s, %s',socket.gethostname(),platform.node())
         logger.info('Droid node has %d cpus',multiprocessing.cpu_count())
         logger.info('Droid uname: %s',','.join(platform.uname()))
         logger.info('Droid processor: %s',platform.processor())


      # read in config variables inside the thread run function to avoid
      # duplicating objects in memory across threads
      self.read_config()


      # create custom droid working directory
      droid_working_path = os.path.join(os.getcwd(),self.working_path.format(rank='%05d' % MPIService.rank))
      if not os.path.exists(droid_working_path):
         os.makedirs(droid_working_path,0775)

      # create queues for subthreads to send messages out
      queues = {}
      queues['JobComm']          = SerialQueue.SerialQueue()
      queues['Droid']            = SerialQueue.SerialQueue()
      queues['MPIService']       = SerialQueue.SerialQueue()

      # create forwarding map for MPIService
      forwarding_map = {
         MessageTypes.NEW_JOB: ['Droid','JobComm'],
         MessageTypes.NEW_EVENT_RANGES: ['JobComm'],
         MessageTypes.WALLCLOCK_EXPIRING: ['Droid'],
         MessageTypes.DROID_EXIT: ['Droid'],
      }

      # initialize MPI Service
      MPIService.mpiService.initialize(self.config,queues,forwarding_map,self.loop_timeout)
      MPIService.mpiService.set_queue_blocking()
      # wait until MPI Service has started up
      while not MPIService.mpiService.is_alive():
         time.sleep(1)
      
      # a dictionary of subthreads
      subthreads = {}

      # create job comm thread
      subthreads['JobComm']      = JobComm.JobComm(self.config,queues,droid_working_path,self.yampl_socket_name)
      subthreads['JobComm'].start()

      # begin in the REQUEST_JOB state
      self.set_state(Droid.REQUEST_JOB)
      
      logger.debug('cwd: %s',os.getcwd())

      # begin while loop 
      while not self.exit.isSet():
         state = self.get_state()
         logger.debug('droid start loop, state = %s',state)

         ###############################
         # check for queue message
         qmsg = None
         if not queues['Droid'].empty():
            qmsg = queues['Droid'].get()
            if 'type' not in qmsg:
               logger.error('queue message has not "type" key so disregarding it, msg = %s',qmsg)
               qmsg = None
            # if these are exit messages, then exit
            # otherwise continue
            logger.debug('received message: %s',qmsg)
            
            if qmsg['type'] == MessageTypes.DROID_EXIT:
               logger.debug('received exit message signaling stop')
               self.set_state(Droid.EXITING)
               self.stop()
               break
            elif qmsg['type'] == MessageTypes.WALLCLOCK_EXPIRING:
               logger.debug('received wallclock expiring message')
               # send message to other threads
               self.set_state(Droid.EXITING)
               self.stop()
               break
         elif self.get_state() == Droid.WAITING_FOR_JOB:
            try:
               logger.debug('blocking on message queue for %s',self.loop_timeout)
               qmsg = queues['Droid'].get(block=True,timeout=self.loop_timeout)
            except:
               logger.debug('no queue message')
         else:
            logger.debug('no message and not waiting for job')


         ###############################################
         # Request a job definition from Yoda
         #########
         if self.get_state() == Droid.REQUEST_JOB:
            self.request_job(queues)
            self.set_state(Droid.WAITING_FOR_JOB)
            # wait for MPIService to get message
            while not queues['MPIService'].empty(): time.sleep(1)
            # set MPIService to be MPI focused
            MPIService.mpiService.set_mpi_blocking()
         ###############################################
         # Waiting for a job definition from Yoda
         #########
         elif self.get_state() == Droid.WAITING_FOR_JOB:
            # check if message was received and was correct type
            logger.debug('waiting for job')
            if qmsg is not None:
               if qmsg['type'] == MessageTypes.NEW_JOB:
                  if 'job' in qmsg:
                     logger.debug('job received')
                     new_job_msg = qmsg
                     self.set_state(Droid.JOB_RECEIVED)
                  else:
                     logger.error('received NEW_JOB message but it did not contain a job description key, requesting new job, message = %s',qmsg)
                     self.set_state(Droid.REQUEST_JOB)
                  # set MPIService to be Queue focused
                  MPIService.mpiService.set_queue_blocking()
               else:
                  logger.debug('message type was not NEW_JOB: %s',qmsg)
            else:
               logger.debug('no message received')
        

         ###############################################
         # Job received
         #########
         elif self.get_state() == Droid.JOB_RECEIVED:
            # launch TransformManager to run job
            transform = TransformManager.TransformManager(new_job_msg['job'],
                                                          self.config,
                                                          queues,
                                                          droid_working_path,
                                                          os.getcwd(),
                                                          self.loop_timeout,
                                                          self.stdout_filename,
                                                          self.stderr_filename,
                                                          self.yampl_socket_name)
            transform.start()

            # transition to monitoring state
            self.set_state(Droid.MONITORING)

            # set MPIService to be balanced
            MPIService.mpiService.set_balanced()




         ###############################################
         # Monitoring a job
         #    in this state, Droid should monitor the 
         #  job subprocess and jobComm object to ensure
         #  they are running. It should also respond to
         #  queue messages if needed.
         #########
         elif self.get_state() == Droid.MONITORING:
            if transform.is_alive():
               # if JobComm is not alive, we have a problem
               if not subthreads['JobComm'].is_alive():
                  # check to see if JobComm exited properly or if there is no more work.
                  if subthreads['JobComm'].no_more_work():
                     # JobComm received no more work message from Harvester so exiting
                     self.set_state(Droid.EXITING)
                     self.stop()
                     break
                  else:
                     # log error
                     logger.error('JobComm thread exited, but the not sure why')
                     self.set_state(Droid.EXITING)
                     self.stop()
                     break
               else:
                  # sleep for a bit
                  logger.debug('monitoring transform, sleep for %s',self.loop_timeout)
                  time.sleep(self.loop_timeout)

            else:
               logger.debug('transform exited')
               self.set_state(Droid.TRANSFORM_EXITED)


         ###############################################
         # The transform exited
         #########
         elif self.get_state() == Droid.TRANSFORM_EXITED:

            # transform has exited
            if transform.in_state(transform.FINISHED):
               # log transform output
               logger.info('transform exited with return code: %s',transform.get_returncode())
            else:
               # log transform error
               logger.error('transform exited but is not in FINISHED state, returncode = %s, see output files for details = [%s,%s]',transform.get_returncode(),self.stderr_filename,self.stdout_filename)

            # send message to JobCommand that transform exited
            queues['JobComm'].put({'type':MessageTypes.TRANSFORM_EXITED})


            # if JobComm is still alive, request another job
            if subthreads['JobComm'].is_alive():
               self.set_state(Droid.REQUEST_JOB)
               # set MPIService to block on queue messages
               MPIService.mpiService.set_queue_blocking()
            else:
               # check to see if JobComm exited properly or if there is no more work.
               if subthreads['JobComm'].no_more_work():
                  # JobComm received no more work message from Harvester so exiting
                  self.stop()
                  break
               else:
                  # log error
                  logger.error('JobComm thread exited, but the not sure why')
                  self.stop()
                  break


      # send the exit signal to all subthreads
      logger.info('sending exit signal to subthreads')
      for name,thread in subthreads.iteritems():
         thread.stop()

      # wait for sub threads to exit
      logger.info('waiting for subthreads to join')
      for name,thread in subthreads.iteritems():
         thread.join()
         logger.info('%s has joined',name)

      # send yoda message that Droid has exited
      logger.info('droid notifying yoda that it has exited')
      queues['MPIService'].put({'type':MessageTypes.DROID_HAS_EXITED,'destination_rank':0})

      logger.info('droid exiting')


   def read_config(self):

      # read droid loop timeout:
      if self.config.has_option(config_section,'loop_timeout'):
         self.loop_timeout = self.config.getfloat(config_section,'loop_timeout')
      else:
         logger.error('must specify "loop_timeout" in "%s" section of config file',config_section)
         return
      if MPIService.rank == 1:
         logger.info('%s loop_timeout: %d',config_section,self.loop_timeout)


      # read droid yampl_socket_name:
      if self.config.has_option(config_section,'yampl_socket_name'):
         self.yampl_socket_name = self.config.get(config_section,'yampl_socket_name')
         self.yampl_socket_name = self.yampl_socket_name.format(rank=MPIService.rank)
      else:
         logger.error('must specify "yampl_socket_name" in "%s" section of config file',config_section)
         return
      if MPIService.rank == 1:
         logger.info('%s yampl_socket_name: %s',config_section,self.yampl_socket_name)

      # read droid subprocessStdout:
      if self.config.has_option(config_section,'subprocessStdout'):
         self.stdout_filename = self.config.get(config_section,'subprocessStdout')
      else:
         logger.error('must specify "subprocessStdout" in "%s" section of config file',config_section)
         return
      if MPIService.rank == 1:
         logger.info('%s subprocessStdout: %s',config_section,self.stdout_filename)

      # read droid subprocessStderr:
      if self.config.has_option(config_section,'subprocessStderr'):
         self.stderr_filename = self.config.get(config_section,'subprocessStderr')
      else:
         logger.error('must specify "subprocessStderr" in "%s" section of config file',config_section)
         return
      if MPIService.rank == 1:
         logger.info('%s subprocessStderr: %s',config_section,self.stderr_filename)

      # read droid working_path:
      if self.config.has_option(config_section,'working_path'):
         self.working_path = self.config.get(config_section,'working_path')
      else:
         logger.error('must specify "working_path" in "%s" section of config file',config_section)
         return
      if MPIService.rank == 1:
         logger.info('%s working_path: %s',config_section,self.working_path)

   def request_job(self,queues):
      qmsg = {'type': MessageTypes.REQUEST_JOB,'destination_rank':0}
      queues['MPIService'].put(qmsg)

   def get_queue_message(self,queues,block=False,timeout=None):
      try:
         qmsg = queues['Droid'].get(block=block,timeout=timeout)
         return qmsg
      except SerialQueue.Empty:
         logger.debug('no messages for Droid')
         return None



