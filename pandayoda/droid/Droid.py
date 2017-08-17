import logging,threading,os,time,socket,multiprocessing,platform
from pandayoda.common import yoda_droid_messenger as ydm,SerialQueue,MessageTypes,MPIService
from pandayoda.droid import JobManager,JobComm
logger = logging.getLogger(__name__)


config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]

class Droid(threading.Thread):
   ''' 1 Droid runs per node of a parallel job and launches the AthenaMP process '''
   def __init__(self,config):
      ''' config: the ConfigParser handle for yoda '''

      # call Thread constructor
      super(Droid,self).__init__()

      # configuration of Yoda
      self.config                = config

      # this is used to trigger the thread exit
      self.exit                  = threading.Event()

   def stop(self):
      ''' this function can be called by outside threads to cause the Yoda thread to exit'''
      self.exit.set()


   # this runs when 'droid_instance.start()' is called
   def run(self):
      ''' this is the function called when the user runs droid_instance.start() '''
      
      if self.rank == 0:
         logger.info('Droid Thread starting')
         logger.debug('config_section: %s',config_section)
      
      logger.info('Droid running on hostname: %s, %s',socket.gethostname(),platform.node())
      logger.info('Droid node has %d cpus',multiprocessing.cpu_count())
      logger.info('Droid uname: %s',','.join(platform.uname()))
      logger.info('Droid processor: %s',platform.processor())

      # read droid loop timeout:
      if self.config.has_option(config_section,'loop_timeout'):
         loop_timeout = self.config.getfloat(config_section,'loop_timeout')
      else:
         logger.error('must specify "loop_timeout" in "%s" section of config file',config_section)
         return
      if self.rank == 0:
         logger.info('%s loop_timeout: %d',config_section,loop_timeout)

      # place holder for exit message in case there is an error
      exit_msg = ''

      # change to custom droid working directory
      droid_working_path = os.path.join(os.getcwd(),"droid_rank_%05i" % self.rank)
      if not os.path.exists(droid_working_path):
         os.makedirs(droid_working_path)

      # create queues for subthreads to send messages out
      queues = {}
      queues['JobManager']       = SerialQueue.SerialQueue()
      queues['JobComm']          = SerialQueue.SerialQueue()
      queues['Droid']            = SerialQueue.SerialQueue()
      queues['MPIService']       = SerialQueue.SerialQueue()

      # create forwarding map for MPIService
      forwarding_map = {
         MessageTypes.NEW_JOB: 'JobManager',
         MessageTypes.NEW_EVENT_RANGES: 'JobComm',
         MessageTypes.WALLCLOCK_EXPIRING: 'Droid',
      }

      # initialize MPI Service
      MPIService.mpiService.initialize(queues,forwarding_map)
      
      # a dictionary of subthreads
      subthreads = {}
      
      # create job manager thread
      subthreads['JobManager']   = JobManager.JobManager(self.config,queues,droid_working_path)
      subthreads['JobManager'].start()

      # create job comm thread
      subthreads['JobComm']      = JobComm.JobComm(self.config,queues,droid_working_path)
      subthreads['JobComm'].start()

      # begin while loop to monitor subthreads
      while not self.exit.isSet():
         logger.debug('droid start loop')
         logger.debug('cwd: %s',os.getcwd())

         logger.debug(' MPI.Query_thread(): %s',MPI.Query_thread())
         logger.debug(' MPI.THREAD_MULTIPLE: %s',MPI.THREAD_MULTIPLE)

         # check for queue message
         try:
            qmsg = queues['Droid'].get(block=False)
            logger.debug('received message: %s',qmsg)
            if qmsg['type'] == MessageTypes.DROID_EXIT:
               logger.debug('received exit message signaling stop')
               self.stop()
               break
            if qmsg['type'] == MessageTypes.WALLCLOCK_EXPIRING:
               logger.debug('received wallclock expiring message')
               # send message to other threads\
               self.stop()
               break
            else:
               logger.error('failed to parse message')
         except SerialQueue.Empty():
            logger.debug('no messages for Droid')


         # check the status of each subthread
         logger.debug('checking subthreads')
         keys = subthreads.keys()
         number_running = 0
         for name in keys:
            logger.debug('checking thread %s',name)
            thread = subthreads[name]
            # if the thread is not alive, throw an error
            if not thread.isAlive():
               logger.warning('%s is no longer running.',name)
               if name == 'JobManager' and thread.no_more_jobs.get():
                  logger.debug('JobManager reports no more jobs so it exited.')
               elif name == 'JobComm' and thread.all_work_done.get():
                  logger.debug('JobComm reports no more work so it exited.')
               else:
                  exit_msg += 'is no longer running.' % name
                  self.stop()
            else:
               number_running += 1
         logger.debug('threads running %s',number_running)

         # if no process are running, exit
         if number_running == 0:
            logger.info('no more processes running so exiting.')
            self.stop()

         if len(subthreads) == 0:
            logger.info('no subthreads remaining, exiting')
            exit_msg += ' no subthreads remaining, exiting.'
            break

         logger.debug('sleeping %s',loop_timeout)
         time.sleep(loop_timeout)

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
      ydm.send_droid_has_exited(exit_msg)

      logger.info('droid exiting')




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


      droid = Droid(config)

      droid.start()

      req = None
      jobn = 0
      evtrgn = 0
      while droid.join(timeout=5) is None:
         if not droid.isAlive(): break

         if req is None:
            # get message from droid
            req = ydm.get_droid_message()

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
                  ydm.send_droid_new_job(job,source_rank)
               elif data['type'] == MessageTypes.REQUEST_EVENT_RANGES:
                  logger.info('received request for event range from rank %d',source_rank)
                  jobwise_evtrgs = serializer.deserialize(open(args.jobWorkingDir + '/JobsEventRanges.json').read())
                  evtrgs = jobwise_evtrgs[jobwise_evtrgs.keys()[0]]
                  if evtrgn < len(evtrgs):
                     logger.info('sending event range to droid rank: %d',source_rank)
                     ydm.send_droid_new_eventranges([evtrgs[evtrgn]],source_rank)
                     evtrgn += 1
                  else:
                     logger.info('sending no more event range message to droid rank: %d',source_rank)
                     ydm.send_droid_no_eventranges_left(source_rank)
                     evtrgn += 1


            else:
               continue
   except:
      droid.stop()

      droid.join()

   logger.info('Droid test exiting')


      