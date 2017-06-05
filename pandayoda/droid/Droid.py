import logging,threading,os,time
from mpi4py import MPI
from pandayoda.common import yoda_droid_messenger as ydm,SerialQueue
from pandayoda.droid import JobManager,JobComm,FileManager
logger = logging.getLogger(__name__)


config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]

class Droid(threading.Thread):
   ''' 1 Droid runs per node of a parallel job and launches the AthenaMP process '''
   def __init__(self,config):
      ''' config: the ConfigParser handle for yoda '''

      # call Thread constructor
      super(Droid,self).__init__()

      # configuration of Yoda
      self.config          = config

      # get current rank
      self.rank                  = MPI.COMM_WORLD.Get_rank()

      # the prelog is just a string to attach before each log message
      self.prelog                = 'Rank %03i:' % self.rank

      # this is used to trigger the thread exit
      self.exit = threading.Event()

   def stop(self):
      ''' this function can be called by outside threads to cause the Yoda thread to exit'''
      self.exit.set()


   # this runs when 'droid_instance.start()' is called
   def run(self):
      ''' this is the function called when the user runs droid_instance.start() '''
      if self.rank == 0:
         logger.info('%s Droid Thread starting',self.prelog)
         logger.debug('%s config_section: %s',self.prelog,config_section)

      # read droid loop timeout:
      if self.config.has_option(config_section,'loop_timeout'):
         loop_timeout = self.config.getfloat(config_section,'loop_timeout')
      else:
         logger.error('%s must specify "loop_timeout" in "%s" section of config file',self.prelog,config_section)
         return
      if self.rank == 0:
         logger.info('%s %s loop_timeout: %d',self.prelog,config_section,loop_timeout)

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
      queues['FileManager']      = SerialQueue.SerialQueue()
      queues['Droid']            = SerialQueue.SerialQueue()
      
      # a dictionary of subthreads
      subthreads = {}
      
      # create job manager thread
      subthreads['JobManager']   = JobManager.JobManager(self.config,queues,droid_working_path)
      subthreads['JobManager'].start()

      # create job comm thread
      subthreads['JobComm']      = JobComm.JobComm(self.config,queues)
      subthreads['JobComm'].start()

      # create file manager thread
      subthreads['FileManager']  = FileManager.FileManager(self.config,queues,os.getcwd())
      subthreads['FileManager'].start()

      yoda_recv = None

      # begin while loop to monitor subthreads
      while not self.exit.isSet():
         logger.debug('%s droid start loop',self.prelog)
         logger.debug('%s cwd: %s',self.prelog,os.getcwd())

         # check for exit message
         if yoda_recv is None:
            logger.debug('%s requesting message from yoda',self.prelog)
            yoda_recv = ydm.recv_yoda_message()
         else:
            logger.debug('%s testing for message from yoda',self.prelog)
            msg_received,msg = yoda_recv.test()
            if msg_received:
               logger.debug('%s received message from yoda: %s',self.prelog,msg)
               if msg['type'] == MessageTypes.DROID_EXIT:
                  logger.debug('%s received exit message signaling stop',self.prelog)
                  self.stop()
                  continue
               else:
                  logger.error('%s failed to parse message from Yoda: %s',self.prelog,msg)

               # reset message
               yoda_recv = None


         # check the status of each subthread
         keys = subthreads.keys()
         for name in keys:
            thread = subthreads[name]
            # if the thread is not alive, throw an error
            if not thread.isAlive():
               logger.warning('%s %s is no longer running.',self.prelog,name)
               exit_msg += '%s is no longer running.' % name
               self.stop()
            #else:
               #logger.debug('%s %s is running.',self.prelog,name)

         if len(subthreads) == 0:
            logger.info('%s no subthreads remaining, exiting',self.prelog)
            exit_msg += ' no subthreads remaining, exiting.'
            break
         time.sleep(loop_timeout)

      # send the exit signal to all subthreads
      logger.info('%s sending exit signal to subthreads',self.prelog)
      for name,thread in subthreads.iteritems():
         thread.stop()

      # wait for sub threads to exit
      logger.info('%s waiting for subthreads to join',self.prelog)
      for name,thread in subthreads.iteritems():
         thread.join()

      # send yoda message that Droid has exited
      ydm.send_droid_has_exited(exit_msg)

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


      