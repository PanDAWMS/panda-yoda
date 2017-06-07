import logging,threading,time,os
import WorkManager
from mpi4py import MPI
from pandayoda.common import yoda_droid_messenger as ydm
from pandayoda.common import SerialQueue,MessageTypes
logger = logging.getLogger(__name__)

config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]


class Yoda(threading.Thread):
   def __init__(self,config,start_time,wall_clock_limit):
      ''' config: configuration of Yoda
          start_time: the ealiest measure of the start time for Yoda/Droid
                        this is used to determine when to exit
                        it is expected to be output from time.time() so 
                        in seconds since the epoch
          wall_clock_limit: the time in minutes of the wall clock given to the local
                        batch scheduler. If a non-negative number, this value
                        determines when yoda will signal all Droids to kill
                        their running processes and exit
                        after which yoda will peform clean up actions
      '''
      # call Thread constructor
      super(Yoda,self).__init__()

      # configuration of Yoda
      self.config             = config

      # the ealiest measure of the start time for Yoda/Droid
      # this is used to determine when to exit
      # it is expected to be output from time.time() so 
      # in seconds since the epoch
      self.start_time         = start_time

      # the time in minutes of the wall clock given to the local
      # batch scheduler. If a non-negative number, this value
      # determines when yoda will signal all Droids to kill
      # their running processes and exit
      # after which yoda will peform clean up actions
      self.wall_clock_limit   = wall_clock_limit

      # this is used to trigger the thread exit
      self.exit               = threading.Event()

   def stop(self):
      ''' this function can be called by outside threads to cause the Yoda thread to exit'''
      self.exit.set()



   # this runs when 'yoda_instance.start()' is called
   def run(self):
      ''' this function is executed as the subthread. '''
      logger.info('Yoda Thread starting')
      logger.debug('config_section: %s',config_section)
      logger.debug('cwd: %s',os.getcwd())

      # read yoda loop timeout:
      if self.config.has_option(config_section,'loop_timeout'):
         loop_timeout = self.config.getfloat(config_section,'loop_timeout')
      else:
         logger.error('must specify "loop_timeout" in "%s" section of config file',config_section)
         return
      logger.info('%s loop_timeout: %d',config_section,loop_timeout)

      # read wallclock leadtime which sets how far from the end of 
      # the wallclock yoda should initiate droids to exit
      if self.config.has_option(config_section,'wallclock_expiring_leadtime'):
         wallclock_expiring_leadtime = self.config.getfloat(config_section,'wallclock_expiring_leadtime')
      else:
         logger.error('must specify "wallclock_expiring_leadtime" in "%s" section of config file',config_section)
         return
      logger.info('%s wallclock_expiring_leadtime: %d',config_section,wallclock_expiring_leadtime)
      # flag to keep track of whether we are inside teh wallclock expiring limit
      inside_wallclock_leadtime = False

      # create queues for subthreads to send messages out
      queues = {'WorkManager':SerialQueue.SerialQueue()}
      
      # a dictionary of subthreads
      subthreads = {}
      
      # create droid comm thread
      subthreads['WorkManager']  = WorkManager.WorkManager(self.config,queues)
      subthreads['WorkManager'].start()

      # handle for droid messages
      droid_msg_request = None

      # start message loop
      while not self.exit.isSet():
         logger.debug('start loop')

         # check for wallclock expiring
         if self.wall_clock_limit > 0:
            running_time = time.time() - self.start_time
            timeleft = self.wall_clock_limit - running_time
            if timeleft < wallclock_expiring_leadtime:
               logger.debug('time left %d is less than the leadtime %d, triggering exit.',timeleft,wallclock_expiring_leadtime)
               # set flag
               inside_wallclock_leadtime = True
               # time to send message to Droid ranks to exit
               for ranknum in range(MPI.COMM_WORLD.Get_size()):
                  ydm.send_droid_wallclock_expiring(ranknum).wait()
         else:
            logger.debug('no wallclock limit set, no exit will be triggered')
         
         # check for message from Droids
         if droid_msg_request is None:
            logger.debug('requesting MPI message from droids')
            droid_msg_request = ydm.get_droid_message_for_yoda()
         # check if message has been received
         else:
            logger.debug('checking MPI request for message from droids')
            status = MPI.Status()
            msg_received,msg = droid_msg_request.test(status=status)
            if msg_received:
               logger.debug('received MPI messsage %s',msg)
               if msg['type'] == MessageTypes.DROID_HAS_EXITED:
                  logger.debug(' droid rank %d has exited',status.Get_source())
               else:
                  logger.error(' could not interpret message from droid rank %d: %s',status.Get_source(),msg)

               # reset message
               droid_msg_request = None
            else:
               logger.debug('waiting for MPI message')



         # check the status of each subthread
         logger.debug('checking all threads still alive')
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
         time.sleep(loop_timeout)
      
      # send the exit signal to all droid ranks
      logger.info('sending exit signal to droid ranks')
      for ranknum in range(MPI.COMM_WORLD.Get_size()):
         ydm.send_droid_exit(ranknum).wait()

      # send the exit signal to all subthreads
      logger.info('sending exit signal to subthreads')
      for name,thread in subthreads.iteritems():
         thread.stop()

      # wait for sub threads to exit
      logger.info('waiting for subthreads to join')
      for name,thread in subthreads.iteritems():
         thread.join()

      logger.info('Yoda is exiting')


      




      