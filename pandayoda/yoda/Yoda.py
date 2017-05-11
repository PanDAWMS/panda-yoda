import logging,threading
import WorkManager
from pandayoda.common import yoda_droid_messenger as ydmess
from mpi4py import MPI
logger = logging.getLogger(__name__)


class Yoda(threading.Thread):
   def __init__(self,jobWorkingDir,messenger,config
                     outputDir = None,
               ):
      # call Thread constructor
      super(Yoda,self).__init__()

      # working directory for the jobs
      self.jobWorkingDir   = jobWorkingDir

      # output directory where event output is placed
      self.outputDir       = outputDir

      # messenger for talking with Harvester
      self.messenger       = messenger

      # configuration of Yoda
      self.config          = config

      # current rank
      self.rank            = MPI.COMM_WORLD.Get_rank()

      # size of the MPI world
      self.worldsize       = MPI.COMM_WORLD.Get_size()


   # this runs when 'yoda_instance.start()' is called
   def run(self):

      logger.info('Yoda Thread starting')

      # create the work manager
      workManager = WorkManager.WorkManager()

      # get the jobs from Harvester
      pandajobs = self.messenger.get_pandajobs()
      # add these jobs to the work manager
      self.workManager.add_pandajobs(pandajobs)

      logger.info('Yoda received %i jobs from Harvester',len(pandajobs))

      # get event ranges from Harvester
      eventranges = self.messenger.get_eventranges()
      # add these event ranges to the work manager
      self.workManager.add_eventranges(eventranges)

      # start message loop
      while True:

         # check for message from Droids
         message,status = ydmess.get_droid_message()

         # Droid rank is requesting work
         # send it panda jobs and event ranges to go with that panda job
         if ydmess.REQUEST_JOB_MESSAGE in message:
            logger.debug('received request for job from rank %05i',status.Get_source())
            # get work from work manager
            try:
               pjob,evtrng = workManager.get_work()
            except WorkManager.NoWorkLeft:
               # there is no work left, so tell Droid.
               ydmess.send_droid_no_work_left(status.Get_source())


      




      