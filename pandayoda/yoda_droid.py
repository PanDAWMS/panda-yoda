#!/usr/bin/env python
import argparse,logging,os,sys,time,Queue,importlib
from mpi4py import MPI
from pandayoda import Yoda,Droid
logger = logging.getLogger(__name__)


def yoda_droid(jobWorkingDir,
               outputDir               = None,
               dumpEventOutputs        = False, 
               jobSpecFile             = 'HPCJobs.json', 
               eventRangesFile         = 'JobsEventRanges.json',
               workerAttributesFile    = 'worker_attributes.json',
               jobReportFile           = 'jobReport.json',
               eventStatusDumpJsonFile = 'event_status.dump.json',
               eventStatusDumpXmlFile  = '_event_status.dump',
               jobRequestFile          = 'worker_requestjob.json',
               eventRequestFile        = 'worker_requestevents.json',
               updateEventsFile        = 'worker_updateevents.json',
               xmlPoolCatalogFile      = 'PoolFileCatalog_H.xml',
               loop_timeout            = 300,
               config                  = 'yoda.cfg',
               messenger_plugin        = 'shared_file_messenger',
              ):

   
   # get MPI world info
   try:
      comm = MPI.COMM_WORLD
      mpirank = comm.Get_rank()
      mpisize = comm.Get_size()
      logger.debug(' rank %10i of %10i',mpirank,mpisize)
   except:
      logger.error('Exception retrieving MPI rank information')
      raise

   if mpirank == 0:
      logger.info("JobWorkingDir:               %s",jobWorkingDir)
      logger.info("OutputDir:                   %s",outputDir)
      logger.info("DumpEventOutputs:            %s",str(dumpEventOutputs))
      logger.info("jobSpecFile:                 %s",str(jobSpecFile))
      logger.info("eventRangesFile:             %s",str(eventRangesFile))
      logger.info("workerAttributesFile:        %s",str(workerAttributesFile))
      logger.info("jobReportFile:               %s",str(jobReportFile))
      logger.info("eventStatusDumpJsonFile:     %s",str(eventStatusDumpJsonFile))
      logger.info("eventStatusDumpXmlFile:      %s",str(eventStatusDumpXmlFile))
      logger.info("jobRequestFile:              %s",str(jobRequestFile))
      logger.info("eventRequestFile:            %s",str(eventRequestFile))
      logger.info("updateEventsFile:            %s",str(updateEventsFile))
      logger.info("xmlPoolCatalogFile:          %s",str(xmlPoolCatalogFile))
      logger.info("loop_timeout:                %s",str(loop_timeout))
      logger.info("config:                      %s",str(config))
      logger.info("messenger_plugin:            %s",str(messenger_plugin))


   # Create separate working directory for each rank
   os.chdir(jobWorkingDir)
   curdir = os.path.abspath(jobWorkingDir)
   wkdirname = "rank_%05i" % mpirank
   wkdir  = os.path.abspath(os.path.join(curdir,wkdirname))
   if not os.path.exists(wkdir):
      os.makedirs(wkdir)
   #os.chdir(wkdir)

   # create the messenger, which is a plugin
   # try to import the module specified by the parameter 'messenger_plugin'
   # if it is not in the PYTHONPATH this will fail
   try:
      messenger = importlib.import_module(messenger_plugin)
   except ImportError:
      logger.exception('Failed to import messenger_plugin: %s',messenger_plugin)
      raise
   # now seteup messenger
   messenger.setup(config)

   logger.info('Rank %08i of %08i',mpirank,mpisize)
   logger.info('Rank %s: current working directory: %s',mpirank,wkdir)

   yoda = None
   # if you are rank 0, start the yoda thread
   if mpirank==0:
      try:
         yoda = Yoda.Yoda(jobWorkingDir,
                          messenger,
                          outputDir)
         yoda.start()
      except:
         logger.exception('Rank %s: failed to start Yoda.',mpirank)
         raise
   # all ranks start Droid threads
   try:
      droid = Droid.Droid(jobWorkingDir,messenger,outputDir)
      droid.start()
   except:
      logger.exception('Rank %s: failed to start Droid',mpirank)
      raise

   # loop until droid and/or yoda exit
   while True:
      logger.debug('in loop')
      time.sleep(loop_timeout)
      if not droid.isAlive():
         logger.info('Rank %s: droid has finished',mpirank)
         break
      if yoda:
         if not yoda.isAlive():
            logger.info('Rank %s: yoda has finished',mpirank)
            break
   
def main():
   logging.basicConfig(level=logging.INFO,
         format='%(asctime)s|%(process)s|%(levelname)s|%(name)s|%(message)s',
         datefmt='%Y-%m-%d %H:%M:%S')
   logging.info('Start yoda_droid')
   oparser = argparse.ArgumentParser()
   oparser.add_argument('-l','--jobWorkingDir', dest="jobWorkingDir", default=None, help="Job's working directory.",required=True)
   oparser.add_argument('-o','--outputDir', dest="outputDir", default=None, help="Copy output files to this directory")
   oparser.add_argument('--jobSpecFile', dest="jobSpecFile", default='HPCJobs.json', help="File containing the job definitions, written by Harvester")
   oparser.add_argument('--eventRangesFile', dest="eventRangesFile", default='JobsEventRanges.json', help="File containing the job event ranges to process, written by Harvester")
   oparser.add_argument('--workerAttributesFile', dest='workerAttributesFile', default='worker_attributes.json', help="worker attributes")
   oparser.add_argument('--jobReportFile', dest='jobReportFile', default='jobReport.json', help="job report")
   oparser.add_argument('--eventStatusDumpJsonFile', dest='eventStatusDumpJsonFile', default='event_status.dump.json', help="event status dump file in json")
   oparser.add_argument('--eventStatusDumpXmlFile', dest='eventStatusDumpXmlFile', default='_event_status.dump', help="event status dump file in xml")
   oparser.add_argument('--jobRequestFile', dest='jobRequestFile', default='worker_requestjob.json', help="job request")
   oparser.add_argument('--eventRequestFile', dest='eventRequestFile', default='worker_requestevents.json', help="event request")
   oparser.add_argument('--updateEventsFile', dest='updateEventsFile', default='worker_updateevents.json', help="update events")
   oparser.add_argument('--xmlPoolCatalogFile', dest='xmlPoolCatalogFile', default='PoolFileCatalog_H.xml', help="PFC for input files")
   oparser.add_argument('--dumpEventOutputs', dest='dumpEventOutputs', default=False, action='store_true', help="Dump event output info to xml")
   oparser.add_argument('--debug', dest='debug', default=False, action='store_true', help="Set Logger to DEBUG")
   oparser.add_argument('--error', dest='error', default=False, action='store_true', help="Set Logger to ERROR")
   oparser.add_argument('--warning', dest='warning', default=False, action='store_true', help="Set Logger to ERROR")
   oparser.add_argument('--loop-timeout', dest='loop_timeout', type=int,default=300, help="How often to run the loop checking that Yoda/Droid still alive.")
   oparser.add_argument('--yoda-config', dest='yoda_config', help="The Yoda Config file where some configuration information is set.",default='yoda.cfg')
   args = oparser.parse_args()

   if args.debug and not args.error and not args.warning:
      # remove existing root handlers and reconfigure with DEBUG
      for h in logging.root.handlers:
         logging.root.removeHandler(h)
      logging.basicConfig(level=logging.DEBUG,
         format='%(asctime)s|%(process)s|%(levelname)s|%(name)s|%(message)s',
         datefmt='%Y-%m-%d %H:%M:%S')
      logger.setLevel(logging.DEBUG)
   elif not args.debug and args.error and not args.warning:
      # remove existing root handlers and reconfigure with ERROR
      for h in logging.root.handlers:
         logging.root.removeHandler(h)
      logging.basicConfig(level=logging.ERROR,
         format='%(asctime)s|%(process)s|%(levelname)s|%(name)s|%(message)s',
         datefmt='%Y-%m-%d %H:%M:%S')
      logger.setLevel(logging.ERROR)
   elif not args.debug and not args.error and args.warning:
      # remove existing root handlers and reconfigure with WARNING
      for h in logging.root.handlers:
         logging.root.removeHandler(h)
      logging.basicConfig(level=logging.WARNING,
         format='%(asctime)s|%(process)s|%(levelname)s|%(name)s|%(message)s',
         datefmt='%Y-%m-%d %H:%M:%S')
      logger.setLevel(logging.WARNING)
   




   yoda_droid(
              args.jobWorkingDir,
              args.outputDir,
              args.dumpEventOutputs,
              args.jobSpecFile,
              args.eventRangesFile,
              args.workerAttributesFile,
              args.jobReportFile,
              args.eventStatusDumpJsonFile,
              args.eventStatusDumpXmlFile,
              args.jobRequestFile,
              args.eventRequestFile,
              args.updateEventsFile,
              args.xmlPoolCatalogFile,
              args.loop_timeout,
              args.yoda_config)


if __name__ == "__main__":
   main()
