#!/usr/bin/env python
import argparse
import logging
import os
import sys
import time
import traceback
from mpi4py import MPI
from pandayoda.yoda import Yoda
from pandayoda.droid import Droid
logger = logging.getLogger(__name__)


def yoda_droid(globalWorkingDir,localWorkingDir = None,
               outputDir = None,dumpEventOutputs = False, 
               inputJobFile = 'HPCJobs.json'):

   if localWorkingDir is None:
      localWorkingDir = globalWorkingDir

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
      logger.info("GlobalWorkingDir:    %s",globalWorkingDir)
      logger.info("LocalWorkingDir:     %s",localWorkingDir)
      logger.info("OutputDir:           %s",outputDir)
      logger.info("DumpEventOutputs:    %s",str(dumpEventOutputs))
      logger.info("inputJobFile:        %s",str(inputJobFile))



   # Create separate working directory for each rank
   curdir = os.path.abspath(localWorkingDir)
   wkdirname = "rank_%08i" % mpirank
   wkdir  = os.path.abspath(os.path.join(curdir,wkdirname))
   if not os.path.exists(wkdir):
      os.makedirs(wkdir)
   os.chdir(wkdir)

   logger.info("RANK %08i of %08i",mpirank,mpisize)

   if mpirank==0:
      try:
         yoda = Yoda.Yoda(globalWorkingDir, localWorkingDir, 
                           outputDir=outputDir, dumpEventOutputs=dumpEventOutputs,
                           inputJobFile=inputJobFile)
         yoda.start()
         
         droid = Droid.Droid(globalWorkingDir, localWorkingDir, outputDir=outputDir)
         droid.start()

         while yoda.isAlive() and droid.isAlive():
            logger.info("Rank %s: Yoda isAlive %s",mpirank, yoda.isAlive())
            logger.info("Rank %s: Droid isAlive %s",mpirank, droid.isAlive())
            time.sleep(300)
            
         logger.info("Rank %s: Yoda and Droid finished",mpirank)
      except:
         logger.exception("Rank %s: Yoda failed",mpirank)
         raise
     #os._exit(0)
   else:
      try:
         droid = Droid.Droid(globalWorkingDir, localWorkingDir, outputDir=outputDir)
         droid.start()
         
         while droid.isAlive():
            droid.join(timeout=300)
            logger.info("Rank %s: Droid isAlive %s",mpirank, droid.isAlive())
         logger.info("Rank %s: Droid finished",mpirank)
      except:
         logger.exception("Rank %s: Droid failed",mpirank)
         raise
   
def main():
   logging.basicConfig(level=logging.INFO,
         format='%(asctime)s|%(process)s|%(levelname)s|%(name)s|%(message)s',
         datefmt='%Y-%m-%d %H:%M:%S')
   logging.info('Start yoda_droid')
   oparser = argparse.ArgumentParser()
   oparser.add_argument('-g','--globalWorkingDir', dest="globalWorkingDir", help="Global share working directory",required=True)
   oparser.add_argument('-l','--localWorkingDir', dest="localWorkingDir", default=None, help="Local working directory. if it's not set, it will use global working directory")
   oparser.add_argument('-o','--outputDir', dest="outputDir", default=None, help="Copy output files to this directory")
   oparser.add_argument('-i','--inputJobFile', dest="inputJobFile", default='HPCJobs.json', help="Copy output files to this directory")
   oparser.add_argument('-d','--dumpEventOutputs', dest='dumpEventOutputs', default=False, action='store_true', help="Dump event output info to xml")
   oparser.add_argument('--debug', dest='debug', default=False, action='store_true', help="Set Logger to DEBUG")
   oparser.add_argument('--error', dest='error', default=False, action='store_true', help="Set Logger to ERROR")
   oparser.add_argument('--warning', dest='warning', default=False, action='store_true', help="Set Logger to ERROR")
   args = oparser.parse_args()

   if args.debug and not args.error and not args.warning:
      logger.setLevel(logging.DEBUG)
   elif not args.debug and args.error and not args.warning:
      logger.setLevel(logging.ERROR)
   elif not args.debug and not args.error and args.warning:
      logger.setLevel(logging.WARNING)
   elif not args.debug and not args.error and not args.warning:
      logger.setLevel(logging.INFO)
   else:
      logger.error('cannot set more than one of --debug, --error, or --warning.')
      oparser.print_help()
      return



   yoda_droid(args.globalWorkingDir,args.localWorkingDir,
              args.outputDir,args.dumpEventOutputs,args.inputJobFile)


if __name__ == "__main__":
   main()
