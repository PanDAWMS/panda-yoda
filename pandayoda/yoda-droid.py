#!/usr/bin/env python
import argparse
import logging
import os
import sys
import time
import traceback
import ConfigParser
from mpi4py import MPI
from pandayoda.yoda import Yoda
from pandayoda.droid import Droid
logger = logging.getLogger(__name__)


def main(globalWorkDir, localWorkDir, outputDir=None, dumpEventOutputs=True):


   logger.info("GlobalWorkDir:    %s",globalWorkDir)
   logger.info("LocalWorkDir:     %s",localWorkDir)
   logger.info("OutputDir:        %s",outputDir)
   logger.info("DumpEventOutputs: %s",str(dumpEventOutputs))

   # parse config file
   configFile = os.path.join(os.path.realpath(__file__),'yoda.cfg')
   config = ConfigParser.RawConfigParser()
   config.read(configFile)

   inputJobFile = config.get('yoda','InputJobFile')
   logger.info("InputJobFile:  %s",inputJobFile)


   # get MPI world info
   try:
      comm = MPI.COMM_WORLD
      mpirank = comm.Get_rank()
      mpisize = comm.Get_size()
   except:
      logger.error('Exception retrieving MPI rank information')
      raise

   # Create separate working directory for each rank
   curdir = os.path.abspath(localWorkDir)
   wkdirname = "rank_%08i" % mpirank
   wkdir  = os.path.abspath(os.path.join(curdir,wkdirname))
   if not os.path.exists(wkdir):
      os.makedirs(wkdir)
   os.chdir(wkdir)

   logger.info("RANK %08i of %08i",mpirank,mpisize)

   if mpirank==0:
      try:
         yoda = Yoda.Yoda(globalWorkDir, localWorkDir, 
                           outputDir=outputDir, dumpEventOutputs=dumpEventOutputs,
                           inputJobFile=inputJobFile)
         yoda.start()
         
         droid = Droid.Droid(globalWorkDir, localWorkDir, outputDir=outputDir)
         droid.start()

         while yoda.isAlive() or droid.isAlive():
            logger.info("Rank %s: Yoda isAlive %s",mpirank, yoda.isAlive())
            logger.info("Rank %s: Droid isAlive %s",mpirank, droid.isAlive())
            time.sleep(300)
            
         logger.info("Rank %s: Yoda finished",mpirank)
      except:
         logger.exception("Rank %s: Yoda failed",mpirank)
         raise
     #os._exit(0)
   else:
      try:
         status = 0
         droid = Droid.Droid(globalWorkDir, localWorkDir, outputDir=outputDir)
         droid.start()
         
         while droid.isAlive():
            droid.join(timeout=300)
         logger.info("Rank %s: Droid finished status: %s",mpirank, status)
      except:
         logger.exception("Rank %s: Droid failed",mpirank)
         raise
   

if __name__ == "__main__":
   logging.basicConfig(level=logging.INFO,
         format='%(asctime)s|%(process)s|%(levelname)s|%(name)s|%(message)s',
         datefmt='%Y-%m-%d %H:%M:%S')

   oparser = argparse.ArgumentParser()
   oparser.add_argument('--globalWorkingDir',dest="globalWorkingDir",
                        help="Global share working directory",required=True)
   oparser.add_argument('--localWorkingDir',dest="localWorkingDir",
                        help="Local working directory. if it's not set, it will use global working directory",default=None)
   oparser.add_argument('--outputDir', dest="outputDir",
                        help="Copy output files to this directory",default=None)
   oparser.add_argument('--dumpEventOutputs',default=False,action='store_true',
                        help="Dump event output info to xml")
   oparser.add_argument('--verbose', '-v', default=False, action='store_true',
                        help="Set logger to DEBUG.")
   oparser.add_argument('--quiet', '-q', default=False, action='store_true',
                        help="Set logger to ERROR.")
   args = oparser.parse_args()
   
   if args.verbose and not args.quiet:
      logger.setLevel(logging.DEBUG)
   elif not args.verbose and args.quiet:
      logger.setLevel(logging.ERROR)
   elif not args.verbose and not args.quiet:
      logger.setLevel(logging.INFO)
   else:
      logger.error(' Cannot set both verbose and quiet flag. ')
      oparser.print_help()
      sys.exit(-1)

   if args.localWorkingDir == None:
      args.localWorkingDir = args.globalWorkingDir

   main(args.globalWorkingDir, args.localWorkingDir, args.outputDir, args.dumpEventOutputs)

