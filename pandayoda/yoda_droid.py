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


def yoda_droid(configFile='yoda.cfg'):

   # config defaults
   defaults={
      'DumpEventOutputs':'false',
      'LogLevel':'INFO',
      'LocalWorkingDir':None,
      'OutputDir':None,
      'InputJobFile':'HPCJobs.json',
   }

   # parse config file
   logger.info("Reading Configuration from file: %s",configFile)
   if os.path.exists(configFile):
      config = ConfigParser.RawConfigParser(defaults)
      config.read(configFile)
   else:
      raise Exception(" input configuration file does not exist ")

   logLevel = config.get('yoda','LogLevel')
   if logLevel == 'INFO':
      logger.setLevel(logging.INFO)
   elif logLevel == 'DEBUG':
      logger.setLevel(logging.DEBUG)
   elif logLevel == 'WARNING':
      logger.setLevel(logging.WARNING)
   elif logLevel == 'ERROR':
      logger.setLevel(logging.ERROR)
   elif logLevel == 'CRITICAL':
      logger.setLevel(logging.CRITICAL)
   else:
      raise Exception(' LogLevel set to %s which is not accepted',logLevel)


   try:
      globalWorkingDir     = config.get('yoda','GlobalWorkingDir')
   except:
      logger.error(' must specify GlobalWorkingDir in the config file %s',configFile)
      raise
   localWorkingDir      = config.get('yoda','LocalWorkingDir')
   if localWorkingDir is None:
      localWorkingDir = globalWorkingDir
   outputDir         = config.get('yoda','OutputDir')
   dumpEventOutputs  = config.getboolean('yoda','DumpEventOutputs')

   logger.info("GlobalWorkingDir:    %s",globalWorkingDir)
   logger.info("LocalWorkingDir:     %s",localWorkingDir)
   logger.info("OutputDir:        %s",outputDir)
   logger.info("DumpEventOutputs: %s",str(dumpEventOutputs))

   
   inputJobFile = config.get('yoda','InputJobFile')
   logger.info("InputJobFile:  %s",inputJobFile)


   # get MPI world info
   try:
      comm = MPI.COMM_WORLD
      mpirank = comm.Get_rank()
      mpisize = comm.Get_size()
      logger.debug(' rank %10i of %10i',mpirank,mpisize)
   except:
      logger.error('Exception retrieving MPI rank information')
      raise

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
         droid = Droid.Droid(globalWorkingDir, localWorkingDir, outputDir=outputDir)
         droid.start()
         
         while droid.isAlive():
            droid.join(timeout=300)
         logger.info("Rank %s: Droid finished status: %s",mpirank, status)
      except:
         logger.exception("Rank %s: Droid failed",mpirank)
         raise
   
def main():
   logging.basicConfig(level=logging.INFO,
         format='%(asctime)s|%(process)s|%(levelname)s|%(name)s|%(message)s',
         datefmt='%Y-%m-%d %H:%M:%S')
   logging.info('Start yoda_droid')
   oparser = argparse.ArgumentParser()
   oparser.add_argument('-c','--config',dest='configFile',help='input configuration filename',default='yoda.cfg')
   args = oparser.parse_args()

   yoda_droid(args.configFile)


if __name__ == "__main__":
   main()
