
import argparse
import logging
import os
import sys
import time
import traceback
from mpi4py import MPI
from pandayoda.yodacore import Yoda
from pandayoda.yodaexe import Droid
logger = logging.getLogger(__name__)

def main(globalWorkDir, localWorkDir, outputDir=None, dumpEventOutputs=True):

   # get MPI world info
   comm = MPI.COMM_WORLD
   mpirank = comm.Get_rank()
   mpisize = comm.Get_size()

   # Create separate working directory for each rank
   curdir = os.path.abspath (localWorkDir)
   wkdirname = "rank_%08i" % mpirank
   wkdir  = os.path.abspath (os.path.join(curdir,wkdirname))
   if not os.path.exists(wkdir):
      os.makedirs (wkdir)
   os.chdir (wkdir)

   logger.info("GlobalWorkDir: %s" % globalWorkDir)
   logger.info("LocalWorkDir: %s" % localWorkDir)
   logger.info("OutputDir: %s" % outputDir)
   logger.info("RANK: %08i of %08i" % (mpirank,mpisize))

   if mpirank==0:
     try:
         yoda = Yoda.Yoda(globalWorkDir, localWorkDir, rank=0, nonMPIMode=nonMPIMode, 
                           outputDir=outputDir, dumpEventOutputs=dumpEventOutputs)
         yoda.start()

         
         if nonMPIMode:
            reserveCores = 0
         else:
            reserveCores = 1
         droid = Droid.Droid(globalWorkDir, localWorkDir, rank=0, nonMPIMode=True, 
                             reserveCores=reserveCores, outputDir=outputDir)
         droid.start()

         i = 30
         while True:
            logger.info("Rank %s: Yoda isAlive %s" % (mpirank, yoda.isAlive()))
            logger.info("Rank %s: Droid isAlive %s" % (mpirank, droid.isAlive()))

            if yoda and yoda.isAlive():
              time.sleep(60)
            else:
               break
         logger.info("Rank %s: Yoda finished" % (mpirank))
      except:
         logger.exception("Rank %s: Yoda failed" % mpirank)
         raise
     #os._exit(0)
   else:
      try:
         status = 0
         droid = Droid.Droid(globalWorkDir, localWorkDir, rank=mpirank, 
                             nonMPIMode=nonMPIMode, outputDir=outputDir)
         droid.start()
         while (droid and droid.isAlive()):
             droid.join(timeout=1)
         # parent process
         #pid, status = os.waitpid(child_pid, 0)
         logger.info("Rank %s: Droid finished status: %s" % (mpirank, status))
      except:
         logger.exception("Rank %s: Droid failed" % mpirank)
         raise
   return mpirank

if __name__ == "__main__":
   logging.basicConfig(level=logging.INFO,
         format='%(asctime)s|%(process)s|%(levelname)s|%(name)s|%(message)s',
         datefmt='%Y-%m-%d %H:%M:%S')

   oparser = argparse.ArgumentParser()
   oparser.add_argument('--globalWorkingDir',dest="globalWorkingDir",
                        help="Global share working directory",required=True)
   oparser.add_argument('--localWorkingDir',dest="localWorkingDir",
                        help="Local working directory. if it's not set, it will use global working directory",
                        required=True)
   oparser.add_argument('--outputDir', dest="outputDir",help="Copy output files to this directory")
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

   try:
     logger.info("Start HPCJob")
     rank = main(args.globalWorkingDir, args.localWorkingDir, args.nonMPIMode, args.outputDir, args.dumpEventOutputs)
     logger.info( "Rank %s: HPCJob-Yoda success" % rank )
     if rank == 0:
         # this causes all MPI ranks to exit, uncleanly
         MPI.COMM_WORLD.Abort(-1)
         sys.exit(0)
   except Exception as e:
     logger.exception("Rank " + rank + ": HPCJob-Yoda failed, exiting all ranks.")
     if rank == 0:
         # this causes all MPI ranks to exit, uncleanly
         MPI.COMM_WORLD.Abort(-1)
         sys.exit(-1)
   #os._exit(0)
