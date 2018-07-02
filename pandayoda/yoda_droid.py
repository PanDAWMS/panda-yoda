#!/usr/bin/env python
try:
   import argparse,logging,os,sys,datetime,time,socket
   import ConfigParser

   from pandayoda.yoda import Yoda
   from pandayoda.droid import Droid
   from pandayoda.common import MPIService
   logger = logging.getLogger(__name__)
except Exception,e:
   print('Exception received during import: %s' % str(e))
   import traceback
   traceback.print_exc()
   from mpi4py import MPI
   MPI.COMM_WORLD.Abort()
   import sys
   sys.exit(-1)

config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]


def yoda_droid(working_path,
               config_filename,
               wall_clock_limit,
               start_time = datetime.datetime.now()):
   
   # dereference any links on the working path
   working_path = os.path.normpath(working_path)

   # make we are in the working path
   if os.getcwd() != working_path:
      os.chdir(working_path)
   
   # get MPI world info
   mpirank = MPIService.rank
   mpisize = MPIService.nranks
   logger.debug(' rank %10i of %10i',mpirank,mpisize)
   
   if mpirank == 0:
      logger.info('working_path:                %s',working_path)
      logger.info('config_filename:             %s',config_filename)
      logger.info('starting_path:               %s',os.getcwd())
      logger.info('wall_clock_limit:            %d',wall_clock_limit)
   
   logger.info('running on hostname: %s',socket.gethostname())

   # parse wall_clock_limit
   # the time in minutes of the wall clock given to the local
   # batch scheduler. If a non-negative number, this value
   # determines when yoda will signal all Droids to kill
   # their running processes and exit
   # after which yoda will peform clean up actions.
   # It should be in number of minutes.
   wall_clock_limit = datetime.timedelta(minutes=wall_clock_limit)

   # the ealiest measure of the start time for Yoda/Droid
   # this is used to determine when to exit
   # it is expected to be output from datetime.datetime.now()
   start_time         = start_time

   # parse configuration file
   if os.path.exists(config_filename):
      config = ConfigParser.ConfigParser()
      config.read(config_filename)

      # parse config for this module

      # loop timeout
      if config.has_option(config_section,'loop_timeout'):
         loop_timeout = config.getfloat(config_section,'loop_timeout')
      else:
         loop_timeout = 60
         logger.warning('no "loop_timeout" in "%s" section so using default %s',config_section,loop_timeout)
      
      # wallclock_expiring_leadtime
      if config.has_option(config_section,'wallclock_expiring_leadtime'):
         wallclock_expiring_leadtime = config.getint(config_section,'wallclock_expiring_leadtime')
      else:
         wallclock_expiring_leadtime = 300
         logger.warning('no "wallclock_expiring_leadtime" in "%s" section so using default %s',config_section,wallclock_expiring_leadtime)

   else:
      raise Exception('Rank %d: failed to parse config file: %s' % (mpirank,config_filename))

   # track starting path
   starting_path = os.path.normpath(os.getcwd())
   if starting_path != working_path:
      # move to working path
      os.chdir(working_path)

   yoda = None
   droid = None
   # if you are rank 0, start the yoda thread
   if mpirank == 0:
      try:
         yoda = Yoda.Yoda(config)
         yoda.start()
      except Exception:
         logger.exception('Rank %s: failed to start Yoda.',mpirank)
         raise
   else:
      # all other ranks start Droid threads
      try:
         droid = Droid.Droid(config)
         droid.start()
      except Exception:
         logger.exception('Rank %s: failed to start Droid',mpirank)
         raise

   # loop until droid and/or yoda exit
   while True:
      logger.debug('yoda_droid start loop')

      if wallclock_expiring(wall_clock_limit,start_time,wallclock_expiring_leadtime):
         logger.info('wall clock is expiring')
         if droid is not None:
            droid.stop()
         if yoda is not None:
            yoda.wallclock_expired.set()
            yoda.stop()
         break

      if droid and not droid.isAlive():
         logger.debug('droid has finished')
         if droid.get_state() == droid.EXITED:
            logger.info('droid exited cleanly')
         else:
            logger.error('droid exited uncleanly, killing all ranks')
            MPIService.MPI.COMM_WORLD.Abort()
         break
      if yoda and not yoda.isAlive():
         logger.info('yoda has finished')
         break
      time.sleep(loop_timeout)


   # logger.info('Rank %s: waiting for other ranks to reach MPI Barrier',mpirank)
   # MPI.COMM_WORLD.Barrier()
   # logger.info('yoda_droid aborting all MPI ranks')
   # MPIService.MPI.COMM_WORLD.Abort()

   logger.info(' yoda_droid waiting for MPIService to join')
   MPIService.mpiService.stop()
   MPIService.mpiService.join()
   logger.info('yoda_droid exiting')


def main():
   start_time = datetime.datetime.now()
   logging_format = '%(asctime)s|%(process)s|%(thread)s|' + ('%05d' % MPIService.rank) + '|%(levelname)s|%(name)s|%(message)s'
   logging_datefmt = '%Y-%m-%d %H:%M:%S'
   logging_filename = 'yoda_droid_%05d.log' % MPIService.rank
   logging.basicConfig(level=logging.INFO,
                       format=logging_format,
                       datefmt=logging_datefmt,
                       filename=logging_filename)
   logger.info('Start yoda_droid: %s',__file__)
   oparser = argparse.ArgumentParser()

   # set yoda config file where most settings are placed
   oparser.add_argument('-c','--yoda-config', dest='yoda_config', help='The Yoda Config file is where most configuration information is set.',required=True)
   oparser.add_argument('-w','--working-path', dest='working_path', help='The Directory in which to run yoda_droid',required=True)
   oparser.add_argument('-t','--wall-clock-limit',dest='wall_clock_limit', help='The wall clock time limit in minutes. If given, yoda will trigger all droid ranks to kill their subprocesses and exit. Then Yoda will perform log/output file cleanup.',default=-1,type=int)
   # control output level
   oparser.add_argument('--debug', dest='debug', default=False, action='store_true', help="Set Logger to DEBUG")
   oparser.add_argument('--error', dest='error', default=False, action='store_true', help="Set Logger to ERROR")
   oparser.add_argument('--warning', dest='warning', default=False, action='store_true', help="Set Logger to ERROR")

   
   args = oparser.parse_args()

   if args.debug and not args.error and not args.warning:
      # remove existing root handlers and reconfigure with DEBUG
      for h in logging.root.handlers:
         logging.root.removeHandler(h)
      logging.basicConfig(level=logging.DEBUG,
                          format=logging_format,
                          datefmt=logging_datefmt,
                          filename=logging_filename)
      logger.setLevel(logging.DEBUG)
   elif not args.debug and args.error and not args.warning:
      # remove existing root handlers and reconfigure with ERROR
      for h in logging.root.handlers:
         logging.root.removeHandler(h)
      logging.basicConfig(level=logging.ERROR,
                          format=logging_format,
                          datefmt=logging_datefmt,
                          filename=logging_filename)
      logger.setLevel(logging.ERROR)
   elif not args.debug and not args.error and args.warning:
      # remove existing root handlers and reconfigure with WARNING
      for h in logging.root.handlers:
         logging.root.removeHandler(h)
      logging.basicConfig(level=logging.WARNING,
                          format=logging_format,
                          datefmt=logging_datefmt,
                          filename=logging_filename)
      logger.setLevel(logging.WARNING)
   


   yoda_droid(args.working_path,
              args.yoda_config,
              args.wall_clock_limit,
              start_time)


def wallclock_expiring(wall_clock_limit,start_time,wallclock_expiring_leadtime):
   if wall_clock_limit.total_seconds() > 0:
      running_time = datetime.datetime.now() - start_time
      timeleft = wall_clock_limit - running_time
      if timeleft.total_seconds() < wallclock_expiring_leadtime:
         logger.debug('time left %s is less than the leadtime %s, triggering exit.',timeleft,wallclock_expiring_leadtime)
         return True
      else:
         logger.debug('time left %s before wall clock expires.',timeleft)
   else:
      logger.debug('no wallclock limit set, no exit will be triggered')
   return False


if __name__ == "__main__":
   try:
      main()
   except Exception:
      logger.exception('uncaught exception. Aborting all ranks')
      from mpi4py import MPI
      MPI.COMM_WORLD.Abort()
      import sys
      sys.exit(-1)
