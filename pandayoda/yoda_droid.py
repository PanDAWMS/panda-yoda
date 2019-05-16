#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)
# - Paul Nilsson (paul.nilsson@cern.ch)

try:
    import argparse
    import logging
    import os
    import sys
    import datetime
    import time
    import socket
    imptime = time.time()
    if sys.version_info >= (3, 0, 0):
        import configparser as ConfigParser
    else:
        import ConfigParser

    # print('%06.2f import yoda' % (time.time() - imptime))
    from pandayoda.yoda import Yoda
    # print('%06.2f import droid' % (time.time() - imptime))
    from pandayoda.droid import Droid
    # print('%06.2f import MPIService' % (time.time() - imptime))
    from pandayoda.common import MPIService
    # print('%06.2f import yoda_multiprocessing' % (time.time() - imptime))
    from pandayoda.common.yoda_multiprocessing import Queue,Manager
    # print('%06.2f import commit_timestamp & version' % (time.time() - imptime))
    from pandayoda import commit_timestamp,version
    logger = logging.getLogger(__name__)
except Exception,e:
    print('Exception received during import: %s' % e)
    import traceback,sys
    exc_type, exc_value, exc_traceback = sys.exc_info()
    print(' '.join(line for line in traceback.format_exception(exc_type, exc_value,
                                                               exc_traceback)))

    from pandayoda import commit_timestamp, version
    print('yoda version: %s' % version.version)
    print('git commit string: %s' % commit_timestamp.timestamp)
    from mpi4py import MPI
    MPI.COMM_WORLD.Abort()
    import sys
    sys.exit(-1)

config_section = os.path.basename(__file__)[:os.path.basename(__file__).rfind('.')]


def main():
    # print('%06.2f yoda_droid main' % (imptime - time.time()))
    # keep track of start time for wall-clock monitoring
    start_time = datetime.datetime.now()

    # parse command line
    oparser = argparse.ArgumentParser()

    # set yoda config file where most settings are placed
    oparser.add_argument('-c', '--yoda-config', dest='yoda_config', help='The Yoda Config file is where most configuration information is set.', required=True)
    oparser.add_argument('-w', '--working-path', dest='working_path', help='The Directory in which to run yoda_droid', default='.')
    oparser.add_argument('-t', '--wall-clock-limit', dest='wall_clock_limit', help='The wall clock time limit in minutes. If given, yoda will trigger all droid ranks to kill their subprocesses and exit. Then Yoda will perform log/output file cleanup.', default=-1, type=int)
    # control output level
    oparser.add_argument('--debug', dest='debug', default=False, action='store_true', help="Set Logger to DEBUG")
    oparser.add_argument('--error', dest='error', default=False, action='store_true', help="Set Logger to ERROR")
    oparser.add_argument('--warning', dest='warning', default=False, action='store_true', help="Set Logger to ERROR")

    args, unknown_args = oparser.parse_known_args()

    # parse configuration file
    try:
        config, default = get_config(args.yoda_config, unknown_args)

        # loop timeout
        if config_section in config:
            if 'loop_timeout' in config[config_section]:
                loop_timeout = int(config[config_section]['loop_timeout'])
            else:
                loop_timeout = 60

            # wallclock_expiring_leadtime
            if 'wallclock_expiring_leadtime' in config[config_section]:
                wallclock_expiring_leadtime = int(config[config_section]['wallclock_expiring_leadtime'])
            else:
                wallclock_expiring_leadtime = 300
        else:
            loop_timeout = 60
            wallclock_expiring_leadtime = 300

    except Exception:
        raise

    # start the MPI service
    if 'MPIService' in config:

        mpi_loglevel = 'INFO'
        if 'loglevel' in config['MPIService']:
            mpi_loglevel = config['MPIService']['loglevel']

        mpi_debug_message_char_length = 200
        if 'mpi_debug_message_char_length' in config['MPIService']:
            mpi_debug_message_char_length = int(config['MPIService']['mpi_debug_message_char_length'])

        mpi_default_message_buffer_size = 10000000
        if 'mpi_default_message_buffer_size' in config['MPIService']:
            mpi_default_message_buffer_size = int(config['MPIService']['mpi_default_message_buffer_size'])

        mpi_loop_timeout = 30
        if 'loop_timeout' in config['MPIService']:
            mpi_loop_timeout = int(config['MPIService']['loop_timeout'])
    else:
        raise Exception('no MPIService configuration')

    # need to create a list of queue objects at the top level
    # that can then be passed around
    # after MPIService sets the rank information we can
    # map each queue to a process
    queue_list = [Queue() for _ in range(7)]
    # create a sharable dictionary that will be used
    # to tell MPIService which queue_list entry goes
    # to which thread
    mgr = Manager()
    queue_map = mgr.dict()
    # create MPIService
    mpiService = MPIService.MPIService(
        queue_list,
        queue_map,
        mpi_loglevel,
        mpi_debug_message_char_length,
        mpi_default_message_buffer_size,
        mpi_loop_timeout,
    )
    # start the subprocess
    mpiService.start()

    # get rank and world size
    mpiService.rank.wait()
    rank = mpiService.rank.get()
    mpiService.worldsize.wait()
    nranks = mpiService.worldsize.get()

    # set the queue map
    if rank == 0:
        queue_map['yoda_droid'] = 0
        queue_map['MPIService'] = 1
        queue_map['Yoda'] = 2
        queue_map['WorkManager'] = 3
        queue_map['RequestHarvesterJob'] = 4
        queue_map['RequestHarvesterEventRanges'] = 5
        queue_map['FileManager'] = 6

        queues = {
            'yoda_droid': queue_list[queue_map['yoda_droid']],
            'MPIService': queue_list[queue_map['MPIService']],
            'Yoda': queue_list[queue_map['Yoda']],
            'WorkManager': queue_list[queue_map['WorkManager']],
            'RequestHarvesterJob': queue_list[queue_map['RequestHarvesterJob']],
            'RequestHarvesterEventRanges': queue_list[queue_map['RequestHarvesterEventRanges']],
            'FileManager': queue_list[queue_map['FileManager']],
        }
    else:
        queue_map['yoda_droid'] = 0
        queue_map['MPIService'] = 1
        queue_map['Droid'] = 2
        queue_map['JobComm'] = 3
        queue_map['TransformManager'] = 4


        queues = {
            'yoda_droid': queue_list[queue_map['yoda_droid']],
            'MPIService': queue_list[queue_map['MPIService']],
            'Droid': queue_list[queue_map['Droid']],
            'JobComm': queue_list[queue_map['JobComm']],
            'TransformManager': queue_list[queue_map['TransformManager']],
        }
    # tell MPIService we have set the queue_map
    mpiService.queue_map_is_set()

    logging_format = '%(asctime)s|%(process)s|%(thread)s|' + ('%05d' % rank) + '|%(levelname)s|%(name)s|%(message)s'
    logging_datefmt = '%Y-%m-%d %H:%M:%S'
    logging_filename = 'yoda_droid_%05d.log' % rank
    for h in logging.root.handlers:
        logging.root.removeHandler(h)
    logging.basicConfig(level=logging.INFO,
                        format=logging_format,
                        datefmt=logging_datefmt,
                        filename=logging_filename)
    logger.info('Start yoda_droid: %s', __file__)

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

    # dereference any links on the working path
    working_path = os.path.abspath(args.working_path)

    # make we are in the working path
    if os.getcwd() != working_path:
        os.chdir(working_path)

    # get MPI world info
    logger.info('yoda droid version:          %s', version.version)
    logger.info('git repo commit string:      %s', commit_timestamp.timestamp)
    logger.info('rank %10i of %10i', rank, nranks)
    logger.info('python version:              %s', sys.version)
    logger.info('working_path:                %s', working_path)
    logger.info('config_filename:             %s', args.yoda_config)
    logger.info('starting_path:               %s', os.getcwd())
    logger.info('wall_clock_limit:            %d', args.wall_clock_limit)

    logger.info('running on hostname:         %s', socket.gethostname())

    # parse wall_clock_limit
    # the time in minutes of the wall clock given to the local
    # batch scheduler. If a non-negative number, this value
    # determines when yoda will signal all Droids to kill
    # their running processes and exit
    # after which yoda will peform clean up actions.
    # It should be in number of minutes.
    wall_clock_limit = datetime.timedelta(minutes=args.wall_clock_limit)

    # track starting path
    starting_path = os.path.normpath(os.getcwd())
    if starting_path != working_path:
        # move to working path
        os.chdir(working_path)

    yoda = None
    droid = None
    # if you are rank 0, start the yoda thread
    if rank == 0:
        try:
            yoda = Yoda.Yoda(queues,
                             config,
                             rank,
                             nranks)
            yoda.start()
        except Exception:
            logger.exception('failed to start Yoda.')
            raise
    else:
        # all other ranks start Droid threads
        try:
            droid = Droid.Droid(queues,
                                config,
                                rank)
            droid.start()
        except Exception:
            logger.exception('failed to start Droid')
            raise

    # loop until droid and/or yoda exit
    while True:
        logger.debug('yoda_droid start loop')

        if wallclock_expiring(wall_clock_limit, start_time, wallclock_expiring_leadtime):
            logger.info('wall clock is expiring')
            if droid is not None:
                droid.stop()
            if yoda is not None:
                yoda.wallclock_expired.set()
                yoda.stop()
            # MPIService.MPI.COMM_WORLD.Abort()
            break

        if droid and not droid.is_alive():
            logger.debug('droid has finished')
            if droid.get_state() == droid.EXITED:
                logger.info('droid exited cleanly')
            else:
                logger.error('droid exited uncleanly, killing all ranks')
                from mpi4py import MPI
                MPI.COMM_WORLD.Abort()
            break
        if yoda and not yoda.is_alive():
            logger.info('yoda has finished')
            break
        logger.debug('sleeping %s', loop_timeout)
        time.sleep(loop_timeout)


    # logger.info('Rank %s: waiting for other ranks to reach MPI Barrier',mpirank)
    # MPI.COMM_WORLD.Barrier()
    # logger.info('yoda_droid aborting all MPI ranks')
    # MPIService.MPI.COMM_WORLD.Abort()

    if droid is not None:
        logger.info('join Droid')
        droid.join()
        droid = None

    if yoda is not None:
        logger.info('join Yoda')
        yoda.join()
        yoda = None

    logger.info(' yoda_droid signaling for MPIService to exit')
    mpiService.stop()
    logger.info('join MPIService')
    mpiService.join()
    logger.info('yoda_droid exiting')


def wallclock_expiring(wall_clock_limit, start_time, wallclock_expiring_leadtime):
    if wall_clock_limit.total_seconds() > 0:
        running_time = datetime.datetime.now() - start_time
        timeleft = wall_clock_limit - running_time
        if timeleft.total_seconds() < wallclock_expiring_leadtime:
            logger.debug('time left %s is less than the leadtime %s, triggering exit.', timeleft.total_seconds(), wallclock_expiring_leadtime)
            return True
        else:
            logger.debug('time left %s before wall clock expires.', timeleft)
    else:
        logger.debug('no wallclock limit set, no exit will be triggered')
    return False


def get_config(config_filename, unknown_args):

    config = {}
    default = {}
    configfile = ConfigParser.ConfigParser()
    # make config options case sensitive (insensitive by default)
    configfile.optionxform = str
    logger.debug('reading config file: %s', config_filename)
    with open(config_filename) as fp:
        configfile.readfp(fp)
        for section in configfile.sections():
            config[section] = {}
            for key,value in configfile.items(section):
                # exclude DEFAULT keys
                if key not in configfile.defaults().keys():
                    config[section][key] = value
                else:
                    default[key] = value
    # logger.debug('after bcast %s',config)

    # user can parse arguments on the command line that have the config name
    # then the passed variable overrides that in the config file
    i = 0
    while i < len(unknown_args):
        arg = unknown_args[i]
        if arg.startswith('--'):
            arg = arg[2:]
            if i + 1 < len(unknown_args):
                value = unknown_args[i + 1]
                for section in config.keys():
                    seclist = config[section]

                    if arg in seclist.keys():
                        print('overriding configuration file value %s:%s = %s' % (section, arg, value))
                        config[section][arg] = value
                i += 2  # increment past arg & value, to go to next arg
            else:
                print('no value to go with arg %s, reached end of unknown_args' % arg)
                break
        else:
            raise Exception('found arg %s not starting with "--" on command line, configuration failed' % arg)

    return config, default


if __name__ == "__main__":
    print('%06.2f yoda version: %s' % (time.time() - imptime, version.version))
    print('%06.2f git commit string: %s' % (time.time() - imptime, commit_timestamp.timestamp))
    logging.basicConfig()
    try:
        main()
    except Exception as e:
        print('received exception')
        import traceback
        traceback.print_exc()
        from mpi4py import MPI
        print('Rank %05d: uncaught exception. Aborting all ranks: %s' % (MPI.COMM_WORLD.Get_rank(), e))
        MPI.COMM_WORLD.Abort()
        import sys
        sys.exit(-1)
