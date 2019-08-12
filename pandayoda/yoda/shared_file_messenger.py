import os
import json
import logging
import ConfigParser
import time
import glob
import sys
from pandayoda.common.yoda_multiprocessing import Lock, Manager, Event
from pandayoda.common import exceptions, serializer

logger = logging.getLogger(__name__)

'''

This is a collection of functions used by Yoda/Droid to communicate with Harvester. It can be exchanged with other plugins

The jobSpecFile format is like this:
=================================================================================
{
   "3298217817": {
      "GUID": "BEA4C016-E37E-0841-A448-8D664E8CD570",
      "PandaID": 3298217817,
      "StatusCode": 0,
      "attemptNr": 3,
      "checksum": "ad:363a57ab",
      "cloud": "WORLD",
      "cmtConfig": "x86_64-slc6-gcc47-opt",
      "coreCount": 8,
      "currentPriority": 851,
      "ddmEndPointIn": "NERSC_DATADISK",
      "ddmEndPointOut": "LRZ-LMU_DATADISK,NERSC_DATADISK",
      "destinationDBlockToken": "dst:LRZ-LMU_DATADISK,dst:NERSC_DATADISK",
      "destinationDblock": "mc15_13TeV.362002.Sherpa_CT10_Znunu_Pt0_70_CVetoBVeto_fac025.simul.HITS.e4376_s3022_tid10919503_00_sub0384058277,
      mc15_13TeV.362002.Sherpa_CT10_Znunu_Pt0_70_CVetoBVeto_fac025.simul.log.e4376_s3022_tid10919503_00_sub0384058278",
      "destinationSE": "LRZ-LMU_C2PAP_MCORE",
      "dispatchDBlockToken": "NULL",
      "dispatchDBlockTokenForOut": "NULL,NULL",
      "dispatchDblock": "panda.10919503.03.15.GEN.c2a897d6-ea51-4054-83a5-ce0df170c6e1_dis003287071386",
      "eventService": "True",
      "fileDestinationSE": "LRZ-LMU_C2PAP_MCORE,NERSC_Edison",
      "fsize": "24805997",
      "homepackage": "AtlasProduction/19.2.5.3",
      "inFilePaths": "/scratch2/scratchdirs/dbenjami/harvester_edison/test-area/test-18/EVNT.06402143._000615.pool.root.1",
      "inFiles": "EVNT.06402143._000615.pool.root.1",
      "jobDefinitionID": 0,
      "jobName": "mc15_13TeV.362002.Sherpa_CT10_Znunu_Pt0_70_CVetoBVeto_fac025.simul.e4376_s3022.3268661856",
      "jobPars": "--inputEVNTFile=EVNT.06402143._000615.pool.root.1 --AMITag=s3022 --DBRelease=\"default:current\" --DataRunNumber=222525
      --conditionsTag \"default:OFLCOND-RUN12-SDR-19\"
      --firstEvent=1 --geometryVersion=\"default:ATLAS-R2-2015-03-01-00_VALIDATION\" --maxEvents=1000 --outputHITSFile=HITS.10919503._000051.pool.root.1
      --physicsList=FTFP_BERT --postInclude \"default:PyJobTransforms/UseFrontier.py\"
      --preInclude \"EVNTtoHITS:SimulationJobOptions/preInclude.BeamPipeKill.py,SimulationJobOptions/preInclude.FrozenShowersFCalOnly.py,AthenaMP/
      AthenaMP_EventService.py\"
       --randomSeed=611 --runNumber=362002 --simulator=MC12G4
      --skipEvents=0 --truthStrategy=MC15aPlus",
      "jobsetID": 3287071385,
      "logFile": "log.10919503._000051.job.log.tgz.1.3298217817",
      "logGUID": "6872598f-658b-4ecb-9a61-0e1945e44dac",
      "maxCpuCount": 46981,
      "maxDiskCount": 323,
      "maxWalltime": 46981,
      "minRamCount": 23750,
      "nSent": 1,
      "nucleus": "LRZ-LMU",
      "outFiles": "HITS.10919503._000051.pool.root.1,log.10919503._000051.job.log.tgz.1.3298217817",
      "processingType": "validation",
      "prodDBlockToken": "NULL",
      "prodDBlockTokenForOutput": "NULL,NULL",
      "prodDBlocks": "mc15_13TeV:mc15_13TeV.362002.Sherpa_CT10_Znunu_Pt0_70_CVetoBVeto_fac025.evgen.EVNT.e4376/",
      "prodSourceLabel": "managed",
      "prodUserID": "glushkov",
      "realDatasets": "mc15_13TeV.362002.Sherpa_CT10_Znunu_Pt0_70_CVetoBVeto_fac025.simul.HITS.e4376_s3022_tid10919503_00,
      mc15_13TeV.362002.Sherpa_CT10_Znunu_Pt0_70_CVetoBVeto_fac025.simul.log.e4376_s3022_tid10919503_00",
      "realDatasetsIn": "mc15_13TeV:mc15_13TeV.362002.Sherpa_CT10_Znunu_Pt0_70_CVetoBVeto_fac025.evgen.EVNT.e4376/",
      "scopeIn": "mc15_13TeV",
      "scopeLog": "mc15_13TeV",
      "scopeOut": "mc15_13TeV",
      "sourceSite": "NULL",
      "swRelease": "Atlas-19.2.5",
      "taskID": 10919503,
      "transferType": "NULL",
      "transformation": "Sim_tf.py"
   }
}
=================================================================================

The eventRangesFile format looks like this:

=================================================================================
{
   "3298217817": [
      {
         "GUID": "BEA4C016-E37E-0841-A448-8D664E8CD570",
         "LFN": "EVNT.06402143._000615.pool.root.1",
         "eventRangeID": "10919503-3298217817-8731829857-1-49",
         "lastEvent": 1,
         "scope": "mc15_13TeV",
         "startEvent": 1
      },
      {
         "GUID": "BEA4C016-E37E-0841-A448-8D664E8CD570",
         "LFN": "EVNT.06402143._000615.pool.root.1",
         "eventRangeID": "10919503-3298217817-8731829857-2-49",
         "lastEvent": 2,
         "scope": "mc15_13TeV",
         "startEvent": 2
      },
      {
         "GUID": "BEA4C016-E37E-0841-A448-8D664E8CD570",
         "LFN": "EVNT.06402143._000615.pool.root.1",
         "eventRangeID": "10919503-3298217817-8731829857-3-49",
         "lastEvent": 3,
         "scope": "mc15_13TeV",
         "startEvent": 3
      },
      {
         "GUID": "BEA4C016-E37E-0841-A448-8D664E8CD570",
         "LFN": "EVNT.06402143._000615.pool.root.1",
         "eventRangeID": "10919503-3298217817-8731829857-4-49",
         "lastEvent": 4,
         "scope": "mc15_13TeV",
         "startEvent": 4
      },
      {
         "GUID": "BEA4C016-E37E-0841-A448-8D664E8CD570",
         "LFN": "EVNT.06402143._000615.pool.root.1",
         "eventRangeID": "10919503-3298217817-8731829857-5-49",
         "lastEvent": 5,
         "scope": "mc15_13TeV",
         "startEvent": 5
      },
      {
         "GUID": "BEA4C016-E37E-0841-A448-8D664E8CD570",
         "LFN": "EVNT.06402143._000615.pool.root.1",
         "eventRangeID": "10919503-3298217817-8731829857-6-49",
         "lastEvent": 6,
         "scope": "mc15_13TeV",
         "startEvent": 6
      },
      {
         "GUID": "BEA4C016-E37E-0841-A448-8D664E8CD570",
         "LFN": "EVNT.06402143._000615.pool.root.1",
         "eventRangeID": "10919503-3298217817-8731829857-7-49",
         "lastEvent": 7,
         "scope": "mc15_13TeV",
         "startEvent": 7
      },
      {
         "GUID": "BEA4C016-E37E-0841-A448-8D664E8CD570",
         "LFN": "EVNT.06402143._000615.pool.root.1",
         "eventRangeID": "10919503-3298217817-8731829857-8-49",
         "lastEvent": 8,
         "scope": "mc15_13TeV",
         "startEvent": 8
      }
   ],
}

=================================================================================


'''

# acquire/release when accessing the harvester config, this avoids collisions.
# I put this in because the setup function was being called in parallel with the requestevents()
# this caused the request events to fail because it was moving faster than the setup function
# and therefore could not find the configuration information yet and threw an exception.

harConfSect = 'payload_interaction'
sfm_mgr = Manager()
sfm_har_config = sfm_mgr.dict()
sfm_har_config_lock = Lock()
sfm_har_config_done = Event()


def setup(config):
    global sfm_har_config, sfm_har_config_lock, sfm_har_config_done
    sfm_har_config_lock.acquire()
    if len(sfm_har_config) == 0:
        logger.debug('loading harvester configuration file')

        if 'shared_file_messenger' in config:
            # get harvester config filename
            if 'harvester_config_file' in config['shared_file_messenger']:
                harvester_config_file = config['shared_file_messenger']['harvester_config_file']
                logger.info('harvester_config_file: %s', harvester_config_file)
            else:
                raise Exception('must specify "harvester_config_file" in shared_file_messenger config section')

            # get harvester config filename
            if 'loglevel' in config['shared_file_messenger']:
                loglevel = config['shared_file_messenger']['loglevel']
                logger.info('loglevel: %s', loglevel)
                logger.setLevel(logging.getLevelName(loglevel))
        else:
            raise Exception('must include "shared_file_messenger" section in config file')

        # parse harvester configuration file
        try:
            har_config, default = get_config(harvester_config_file)
            if harConfSect in har_config:
                for key in har_config[harConfSect].keys():
                    sfm_har_config[key] = har_config[harConfSect][key]
            else:
                raise Exception('harvester config file has no "payload_interaction" section')
        except Exception:
            sfm_har_config_lock.release()
            logger.exception('error parsing harvester config')
            raise

        if len(har_config) == 0:
            sfm_har_config_lock.release()
            raise Exception('Failed to parse config file: %s' % (harvester_config_file))

        sfm_har_config_done.set()
    else:
        logger.debug('harvester configuration already loaded')

    sfm_har_config_lock.release()


def get_config(config_filename):
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
            for key, value in configfile.items(section):
                # exclude DEFAULT keys
                if key not in configfile.defaults().keys():
                    config[section][key] = value
                else:
                    default[key] = value
    # logger.debug('after bcast %s',config)

    return config, default


def request_jobs():
    global sfm_har_config, sfm_har_config_lock, sfm_har_config_done
    sfm_har_config_done.wait()
    logger.debug('requesting job')
    jobrequestfile = sfm_har_config['jobRequestFile']

    logger.debug('writing jobRequestFile to signal Harvester: %s', jobrequestfile)
    open(jobrequestfile, 'w').write('jobRequestFile')


def pandajobs_ready():
    global sfm_har_config, sfm_har_config_done
    sfm_har_config_done.wait()
    logger.debug('check if panda jobs exist')

    # the file in which job descriptions would be stored
    logger.debug('sfm_har_config keys: %s', sfm_har_config.keys())
    if 'jobSpecFile' in sfm_har_config.keys():
        jobspecfile = sfm_har_config['jobSpecFile']
    else:
        raise Exception('could not find "jobSpecFile" in harvester config file')

    # check to see if a file exists.
    if os.path.exists(jobspecfile):
        logger.debug('found jobSpecFile file from Harvester: %s', jobspecfile)
        return True
    else:
        logger.debug('did not find jobSpecFile file from Harvester: %s', jobspecfile)
    return False


# this function should return a job description or nothing
def get_pandajobs():
    global sfm_har_config, sfm_har_config_done
    sfm_har_config_done.wait()
    logger.debug('in get_pandajobs')

    # the file in which job descriptions would be stored
    jobspecfile = sfm_har_config['jobSpecFile']
    jobrequestfile = sfm_har_config['jobRequestFile']

    # first check to see if a file already exists.
    try:
        logger.debug('jobSpecFile is present, reading job definitions')
        # parse job spec file
        job_def = json.load(open(jobspecfile))
        # remove this file now that we are done with it
        os.rename(jobspecfile, jobspecfile + '.old')
        # remove request file if harvester has not already
        if os.path.exists(jobrequestfile):
            os.remove(jobrequestfile)
        # return job definition
        return job_def
    except Exception:
        logger.exception('failed to parse jobSpecFile: %s' % (jobspecfile))
        raise


def request_eventranges(job_def):
    global sfm_har_config, sfm_har_config_done
    sfm_har_config_done.wait()

    # retrieve event request file
    eventrequestfile = sfm_har_config['eventRequestFile']
    eventrequestfile_tmp = eventrequestfile + '.tmp'

    # crate event request file
    if not os.path.exists(eventrequestfile):
        # need to output a file containing:
        #   {'nRanges': ???, 'pandaID':???, 'taskID':???, 'jobsetID':???}
        logger.debug('requesting new event ranges by writing to file "%s" with this content: %s', eventrequestfile,
                     job_def)

        # get new job definition
        new_job_def = {job_def['pandaID']: job_def}

        f = open(eventrequestfile_tmp, 'w')
        f.write(serializer.serialize(new_job_def))
        f.close()

        # now move tmp filename to real filename
        os.rename(eventrequestfile_tmp, eventrequestfile)

    else:
        logger.debug('request file already exists. Adding requests')

        # move current file to temp
        os.rename(eventrequestfile, eventrequestfile_tmp)

        filedata = open(eventrequestfile_tmp).read()
        requests = serializer.deserialize(filedata)

        pandaid = job_def['pandaID']
        if pandaid in requests:
            logger.debug('adding event range count to existing request')
            thisjob = requests[pandaid]
            if thisjob['jobsetID'] == job_def['jobsetID'] and thisjob['taskID'] == job_def['taskID']:
                thisjob['nRanges'] += job_def['nRanges']
            else:
                logger.warning('existing request for PandaID %s does not match new request details %s', thisjob,
                               job_def)
        else:
            logger.debug('adding new job definition to existing request')
            requests[pandaid] = job_def

        # output updated requests to file
        open(eventrequestfile_tmp, 'w').write(serializer.serialize(requests))

        # now move tmp filename to real filename
        os.rename(eventrequestfile_tmp, eventrequestfile)


def eventranges_ready(block=False, timeout=60, loop_sleep_time=5):
    global sfm_har_config, sfm_har_config_done
    sfm_har_config_done.wait()
    logger.debug('eventranges_ready start')

    # load name of events file
    eventrangesfile = sfm_har_config['eventRangesFile']

    # check to see if a file exists.
    start = time.time()
    timewaiting = int(time.time() - start)
    while block and (timeout > timewaiting):
        logger.debug('checking for eventRangesFile')
        if os.path.exists(eventrangesfile):
            logger.debug('eventRangesFile exists')
            return True
        else:
            logger.debug('no eventRangesFile so sleeping 1 second and checking again, timewaiting = %d; timeout = %d',
                         timewaiting, timeout)
            time.sleep(loop_sleep_time)
            timewaiting = int(time.time() - start)

    logger.debug('no eventRangesFile, exiting')
    return False


def get_eventranges():
    global sfm_har_config, sfm_har_config_done
    sfm_har_config_done.wait()
    logger.debug('getting eventranges')

    # load name of events file
    eventrangesfile = sfm_har_config['eventRangesFile']

    # first check to see if a file already exists.
    if os.path.exists(eventrangesfile):
        try:
            logger.debug('eventRangesFile is present, parsing event ranges')
            # read in event range file
            fstat = os.stat(eventrangesfile)
            logger.debug('eventRangesFile stat: %s', str(fstat))
            try:
                eventranges = json.load(open(eventrangesfile))
            except Exception:
                # try again if the files modify time is within 10 seconds
                succeeded = False
                time_since_last_modified = fstat.st_mtime - time.time()
                while time_since_last_modified < 10 and not succeeded:
                    try:
                        logger.debug(' trying again to open eventRangesFile: %s', str(fstat))
                        eventranges = json.load(open(eventrangesfile))
                        succeeded = True
                    except Exception:
                        logger.debug(' failed to open, time since last modified is %d seconds',
                                     time_since_last_modified)
                        time.sleep(1)
                        # update data before checking again
                        fstat = os.stat(eventrangesfile)
                        time_since_last_modified = fstat.st_mtime - time.time()

                if not succeeded:
                    raise Exception(
                        'failed to open eventRangesFile, even after waiting for modified status. '
                        'Time since last modified: %d seconds' % time_since_last_modified)

            logger.debug('received json object with size %s bytes', sys.getsizeof(eventranges))
            for jobid, ranges in eventranges.iteritems():
                logger.debug('received %s ranges for Panda ID: %s', len(ranges), jobid)

            # remove this file now that we are done with it
            newtmp = glob.glob(eventrangesfile + '.old*')
            os.rename(eventrangesfile, eventrangesfile + ('.old.%02i' % len(newtmp)))
            # remove the request file if harvester has not already
            # if os.path.exists(harvesterConfig.get(harConfSect,'eventRangesFile')):
            # os.remove(harvesterConfig.get(harConfSect,'eventRangesFile'))
            # return event ranges
            return eventranges
        except Exception:
            logger.exception('failed to parse eventRangesFile: %s', eventrangesfile)
            # if the event range file is present, rename it as failedread for debugging
            if os.path.exists(eventrangesfile):
                newtmp = glob.glob(eventrangesfile + '.old*')
                newname = eventrangesfile + ('.old.%02i.failedread' % len(newtmp))
                os.rename(eventrangesfile, newname)
            raise exceptions.MessengerFailedToParse('failed to parse event file, find it here %s' % newname)
    return {}


'''
The worker needs to put eventStatusDumpJsonFile to update events and/or stage-out. The file is a json dump of a dictionary
{pandaID_1:[{'eventRangeID':???, 'eventStatus':???, 'path':???, 'type':???, 'chksum':???, 'guid':???}, ...],
pandaID_2:[...], ...}. Only 'eventRangeID' and 'eventStatus' are required to update events, while other file information
such as 'path', 'type' and 'chksum' are required in addition if the event produced an output file. 'type' is the type of
the file and it can be 'output', 'es_output', or 'log'. 'eventRangeID' should be removed unless the type is 'es_output'.
Harvester uploads files depending on the 'type'. 'chksum' is calculated using adler32 if omitted. If the output has an
intrinsic guid (this is the case for POOL files) 'guid' needs to be set. eventStatusDumpJsonFile is deleted once Harvester
updates the database.
'''


def stage_out_file_exists():
    global sfm_har_config, sfm_har_config_done
    sfm_har_config_done.wait()

    # load name of eventStatusDumpJsonFile file
    eventstatusdumpjsonfile = sfm_har_config['eventStatusDumpJsonFile']

    return os.path.exists(eventstatusdumpjsonfile)


def stage_out_file(output_type, output_path, eventrangeid, eventstatus, pandaid, chksum=None, ):
    global sfm_har_config, sfm_har_config_done
    sfm_har_config_done.wait()

    if output_type not in ['output', 'es_output', 'log']:
        raise Exception('incorrect type provided: %s' % (output_type))

    if not os.path.exists(output_path):
        raise Exception('output file not found: %s' % (output_path))

    # make sure pandaID is a string
    pandaid = str(pandaid)

    # load name of eventStatusDumpJsonFile file
    eventstatusdumpjsonfile = sfm_har_config['eventStatusDumpJsonFile']

    # first create a temp file to place contents
    # this avoids Harvester trying to read the file while it is being written
    eventstatusdumpjsonfile_tmp = eventstatusdumpjsonfile + '.tmp'

    # format data for file:
    file_descriptor = {'eventRangeID': eventrangeid,
                       'eventStatus': eventstatus,
                       'path': output_path,
                       'type': output_type,
                       'chksum': chksum,
                       'guid': None,
                       }

    # if file does not already exists, new data is just what we have
    if not os.path.exists(eventstatusdumpjsonfile):
        data = {pandaid: [file_descriptor]}

    # if the file exists, move it to a tmp filename, update its contents and then recreate it.
    else:

        # first move existing file to tmp so Harvester does not read it while we edit
        try:
            os.rename(eventstatusdumpjsonfile, eventstatusdumpjsonfile_tmp)
        except Exception:
            logger.warning('tried moving %s to a tmp filename to add more output files for Harvester.',
                           eventstatusdumpjsonfile)
            if not os.path.exists(eventstatusdumpjsonfile):
                logger.warning('%s file no longer exists so Harvester must have grabbed it. Need to create a new file',
                               eventstatusdumpjsonfile)
                data = {pandaid: [file_descriptor]}
        else:

            # now open and read in the data
            with open(eventstatusdumpjsonfile_tmp, 'r') as f:
                data = serializer.deserialize(f.read())
            logger.debug('existing data contains %s', data)
            # if the pandaID already exists, just append the new file to that list
            if pandaid in data:
                logger.debug('addding data to existing panda list')
                data[pandaid].append(file_descriptor)
            # if the pandaID does not exist, add a new list
            else:
                logger.debug('addding new panda id list')
                data[pandaid] = [file_descriptor]

    logger.debug('output to file %s: %s', eventstatusdumpjsonfile, data)

    # overwrite the temp file with the updated data
    with open(eventstatusdumpjsonfile_tmp, 'w') as f:
        f.write(serializer.serialize(data))

    # move tmp file into place
    os.rename(eventstatusdumpjsonfile_tmp, eventstatusdumpjsonfile)


def stage_out_files(file_list, output_type):
    global sfm_har_config, sfm_har_config_done
    sfm_har_config_done.wait()

    if output_type not in ['output', 'es_output', 'log']:
        raise Exception('incorrect type provided: %s' % (output_type))

    # load name of eventStatusDumpJsonFile file
        eventstatusdumpjsonfile = sfm_har_config['eventStatusDumpJsonFile']

    eventstatusdumpdata = {}
    # loop over filelist
    for filedata in file_list:

        # make sure pandaID is a string
        pandaid = str(filedata['pandaid'])

        chksum = None
        if 'chksum' in filedata:
            chksum = filedata['chksum']

        # filename = os.path.join(output_path,os.path.basename(filedata['filename']))

        # format data for file:
        file_descriptor = {'eventRangeID': filedata['eventrangeid'],
                           'eventStatus': filedata['eventstatus'],
                           'path': filedata['filename'],
                           'type': output_type,
                           'chksum': chksum,
                           'guid': None,
                           }
        try:
            eventstatusdumpdata[pandaid].append(file_descriptor)
        except KeyError:
            eventstatusdumpdata[pandaid] = [file_descriptor]

    # create a temp file to place contents
    # this avoids Harvester trying to read the file while it is being written
    eventstatusdumpjsonfile_tmp = eventstatusdumpjsonfile + '.tmp'

    # if file does not already exists, new data is just what we have
    if not os.path.exists(eventstatusdumpjsonfile):
        data = eventstatusdumpdata

    # if the file exists, move it to a tmp filename, update its contents and then recreate it.
    else:

        # first move existing file to tmp so Harvester does not read it while we edit
        try:
            os.rename(eventstatusdumpjsonfile, eventstatusdumpjsonfile_tmp)
        except Exception:
            logger.warning('tried moving %s to a tmp filename to add more output files for Harvester.',
                           eventstatusdumpjsonfile)
            if not os.path.exists(eventstatusdumpjsonfile):
                logger.warning('%s file no longer exists so Harvester must have grabbed it. Need to create a new file',
                               eventstatusdumpjsonfile)
                data = eventstatusdumpdata
        else:

            # now open and read in the data
            with open(eventstatusdumpjsonfile_tmp, 'r') as f:
                data = serializer.deserialize(f.read())
            logger.debug('found existing data for pandaIDs: %s', data.keys())

            for pandaid in eventstatusdumpdata:

                # if the pandaID already exists, just append the new file to that list
                try:
                    logger.debug('addding data to existing panda list')
                    data[pandaid] += eventstatusdumpdata[pandaid]
                # if the pandaID does not exist, add a new list
                except KeyError:
                    logger.debug('addding new panda id list')
                    data[pandaid] = eventstatusdumpdata[pandaid]

    if logger.getEffectiveLevel() == logging.DEBUG:
        tmpstr = ' '.join('%s:%s' % (x, len(data[x])) for x in data)
        logger.debug('writing output to file %s with keys: %s', eventstatusdumpjsonfile, tmpstr)

    # overwrite the temp file with the updated data
    with open(eventstatusdumpjsonfile_tmp, 'w') as f:
        f.write(serializer.serialize(data, pretty_print=True))

    # move tmp file into place
    os.rename(eventstatusdumpjsonfile_tmp, eventstatusdumpjsonfile)

    logger.debug('done')
