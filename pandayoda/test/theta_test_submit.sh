#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)

#COBALT -n 3
#COBALT -q debug-flat-quad
#COBALT -t 40
#COBALT --attrs mcdram=cache:numa=quad
#COBALT -A AtlasADSP

echo [$SECONDS] Setting up yoda environment
MPI_RANKS_PER_NODE=1
HARVESTER_DIR=/projects/AtlasADSP/atlas/harvester
YODA_DIR=$HARVESTER_DIR/panda-yoda
source $YODA_DIR/pandayoda/test/setup.sh

WORK_DIR=/projects/AtlasADSP/atlas/tests/cobalt-$COBALT_JOBID
mkdir -p $WORK_DIR
echo [$SECONDS] WORK_DIR = $WORK_DIR
# copy input files that Harvester would provide
cp /projects/AtlasADSP/atlas/tests/yodajob_input/* $WORK_DIR/

ulimit -c unlimited

RUNTIME=$(( $COBALT_ENDTIME - $COBALT_STARTTIME ))
RUNTIME=$(( $RUNTIME / 60 ))
echo [$SECONDS] RUNTIME=$RUNTIME

echo [$SECONDS] start environment dump
env | sort
echo [$SECONDS] end environment dump

echo [$SECONDS] Starting yoda_droid
aprun -n $COBALT_PARTSIZE -N 1 -d 64 -j 1 --cc depth -e KMP_AFFINITY=none python -u $YODA_DIR/pandayoda/yoda_droid.py -w $WORK_DIR --debug -c $YODA_DIR/pandayoda/yoda.cfg #-t $RUNTIME
EXIT_CODE=$?
echo [$SECONDS] yoda_droid exit code = $EXIT_CODE
exit $EXIT_CODE
