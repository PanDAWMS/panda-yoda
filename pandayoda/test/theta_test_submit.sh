#!/usr/bin/env bash
#COBALT -n 1
#COBALT -t 60
#COBALT --attrs mcdram=cache:numa=quad
#COBALT -A AtlasADSP

echo [$SECONDS] Setting up yoda environment
MPI_RANKS_PER_NODE=2
HARVESTER_DIR=/projects/AtlasADSP/atlas/harvester

source $HARVESTER_DIR/setup.sh

WORK_DIR=/projects/AtlasADSP/atlas/tests/cobalt-$COBALT_JOBID
mkdir -p $WORK_DIR
echo [$SECONDS] WORK_DIR = $WORK_DIR
echo [$SECONDS] Starting yoda_droid
aprun -n  $(( COBALT_PARTSIZE * $MPI_RANKS_PER_NODE )) -N $MPI_RANKS_PER_NODE ../yoda_droid.py -w $WORK_DIR --debug -c $YODADIR/pandayoda/yoda.cfg
EXIT_CODE=$?
echo [$SECONDS] yoda_droid exit code = $EXIT_CODE
exit $EXIT_CODE
