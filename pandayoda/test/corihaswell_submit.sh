#!/bin/bash -l
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)

#SBATCH -p debug
#SBATCH --qos=premium
#SBATCH -t 0:30:00
#SBATCH -N 2
#SBATCH -L SCRATCH,project
#SBATCH --cpus-per-task=32
#SBATCH -C haswell
#SBATCH -A m2015

module load python/2.7-anaconda

echo [$SECONDS] Setting up yoda environment
MPI_RANKS_PER_NODE=1

BASEDIR=/global/cscratch1/sd/parton/yodatesting
YODADIR=$BASEDIR/panda-yoda

WORK_DIR=$BASEDIR/testjobs/slurm-$SLURM_JOB_ID
mkdir -p $WORK_DIR
echo [$SECONDS] WORK_DIR = $WORK_DIR

# setup yampl and yoda
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$BASEDIR/lib
export PYTHONPATH=$PYTHONPATH:$BASEDIR/lib/python2.7/site-packages:$YODADIR

# copy input files that Harvester would provide
cp $BASEDIR/testjobs/input1/* $WORK_DIR/

echo [$SECONDS] Starting yoda_droid
srun -n $(( $SLURM_NNODES*$MPI_RANKS_PER_NODE )) -N $MPI_RANKS_PER_NODE ../yoda_droid.py -w $WORK_DIR --debug -c $YODADIR/pandayoda/yoda.cfg
EXIT_CODE=$?
echo [$SECONDS] yoda_droid exit code = $EXIT_CODE
exit $EXIT_CODE
