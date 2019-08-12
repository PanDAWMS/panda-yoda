#!/bin/bash -l
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)

echo ThetaSubmitTF Start
echo DATE=$(date)

export ATHENA_PROC_NUMBER=16 # 64 AthenaMP workers per node

export ATLAS_DIR=/global/cscratch1/sd/parton/atlas
export PILOT_DIR=$ATLAS_DIR/pilot

export RUCIO_ACCOUNT=childers
export RUCIO_APPID={processingType}
export FRONTIER_ID="[{taskID}_{pandaID}]"
export CMSSW_VERSION=$FRONTIER_ID

export X509_USER_PROXY=$ATLAS_DIR/gridsecurity/$USER/myproxy


echo ATHENA_PROC_NUMBER:   $ATHENA_PROC_NUMBER
echo HARVESTER_DIR:        $HARVESTER_DIR
echo PILOT_DIR:            $PILOT_DIR
echo RUCIO_ACCOUNT:        $RUCIO_ACCOUNT
echo X509_USER_PROXY:      $X509_USER_PROXY

echo [$SECONDS] Setting up python
module load python/2.7-anaconda

#source $HARVESTER_DIR/bin/activate
echo [$SECONDS] Setting up AtlasLocalRootBase
source $ATLAS_DIR/setup.sh
localSetupRucioClients

echo X509_CERT_DIR:        $X509_CERT_DIR

echo [$SECONDS] Setting up Atlas Software
RELEASE={release}
PACKAGE={package}
CMTCONFIG={cmtConfig}
asetup $RELEASE,$PACKAGE --cmtconfig=$CMTCONFIG --makeflags=\"$MAKEFLAGS\" --cmtextratags=ATLAS,useDBRelease {gcclocation}

export LD_LIBRARY_PATH=$VO_ATLAS_SW_DIR/ldpatch:$LD_LIBRARY_PATH
DBBASEPATH=$ATLAS_DB_AREA/DBRelease/current
export CORAL_DBLOOKUP_PATH=$DBBASEPATH/XMLConfig
export CORAL_AUTH_PATH=$DBBASEPATH/XMLConfig
export DATAPATH=$DBBASEPATH:$DATAPATH
mkdir poolcond
export DBREL_LOCATION=$ATLAS_DB_AREA/DBRelease
cp $DBREL_LOCATION/current/poolcond/*.xml poolcond
export DATAPATH=$PWD:$DATAPATH
unset FRONTIER_SERVER

#export HPC_SW_INSTALL_AREA=/projects/AtlasADSP/atlas/cvmfs/atlas.cern.ch/repo/sw

#export PYTHONPATH=$PILOT_DIR:$YODA_DIR:$YAMPL_PY_DIR:$PYTHONPATH
#export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$YAMPL_DIR

echo PYTHON Version:       $(python -V)
echo PYTHONPATH:           $PYTHONPATH
echo LD_LIBRARY_PATH:      $LD_LIBRARY_PATH

export WORK_DIR=$PWD
echo WORK_DIR:             $WORK_DIR
echo [$SECONDS] Starting transformation
{transformation} {jobPars}
echo [$SECONDS] Transform exited with return code: $?
echo [$SECONDS] Exiting
