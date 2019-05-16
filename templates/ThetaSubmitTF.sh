#!/bin/bash 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)

echo [$SECONDS] ThetaSubmitTF Start
echo [$SECONDS] DATE=$(date)

export ATHENA_PROC_NUMBER=128 # AthenaMP workers per node

export ATLAS_BASE_DIR=/projects/AtlasADSP/atlas
export HARVESTER_DIR=$ATLAS_BASE_DIR/harvester


export RUCIO_APPID={processingType}
export FRONTIER_ID="[{taskID}_{pandaID}]"
export CMSSW_VERSION=$FRONTIER_ID

export X509_USER_PROXY=$HARVESTER_DIR/globus/$USER/myproxy
export X509_CERT_DIR=$ATLAS_BASE_DIR/gridsecurity/certificates

echo [$SECONDS] ATHENA_PROC_NUMBER:   $ATHENA_PROC_NUMBER
echo [$SECONDS] HARVESTER_DIR:        $HARVESTER_DIR
echo [$SECONDS] RUCIO_ACCOUNT:        $RUCIO_ACCOUNT
echo [$SECONDS] X509_USER_PROXY:      $X509_USER_PROXY
echo [$SECONDS] X509_CERT_DIR:        $X509_CERT_DIR

#source $HARVESTER_DIR/bin/activate
echo [$SECONDS] Setting up AtlasLocalRootBase
source /projects/AtlasADSP/atlas/setup_atlaslocalrootbase.sh
lsetup rucio

# add this for mpi4py
export PYTHONPATH=$PYTHONPATH:/projects/AtlasADSP/atlas/harvester/lib/python2.7/site-packages


echo [$SECONDS] Setting up Atlas Software
RELEASE={release}
PACKAGE={package}
CMTCONFIG={cmtConfig}
echo RELEASE=$RELEASE
echo PACKAGE=$PACKAGE
echo CMTCONFIG=$CMTCONFIG
RELEASE_MAJOR=$(python -c "x='$RELEASE';print(x.split('.')[0])")
RELEASE_MAJOR_MINOR=$(python -c "x='$RELEASE';print('.'.join(y for y in x.split('.')[0:2]))")
if [[ $RELEASE_MAJOR == '21' ]]; then
   export AtlasSetup=$VO_ATLAS_SW_DIR/software/$RELEASE_MAJOR_MINOR/AtlasSetup
elif [[ RELEASE_MAJOR == '19' ]]; then
   export AtlasSetup=$VO_ATLAS_SW_DIR/software/$CMTCONFIG/$RELEASE/AtlasSetup
else
   echo ERROR: Release $RELEASE not recognized
   exit -1
fi
echo AtlasSetup = $AtlasSetup

source $AtlasSetup/scripts/asetup.sh $RELEASE,$PACKAGE --cmtconfig=$CMTCONFIG --makeflags=\"$MAKEFLAGS\" --cmtextratags=ATLAS,useDBRelease {gcclocation}

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


echo PYTHON Version:       $(python --version)
echo PYTHONPATH:           $PYTHONPATH
echo LD_LIBRARY_PATH:      $LD_LIBRARY_PATH
echo ATHENA_PROC_NUMBER:   $ATHENA_PROC_NUMBER
echo HARVESTER_DIR:        $HARVESTER_DIR
echo RUCIO_ACCOUNT:        $RUCIO_ACCOUNT
echo X509_USER_PROXY:      $X509_USER_PROXY
echo X509_CERT_DIR:        $X509_CERT_DIR


export WORK_DIR=$PWD
echo WORK_DIR:             $WORK_DIR
echo [$SECONDS] Starting transformation
{transformation} {jobPars}
echo [$SECONDS] Transform exited with return code: $?
echo [$SECONDS] Exiting
