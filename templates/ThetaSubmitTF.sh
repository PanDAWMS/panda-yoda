#!/bin/bash 
echo [$SECONDS] ThetaSubmitTF Start
echo [$SECONDS] DATE=$(date)

export ATHENA_PROC_NUMBER=128 # AthenaMP workers per node

export HARVESTER_DIR=/projects/AtlasADSP/atlas/harvester

export RUCIO_ACCOUNT=childers
export RUCIO_APPID={processingType}
export FRONTIER_ID="[{taskID}_{pandaID}]"
export CMSSW_VERSION=$FRONTIER_ID

export X509_USER_PROXY=$HARVESTER_DIR/globus/$USER/myproxy


echo [$SECONDS] ATHENA_PROC_NUMBER:   $ATHENA_PROC_NUMBER
echo [$SECONDS] HARVESTER_DIR:        $HARVESTER_DIR
echo [$SECONDS] RUCIO_ACCOUNT:        $RUCIO_ACCOUNT
echo [$SECONDS] X509_USER_PROXY:      $X509_USER_PROXY
echo [$SECONDS] X509_CERT_DIR:        $X509_CERT_DIR

#source $HARVESTER_DIR/bin/activate
echo [$SECONDS] Setting up AtlasLocalRootBase
source /projects/AtlasADSP/atlas/setup_atlaslocalrootbase.sh
localSetupRucioClients

# add this for mpi4py
export PYTHONPATH=$PYTHONPATH:/projects/AtlasADSP/atlas/harvester/lib/python2.7/site-packages


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


echo PYTHON Version:       $(python -V)
echo PYTHONPATH:           $PYTHONPATH
echo LD_LIBRARY_PATH:      $LD_LIBRARY_PATH

export WORK_DIR=$PWD
echo WORK_DIR:             $WORK_DIR
echo [$SECONDS] Starting transformation
{transformation} {jobPars}
echo [$SECONDS] Transform exited with return code: $?
echo [$SECONDS] Exiting
