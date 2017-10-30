

# Ensure 'python' is python version 2.7

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo Adding to PYTHONPATH: $DIR
export PYTHONPATH=$DIR:$PYTHONPATH

BASE_DIR=/projects/AtlasADSP/atlas/harvester
echo Adding local harvester site-packages folder to head of python path
export PYTHONPATH=$BASE_DIR/lib/python2.7/site-packages:$PYTHONPATH
#echo Adding local yampl build to ld library path
export LD_LIBRARY_PATH=$BASE_DIR/yampl:$LD_LIBRARY_PATH
export PYTHONPATH=$PYTHONPATH:$BASE_DIR/python-yampl/build/lib.linux-x86_64-2.7

