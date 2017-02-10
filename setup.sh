

# Ensure 'python' is python version 2.7

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo Adding to PYTHONPATH: $DIR
export PYTHONPATH=$DIR:$PYTHONPATH
