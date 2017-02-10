panda-yoda
==========

Ideal installation
-------------------

pip install git+git://github.com/PanDAWMS/panda-yoda

Requirements
-------------

- python 2.7+
- mpi4py

Install on NERSC/Edison:
```bash
# setup environment
module load python/2.7.9
module load mpi4py/1.3.1
module load virutalenv

# create virtualenv area if not already existing
virtualenv panda
cd panda
# upgrade the local version of pip and tell it how to reach outside NERSC
pip install --upgrade --index-url=http://pypi.python.org/simple/ --trusted-host pypi.python.org  pip

pip install git+git://github.com/PanDAWMS/panda-yoda
```

