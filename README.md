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
module swap PrgEnv-intel PrgEnv-gnu

# create virtualenv area if not already existing
virtualenv panda
cd panda
# upgrade the local version of pip and tell it how to reach outside NERSC
pip install --upgrade --index-url=http://pypi.python.org/simple/ --trusted-host pypi.python.org  pip

pip install git+git://github.com/PanDAWMS/panda-yoda

# next you need to install yampl
git clone git@github.com:vitillo/yampl.git
cd yampl
./configure --prefix=$PWD/install CC=cc CXX=CC LDFLAGS=-dynamic
make -j 10 install
# in my case this fails in the zeromq folder
# so do this
cd zeromq
./configure --prefix=$PWD/install CC=cc CXX=CC LDFLAGS=-dynamic
make -j 10 install
cd ..
make -j 10 install
cd ..

# now you need to install the python bindings for yampl
git clone git@github.com:vitillo/python-yampl.git
cd python-yampl
# edit setup.py such that inside the 'include_dirs' inside the 'Extension' have
# both the path to the '/path/to/yampl/install/include/yampl' 
# and '/path/to/yampl/install/include' and the 'extra_link_args' has 
# '-L/path/to/yampl/install/lib'. 

# then run
python setup.py build_ext 
# the last command may fail, if so, remove the '-dynamic' from it and run by hand
# then run
python setup.py install


# in order to run you need to make sure LD_LIBRARY_PATH includes
export LD_LIBRARY_PATH=/path/to/yampl/install/lib:$LD_LIBRARY_PATH

```

