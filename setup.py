# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)

from distutils.core import setup
import subprocess

desc = subprocess.check_output(['git', 'describe', '--tags']).decode("utf-8")
tag = desc.split('-')[0]

setup(
    name="panda-yoda",
    version=tag,
    description=' PanDA Yoda Package',
    long_description="This package contains PanDA Yoda Components",
    license='GPL',
    author='Panda Team',
    author_email='atlas-adc-panda@cern.ch',
    url='https://github.com/PanDAWMS/panda-yoda',
    packages=['pandayoda',
              'pandayoda/yoda',
              'pandayoda/droid',
              'pandayoda/common',
              'pandayoda/jobmod',
              ],
    entry_points={
        'console_scripts': ['yoda_droid=pandayoda.yoda_droid:main'],
    },
    data_files=[
        ('etc/panda', ['pandayoda/yoda_template.cfg']),
        ('templates', ['templates/ThetaSubmitTF.sh',
                       'templates/CoriHaswellSubmitTF.sh']),
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Operating System :: Unix',
    ],
    # install_requires=['mpi4py']
)
