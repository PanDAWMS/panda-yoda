
# get release version
verFile = open('version')
release_version = verFile.read()
verFile.close()

from distutils.core import setup

setup(
   name="panda-yoda",
   version=release_version,
   description=' PanDA Yoda Package',
   long_description="This package contains PanDA Yoda Components",
   license='GPL',
   author='Panda Team',
   author_email='atlas-adc-panda@cern.ch',
   url='https://github.com/PanDAWMS/panda-yoda',
   packages = [ 'pandayoda',
              'pandayoda/yoda',
              'pandayoda/droid',
              'pandayoda/test',
              ],
   entry_points = {
           'console_scripts': ['yoda_droid=pandayoda.yoda_droid:main'],
               },
   data_files = [
      ('etc/panda',['pandayoda/yoda.cfg']),
      ('templates',['templates/ThetaSubmitTF.sh','templates/CoriHaswellSubmitTF.sh']),
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
   install_requires=['mpi4py']
)
