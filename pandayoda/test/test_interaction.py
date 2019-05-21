# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Taylor Childers (john.taylor.childers@cern.ch)
# - Paul Nilsson (paul.nilsson@cern.ch)

from mpi4py import MPI
from pandayoda.yodacore import Interaction

comm = MPI.COMM_WORLD
mpirank = comm.Get_rank()

if mpirank == 0:
    rsv = Interaction.Receiver()
    while rsv.activeRanks():
        tmpStat, method, params = rsv.receiveRequest()
        print(mpirank, 'got', tmpStat, method, params)
        print(rsv.returnResponse({'msg': 'Done'}))
        rsv.decrementNumRank()
    print(mpirank, "done")
else:
    snd = Interaction.Requester()
    print(mpirank, "sending req")
    res = snd.sendRequest('dummy', {1: 2, 3: 4, 'rank': mpirank})
    print(res)
    print(mpirank, "done")
