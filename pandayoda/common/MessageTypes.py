

# Theses are the Queue message and MPI message types 


# sent by Droid to Yoda to get a job
REQUEST_JOB             = 'REQUEST_JOB'
# sent by Yoda to Droid with a panda job definition
NEW_JOB                 = 'NEW_JOB'
# sent by Yoda to Droid when no more jobs are coming
NO_MORE_JOBS            = 'NO_MORE_JOBS'
# sent by Droid to JobComm when a Transform exits
TRANSFORM_EXITED        = 'TRANSFORM_EXITED'

REQUEST_EVENT_RANGES    = 'REQUEST_EVENT_RANGES'
NEW_EVENT_RANGES        = 'NEW_EVENT_RANGES'
NO_MORE_EVENT_RANGES    = 'NO_MORE_EVENT_RANGES'
REQUEST_PENDING         = 'REQUEST_PENDING'

PAYLOAD_READY_FOR_EVENTS = 'PAYLOAD_READY_FOR_EVENTS'
PAYLOAD_HAS_OUTPUT_FILE = 'PAYLOAD_HAS_OUTPUT_FILE'

OUTPUT_FILE             = 'OUTPUT_FILE'

DROID_EXIT              = 'DROID_EXIT'
DROID_HAS_EXITED        = 'DROID_HAS_EXITED'

WALLCLOCK_EXPIRING      = 'WALLCLOCK_EXPIRING'

WAKE_UP                 = 'WAKE_UP'

TYPES=[
   REQUEST_JOB,
   NEW_JOB,
   NO_MORE_JOBS,
   
   REQUEST_EVENT_RANGES,
   NEW_EVENT_RANGES,
   NO_MORE_EVENT_RANGES,
   REQUEST_PENDING,

   OUTPUT_FILE,
   
   DROID_EXIT,
   DROID_HAS_EXITED,

   WALLCLOCK_EXPIRING,
]