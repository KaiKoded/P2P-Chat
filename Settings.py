# CONFIGURATION FILE

# log size of the ring
m = 20
SIZE = 2**m

# successors list size (to continue operating on node failures)
N_SUCCESSORS = 4

BUFFER_SIZE = 256
# INT = interval in seconds
# RET = retry limit

GLOBAL_TIMEOUT = 10

# Stabilize
STABILIZE_INT = 2
STABILIZE_RET = 3

# Fix Fingers
FIX_FINGERS_INT = 4

# Update Successors
UPDATE_SUCCESSORS_INT = 1
UPDATE_SUCCESSORS_RET = 6

# Find Successors
FIND_SUCCESSOR_RET = 3
FIND_PREDECESSOR_RET = 3

CHECK_PREDECESSOR_INT = 5
CHECK_PREDECESSOR_RET = 3