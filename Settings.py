# CONFIGURATION FILE

# log size of the ring
m = 20
SIZE = 2**m
KEY_LIFESPAN = 60 * 60 * 24

BUFFER_SIZE = 2048
# INT = interval in seconds
# RET = retry limit

GLOBAL_TIMEOUT = 5

# Stabilize
STABILIZE_INT = 2
STABILIZE_RET = 3

# Fix Fingers
FIX_FINGERS_INT = 10
FIX_FINGERS_RET = 3

# Check Predecessors
CHECK_PREDECESSOR_INT = 5
CHECK_PREDECESSOR_RET = 3

# Succ
SUCC_RET = 3

# Check Name Distribution
CHECK_DISTRIBUTE_INT = 30
