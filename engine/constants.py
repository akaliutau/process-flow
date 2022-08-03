from enum import Enum

TIMEOUT_SEC = 3600
MIN_PULL_INTERVAL_SEC = 60
MAX_PULL_INTERVAL_SEC = 60
PULL_INTERVAL_COEFF_MULT = 1.5


class TaskStatus(Enum):
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    CANCELLED = 'CANCELLED'
    FINISHED = 'FINISHED'


class TaskResult(Enum):
    NA = 'NA'
    SUCCESS = 'SUCCESS'
    ERROR = 'ERROR'