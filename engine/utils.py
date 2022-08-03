import logging
import os
import signal
import time
from datetime import datetime, timedelta
from typing import Callable

import colorlog
from google.cloud import bigquery

handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s %(asctime)s [%(filename)s] %(levelname)s %(message)s',
    log_colors={
        'DEBUG': 'cyan',
        'WARNING': 'yellow',
        'ERROR': 'red'
    }
))

log = colorlog.getLogger()
log.addHandler(handler)
log.setLevel(logging.INFO)


def set_verbose_mode():
    log.setLevel(logging.DEBUG)
    info_modules = ['grpc', 'google']
    for module in info_modules:
        colorlog.getLogger(module).setLevel(logging.INFO)

class BigQuery:

    def __int__(self, project: str):
        os.environ.setdefault("GCLOUD_PROJECT", project)
        self._project_id = project
        self._client = bigquery.Client()


def raise_timeout(signum, frame):
    raise TimeoutError


def retry_factory(timeout: int, min_pull_interval: int, max_pull_interval: int, pull_interval_coeff: float) -> Callable:
    def decorator(func) -> Callable:
        def func_with_timeout(*args, **kwargs):
            signal.signal(signal.SIGALRM, raise_timeout)
            signal.alarm(timeout)
            pull_interval = min_pull_interval
            while True:
                try:
                    if func(*args, **kwargs):
                        return
                    attempt_time = datetime.now() + timedelta(seconds=pull_interval)
                    print('waiting for result, next check is at %s', attempt_time)
                    time.sleep(pull_interval)
                    next_pull_interval = pull_interval * pull_interval_coeff
                    pull_interval = next_pull_interval if next_pull_interval < max_pull_interval else max_pull_interval
                except Exception as e:
                    print(e)
                    raise Exception('exception thrown during waiting')

        return func_with_timeout

    return decorator
