from time import sleep
from random import random


class Retry(object):
    """
    A Decorator.
    Allow certain operations to retry
    for a specified number of times.
    """

    def __init__(self, *, retry_times=3,
                 wait_secs=5, errors=(Exception,)):
        self.retry_times = retry_times
        self.wait_secs = wait_secs
        self.errors = errors

    def __call__(self, fn):

        def wrapper(*args, **kwargs):
            for _ in range(self.retry_times):
                try:
                    return fn(*args, **kwargs)
                except self.errors as e:
                    print(e)
                    sleep((random() + 1) * self.wait_secs)
            return None

        return wrapper
