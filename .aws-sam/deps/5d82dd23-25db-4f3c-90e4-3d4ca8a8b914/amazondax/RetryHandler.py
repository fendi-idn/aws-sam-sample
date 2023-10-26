import random
import functools
import time

from .DaxError import DaxErrorCode, DaxClientError, DaxServiceError, is_retryable, is_retryable_with_backoff
from .Constants import SDK_DEFAULT_BASE_DELAY_MS


def equal_jitter_backoff(cap, base=SDK_DEFAULT_BASE_DELAY_MS, attempts=0):
    """Calculate time to sleep based on equal jitter and backoff approach
    https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

    The format is::
        temp = min(cap, base * 2 ** attempt)
        sleep = temp / 2 + random(0, temp / 2)
    If ``base`` is not set then the default value will be used.
    Base must be greater than 0 and a number, otherwise a ValueError will be
    raised.
    """

    if base <= 0:
        raise ValueError("The 'base' param must be greater than 0 and a number, "
                         "got: %s" % base)

    temp = min(cap, base * 2 ** attempts)
    time_to_sleep = temp / 2 + random.randint(0, temp / 2)
    return time_to_sleep


def create_equal_jitter_backoff_function(cap, base):
    """Create an equal jitter function based on the attempts.
    This is used so that you only have to pass it the attempts
    parameter to calculate the delay.
    """

    return functools.partial(
        equal_jitter_backoff, cap, base=base)


class RetryHandler(object):
    DEFAULT_RETRIES = 2  # Same as other DAX clients
    THROTTLED_BASE_DELAY_MS = 70  # Same as other DAX clients
    MAX_BACKOFF_MS = 20 * 1000  # Same as other DAX clients

    def __init__(self, config, cluster):
        self._delay = create_equal_jitter_backoff_function(
            RetryHandler.MAX_BACKOFF_MS, RetryHandler.THROTTLED_BASE_DELAY_MS)
        self._attempts = 0
        self._max_attempts = config.retries.get('max_attempts', RetryHandler.DEFAULT_RETRIES) if config and config.retries \
            else RetryHandler.DEFAULT_RETRIES
        self._retry_timeout = config.connect_timeout
        self._cluster = cluster
        self._last_exception = None

    def can_retry(self, exc):
        """
        Indicates whether the last request can be retried.
        @return true if the last request can be retried, false otherwise
        """
        if self._attempts >= self._max_attempts:
            return False

        return is_retryable(exc)

    def pause_before_retry(self, exc):
        self._attempts += 1
        if (not is_retryable_with_backoff(exc)):
            return
        self.wait_for_cluster(exc)
        delayMs = self._delay(attempts=self._attempts)
        return time.sleep(delayMs/1000)

    def wait_for_cluster(self, exc):
        if not isinstance(exc, (DaxClientError, DaxServiceError)):
            return

        if exc.code == DaxErrorCode.NoRoute:
            self._cluster.wait_for_routes(
                min_healthy=1, leader_min=1, timeout=self._retry_timeout)

    def on_exception(self, exc):
        # This function is used to keep in sync with the functionlity of Java Client,
        #   and if the Python client ever has a use case for this.
        self._last_exception = exc
