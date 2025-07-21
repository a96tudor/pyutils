import time
from functools import wraps
from typing import Any, Callable, Optional

from pyutils.decorators.utils import get_jitter_delay_value


def retry_connection(
    retry_count: int = 3,
    delay: int = 5,
    add_retry_jitter: bool = False,
    min_retry_jitter: Optional[int] = None,
    max_retry_jitter: Optional[int] = None,
    add_retry_jitter_exc: Optional[list] = None,
) -> Callable:
    """
    A wrapper that will automatically retry API requests in the case of an exception.
    The function using this wrapper must raise given exception for this to work.

    Parameters:
    retry_count (int): Number of retries before quitting
    delay (int): Delay between retry attempts
    raise_err (bool): Should an error be raised if failure after max # of attempts
    retry_exc (list): List of Exceptions to retry
    add_retry_jitter (bool): Calculates delay by choosing a random number between a
       specified min and a calculated exponential backoff

    Returns:
    Same output as original function, or exception.
    """

    add_retry_jitter_exc = add_retry_jitter_exc or []

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            _att = 0
            for _att in range(1, retry_count + 1):
                retry_delay = delay
                jitter_set = False
                if add_retry_jitter:
                    jitter_set = True
                    retry_delay = get_jitter_delay_value(
                        _att, min_delay=min_retry_jitter, max_delay=max_retry_jitter
                    )

                try:
                    retry_result = func(*args, **kwargs)

                    return retry_result
                except Exception as ex:
                    # Not last attempt - log error and retry
                    if _att < retry_count:
                        if not jitter_set and type(ex) in add_retry_jitter_exc:
                            retry_delay = get_jitter_delay_value(
                                _att,
                                min_delay=min_retry_jitter,
                                max_delay=max_retry_jitter,
                            )

                        time.sleep(retry_delay)
                        continue

            return None

        return wrapper

    return decorator
