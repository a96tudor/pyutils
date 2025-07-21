import time
import traceback
import uuid
from functools import wraps
from typing import Any, Callable, Optional

from beppy.general import logger
from beppy.general.errors import raise_exception_with_new_message

from pyutils.decorators.errors import RetryConnectionError
from pyutils.decorators.utils import get_jitter_delay_value
from pyutils.logging.logger import LoggerBase


def _log_total_retries(
    logger: LoggerBase,
    function_name: str,
    duration: float,
    attempt: int,
    retry_request_id: str,
    retry_count: int,
    identifier: str,
):
    if attempt < 1:
        return

    logger.info(
        {
            "retry_request_id": retry_request_id,
            "retry_max_attempts": retry_count,
            "retry_total_attempts": attempt,
            "identifier": identifier,
            "funcName": function_name,
            "duration": duration,
            "message": (
                f"conn_retry_it - Total retry attempts: {attempt},"
                f" for identifier: '{function_name}'"
                f", with retry_request_id: '{retry_request_id}'"
            ),
        }
    )


def retry_connection(
    retry_exc: list,
    retry_all_exc: bool = False,
    retry_count: int = 3,
    delay: int = 5,
    identifier: str = "",
    domain: str = "",
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
            log_prefix = "conn_retry_it::"
            retry_request_id = f"{time.time()}_{uuid.uuid4()}"
            function_name = func.__qualname__ or func.__name__ or ""

            _att = 0
            for _att in range(1, retry_count + 1):
                retry_result = None
                before_t = time.time()

                retry_delay = delay
                jitter_set = False
                if add_retry_jitter:
                    jitter_set = True
                    retry_delay = get_jitter_delay_value(
                        _att, min_delay=min_retry_jitter, max_delay=max_retry_jitter
                    )

                try:
                    retry_result = func(*args, **kwargs)

                    duration_t = round(time.time() - before_t, 4)

                    # Log total tries (minus 1, since this attempt succeeded)
                    _log_total_retries(
                        retry_request_id, function_name, duration_t, _att - 1
                    )
                    return retry_result
                except Exception as ex:
                    duration_t = round(time.time() - before_t, 4)
                    error_msg = str(ex)
                    error_type = ex.__class__.__name__

                    tb_msg = ""
                    try:
                        tb_msg = traceback.format_exc()
                    except Exception:
                        pass

                    # Error immediately if type of error not in "retry_exc",
                    if not retry_all_exc and type(ex) not in retry_exc:
                        # Log the exception
                        msg = (
                            f"{log_prefix}"
                            f"{function_name} - request failed (Attempt #{_att})"
                            ": Parameterized raising of exception requested"
                        )
                        logger.exception(
                            {
                                "retry_request_id": retry_request_id,
                                "retry_attempt": _att,
                                "identifier": identifier,
                                "funcName": function_name,
                                "duration": duration_t,
                                "domain": domain,
                                "message": msg,
                                "error": error_msg,
                                "error_type": error_type,
                            }
                        )

                        # Log total tries
                        _log_total_retries(
                            retry_request_id,
                            function_name,
                            duration_t,
                            _att,
                            tb_msg=tb_msg,
                        )

                    # Not last attempt - log error and retry
                    if _att < retry_count:
                        if not jitter_set and type(ex) in add_retry_jitter_exc:
                            jitter_set = True
                            retry_delay = get_jitter_delay_value(
                                _att,
                                min_delay=min_retry_jitter,
                                max_delay=max_retry_jitter,
                            )

                        msg = (
                            f"{log_prefix}"
                            f"{function_name} - request failed (Attempt #{_att})"
                            f": Waiting {retry_delay} seconds before retrying."
                            f" Exception: [{error_type}] {ex}"
                        )
                        logger.info(
                            {
                                "retry_request_id": retry_request_id,
                                "retry_attempt": _att,
                                "identifier": identifier,
                                "funcName": function_name,
                                "duration": duration_t,
                                "domain": domain,
                                "message": msg,
                                "error": error_msg,
                                "error_type": error_type,
                            }
                        )
                        time.sleep(retry_delay)
                        continue
                    elif _att == retry_count:
                        # this is last attempt
                        msg = (
                            f"{log_prefix}"
                            f"{function_name} - request failed (Attempt #{_att})"
                            f": Max attempts reached... Exception: [{error_type}] {ex}"
                        )
                        logger.exception(
                            {
                                "retry_request_id": retry_request_id,
                                "retry_attempt": _att,
                                "identifier": identifier,
                                "funcName": function_name,
                                "duration": duration_t,
                                "domain": domain,
                                "message": msg,
                                "error": error_msg,
                                "error_type": error_type,
                            }
                        )

                        # Re-raise error
                        # Log total tries
                        _log_total_retries(
                            retry_request_id,
                            function_name,
                            duration_t,
                            _att,
                            tb_msg=tb_msg,
                        )

            return None

        return wrapper

    return decorator
