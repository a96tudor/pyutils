import builtins
from pyutils.decorators.retry_connection import retry_connection
from pyutils.decorators.retry_connection import time


def test_retry_connection_success_after_retry(mocker):
    call_count = {"n": 0}

    @retry_connection(retry_count=3, delay=0)
    def func():
        call_count["n"] += 1
        if call_count["n"] < 2:
            raise ValueError("fail")
        return "ok"

    sleep = mocker.patch("pyutils.decorators.retry_connection.time.sleep")
    result = func()
    assert result == "ok"
    assert call_count["n"] == 2
    sleep.assert_called_once_with(0)


def test_retry_connection_failure(mocker):
    @retry_connection(retry_count=2, delay=0)
    def func():
        raise ValueError("fail")

    sleep = mocker.patch("pyutils.decorators.retry_connection.time.sleep")
    result = func()
    assert result is None
    sleep.assert_called_once_with(0)


def test_retry_connection_jitter_on_exception(mocker):
    jitter = mocker.patch(
        "pyutils.decorators.retry_connection.get_jitter_delay_value",
        return_value=0,
    )
    sleep = mocker.patch("pyutils.decorators.retry_connection.time.sleep")

    @retry_connection(
        retry_count=2,
        delay=0,
        add_retry_jitter_exc=[ValueError],
        min_retry_jitter=1,
        max_retry_jitter=2,
    )
    def func():
        raise ValueError("fail")

    result = func()
    assert result is None
    jitter.assert_called_once_with(1, min_delay=1, max_delay=2)
    sleep.assert_called_once_with(0)
