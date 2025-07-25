import json
import logging
import sys

import pytest

from pyutils.logging.formatters import JSONLogFormatter
from pyutils.logging.handlers import BetterStackHandler, CommandLineHandler


def test_json_log_formatter_basic():
    formatter = JSONLogFormatter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="hello",
        args=(),
        exc_info=None,
    )
    record.execution_id = "exec"
    payload = json.loads(formatter.format(record))
    assert payload["message"] == "hello"
    assert payload["level"] == "INFO"
    assert payload["execution_id"] == "exec"


def test_json_log_formatter_with_exception():
    formatter = JSONLogFormatter()
    try:
        1 / 0
    except ZeroDivisionError:
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname=__file__,
            lineno=20,
            msg="boom",
            args=(),
            exc_info=sys.exc_info(),
        )
        payload = json.loads(formatter.format(record))
        assert "stack" in payload
        assert len(payload["stack"]) <= 2000


def test_command_line_handler_emits(capsys):
    handler = CommandLineHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s:%(message)s"))
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=5,
        msg="msg",
        args=(),
        exc_info=None,
    )
    handler.emit(record)
    captured = capsys.readouterr().out.strip()
    assert captured == "INFO:msg"


def test_better_stack_handler_emits(mocker):
    handler = BetterStackHandler("API")
    handler.setFormatter(logging.Formatter("%(message)s"))
    post = mocker.patch("pyutils.logging.handlers.requests.post")
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=5,
        msg="msg",
        args=(),
        exc_info=None,
    )
    handler.emit(record)
    post.assert_called_once_with(
        handler.endpoint, headers=handler.headers, data="msg"
    )
