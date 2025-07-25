import io
import json
import logging
import threading

import pytest

from pyutils.logging.formatters import JSONLogFormatter
from pyutils.logging.logger import Logger


@pytest.fixture
def logger_stream():
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    formatter = JSONLogFormatter()
    handler.setFormatter(formatter)

    logger = Logger("test", formatter, handler)
    for h in logger.handlers[:]:
        logger.removeHandler(h)
    logger.addHandler(handler)
    logger.formatter = formatter
    logger.clear_execution_id()
    yield logger, stream
    for h in logger.handlers[:]:
        logger.removeHandler(h)
    logger.clear_execution_id()


def test_execution_id_added(logger_stream):
    logger, stream = logger_stream
    logger.set_execution_id("exec-1")
    logger.info("hello")
    data = json.loads(stream.getvalue())
    assert data["execution_id"] == "exec-1"


def test_no_execution_id_field(logger_stream):
    logger, stream = logger_stream
    logger.info("hello")
    data = json.loads(stream.getvalue())
    assert "execution_id" not in data


def test_parallel_execution_ids(logger_stream):
    logger, stream = logger_stream

    def worker(eid):
        logger.set_execution_id(eid)
        logger.info("msg")
        logger.clear_execution_id()

    t1 = threading.Thread(target=worker, args=("id1",))
    t2 = threading.Thread(target=worker, args=("id2",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    lines = [json.loads(line) for line in stream.getvalue().splitlines()]
    ids = {line["execution_id"] for line in lines}
    assert ids == {"id1", "id2"}
