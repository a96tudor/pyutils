import pytest

from pyutils.helpers.errors import BadArgumentsError, Error


def test_error_repr_and_extension_details():
    err = BadArgumentsError("bad", extra="x")
    repr_str = repr(err)
    assert "BadArgumentsError" in repr_str
    assert "message='bad'" in repr_str
    details = err.extension_details
    assert details["extra"] == "x"
    assert details["code"] == "BadArgumentsError"
