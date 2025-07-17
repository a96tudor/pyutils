from unittest.mock import Mock

import pytest

from pyutils.datatools.general import get_in


@pytest.mark.parametrize(
    "coll,keys,default,formatter,expected",
    [
        ({"a": {"b": {"c": "value"}}}, ["a", "b", "c"], None, None, "value"),
        ([{"a": "b"}, {"b", "c"}], [0, "a"], None, None, "b"),
        ({"a": {"b": {"c": "value"}}}, ["a", "b", "d"], "default", None, "default"),
        (
            {"a": {"b": {"c": "value"}}},
            ["a", "b", "c"],
            None,
            lambda x: x.upper(),
            "VALUE",
        ),
        ([1, 2, 3], [0], None, None, 1),
        ([], [0], "default", None, "default"),
    ],
)
def test_get_in(coll, keys, default, formatter, expected):
    result = get_in(coll, keys, default, formatter)
    assert result == expected
