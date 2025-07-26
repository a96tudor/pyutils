import uuid

import pytest

from pyutils.helpers.errors import BadArgumentsError
from pyutils.helpers.uuid import convert_to_uuid, is_uuid, validate_uuid, validate_uuids


def test_uuid_validation_and_conversion():
    u = str(uuid.uuid4())
    assert is_uuid(u)
    assert convert_to_uuid(u) == uuid.UUID(u)
    validate_uuid(u)
    validate_uuids([u, u])

    bad = "not-a-uuid"
    assert not is_uuid(bad)
    with pytest.raises(BadArgumentsError):
        validate_uuid(bad)
    with pytest.raises(BadArgumentsError):
        validate_uuids([u, bad])
    with pytest.raises(BadArgumentsError):
        convert_to_uuid(bad)
