from typing import List
from uuid import UUID

from pyutils.helpers.errors import BadArgumentsError


def is_uuid(test_string: str) -> bool:
    try:
        UUID(test_string)
    except ValueError:
        return False
    return True


def validate_uuid(id_: str) -> None:
    if not is_uuid(id_):
        raise BadArgumentsError(f"The ID {id_} is not a valid UUID")


def validate_uuids(uuids: List[str]) -> None:
    for id_ in uuids:
        validate_uuid(id_)


def convert_to_uuid(id_: str) -> UUID:
    validate_uuid(id_)
    return UUID(id_)
