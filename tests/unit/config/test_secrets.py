import pytest

from pyutils.config.secrets import SecretValues


@pytest.fixture
def secret_values():
    return SecretValues(["something"], {"key1": "value1", "key2": "value2"})


def test_repr_and_str(secret_values):
    repr_str = repr(secret_values)
    str_str = str(secret_values)
    assert "<SecretValues: ['something'] [LOCKED]>" in repr_str
    assert "<SecretValues: ['something'] [LOCKED]>" in str_str


def test_repr_unlocked(secret_values):
    with secret_values.unlock() as sv:
        repr_str = repr(sv)
        str_str = str(sv)
        assert "<SecretValues: ['something'] [UNLOCKED]>" in repr_str
        assert "<SecretValues: ['something'] [UNLOCKED]>" in str_str


def test_get_attribute(secret_values):
    with secret_values.unlock() as sv:
        assert sv.secret == {"key1": "value1", "key2": "value2"}
        assert sv.name == ["something"]

    with pytest.raises(PermissionError):
        _ = secret_values.secret  # Should raise PermissionError when locked
