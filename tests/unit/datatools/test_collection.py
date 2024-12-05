import pytest

from pyutils.datatools.collection import Collection


@pytest.fixture
def collection():
    return Collection()


@pytest.mark.parametrize("read_only", [True, False], ids=["Read-only", "Not read-only"])
def test_init(read_only):
    collection = Collection(["a", "b"], read_only=read_only)

    assert collection == ["a", "b"]
    assert collection.read_only is read_only


@pytest.mark.parametrize("read_only", [True, False], ids=["Read-only", "Not read-only"])
def test_read_only(collection, read_only):
    collection.read_only = read_only

    assert collection.read_only is read_only


def test_read_only_error(collection):
    with pytest.raises(TypeError) as err:
        collection.read_only = "True"

    assert str(err.value) == "read_only value must be a boolean"


def test_append(collection):
    collection.append("a")
    assert collection == ["a"]

    collection.append("b")
    assert collection == ["a", "b"]


def test_append_read_only(collection):
    collection.read_only = True

    with pytest.raises(ValueError) as err:
        collection.append("a")

    assert str(err.value) == "Cannot append to a read-only collection"


def test_extend(collection):
    collection.extend(["a", "b"])
    assert collection == ["a", "b"]

    collection.extend(["c", "d"])
    assert collection == ["a", "b", "c", "d"]


def test_extend_read_only(collection):
    collection.read_only = True

    with pytest.raises(ValueError) as err:
        collection.extend(["a", "b"])

    assert str(err.value) == "Cannot extend a read-only collection"


def test_insert(collection):
    collection.extend(["a", "b"])
    collection.insert(1, "c")

    assert collection == ["a", "c", "b"]


def test_insert_read_only(collection):
    collection.read_only = True

    with pytest.raises(ValueError) as err:
        collection.insert(1, "a")

    assert str(err.value) == "Cannot insert into a read-only collection"


@pytest.mark.parametrize("read_only", [True, False], ids=["Read-only", "Not read-only"])
def test_clone(collection, read_only):
    collection.extend(["a", "b"])
    clone = collection.clone(read_only=read_only)

    assert isinstance(clone, Collection)
    assert clone == ["a", "b"]
    assert clone.read_only is read_only


def test_delete(collection):
    collection.extend(["a", "b", "c"])
    collection.delete(1)

    assert collection == ["a", "c"]


def test_delete_read_only(collection):
    collection.read_only = True

    with pytest.raises(ValueError) as err:
        collection.delete(1)

    assert str(err.value) == "Cannot delete from a read-only collection"


@pytest.mark.parametrize("use_index", [True, False], ids=["Read-only", "Not read-only"])
def test_pop(collection, use_index):
    collection.extend(["a", "b", "c"])

    if use_index:
        assert collection.pop(1) == "b"
        assert collection == ["a", "c"]
    else:
        assert collection.pop() == "c"
        assert collection == ["a", "b"]


def test_pop_read_only(collection):
    collection.read_only = True

    with pytest.raises(ValueError) as err:
        collection.pop()

    assert str(err.value) == "Cannot pop from a read-only collection"
