import pytest

from pyutils.datatools.attributes import Attribute, AttributesCollection


@pytest.fixture
def attribute():
    return Attribute("name", "value")


@pytest.fixture
def attributes_collection():
    return AttributesCollection(
        [Attribute("name-1", "value-1"), Attribute("name-2", "value-2")]
    )


def test_attribute_str(attribute):
    assert str(attribute) == "name: value"


def test_attribute_repr(attribute):
    assert repr(attribute) == "Attribute(name): value"


def test_attribute_name(attribute):
    assert attribute.name == "name"


def test_attribute_name_setter(attribute):
    attribute.name = "new-name"
    assert attribute.name == "new-name"


def test_attribute_value(attribute):
    assert attribute.value == "value"


def test_attribute_value_setter(attribute):
    attribute.value = "new-value"
    assert attribute.value == "new-value"


def test_attribute_to_dict(attribute):
    assert attribute.to_dict() == {"name": "name", "value": "value"}


def test_attribute_from_dict():
    attribute = Attribute.from_dict({"name": "name", "value": "value"})
    assert attribute.name == "name"
    assert attribute.value == "value"


def test_attributes_collection_get(attributes_collection):
    assert attributes_collection.get("name-1").value == "value-1"
    assert attributes_collection.get("name-2").value == "value-2"
    assert attributes_collection.get("name-3") is None


@pytest.mark.parametrize(
    "name, value",
    [("name-1", "new-value-1"), ("name-3", "value-3")],
    ids=["update-existing", "add-new"],
)
def test_attributes_collection_set(attributes_collection, name, value):
    attributes_collection.set(name, value)
    assert attributes_collection.get(name).value == value


def test_attributes_collection_set_read_only(attributes_collection):
    attributes_collection.read_only = True
    with pytest.raises(ValueError) as err:
        attributes_collection.set("name-1", "new-value-1")

    assert str(err.value) == "Cannot set attribute values in a read-only collection"


def test_attributes_collection_to_dicts(attributes_collection):
    assert attributes_collection.to_dicts() == [
        {"name": "name-1", "value": "value-1"},
        {"name": "name-2", "value": "value-2"},
    ]


def test_attributes_collection_append(attributes_collection):
    attributes_collection.append(Attribute("name-3", "value-3"))
    assert attributes_collection.get("name-3").value == "value-3"
    assert attributes_collection[-1].name == "name-3"
    assert attributes_collection[-1].value == "value-3"

    assert attributes_collection.get("name-3").value == "value-3"


def test_attributes_collection_append_different_type(attributes_collection):
    attributes_collection.read_only = True
    with pytest.raises(ValueError) as err:
        attributes_collection.append("new-attribute")

    assert str(err.value) == (
        "Only Attribute objects can be appended to an AttributesCollection"
    )


def test_attributes_collection_extend(attributes_collection):
    attributes_collection.extend(
        [Attribute("name-3", "value-3"), Attribute("name-4", "value-4")]
    )
    assert attributes_collection.get("name-3").value == "value-3"
    assert attributes_collection.get("name-4").value == "value-4"


def test_attributes_collection_extend_not_attributes(attributes_collection):
    with pytest.raises(ValueError) as err:
        attributes_collection.extend(
            [
                Attribute("name-3", "value-3"),
                Attribute("name-4", "value-4"),
                "new-attribute",
            ]
        )

    assert str(err.value) == (
        "Only lists of Attribute objects can be used to extend an AttributesCollection"
    )


def test_attributes_collection_insert(attributes_collection):
    attribute = Attribute("name-0", "value-0")
    attributes_collection.insert(1, Attribute("name-0", "value-0"))
    assert attributes_collection[1] == attribute
    assert attributes_collection.get("name-0").value == "value-0"


def test_attributes_collection_insert_not_attribute(attributes_collection):
    with pytest.raises(ValueError) as err:
        attributes_collection.insert(1, "new-attribute")

    assert str(err.value) == (
        "Only Attribute objects can be inserted into an AttributesCollection"
    )


def test_attributes_collection_clone(attributes_collection):
    cloned = attributes_collection.clone(read_only=True)
    assert cloned.read_only is True
    assert cloned == attributes_collection
    assert cloned is not attributes_collection


def test_attributes_collection_delete(attributes_collection):
    attributes_collection.delete(0)
    assert attributes_collection.get("name-1") is None
    assert len(attributes_collection) == 1


def test_attributes_collection_pop(attributes_collection):
    result = attributes_collection.pop()
    assert result.name == "name-2"
    assert result.value == "value-2"
    assert len(attributes_collection) == 1
    assert attributes_collection.get("name-2") is None
