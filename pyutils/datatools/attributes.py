from typing import Any, Iterable, List, Optional

from typing_extensions import SupportsIndex

from pyutils.datatools.collection import Collection


class Attribute:
    def __init__(self, name: str, value: str):
        self.__name = name
        self.__value = value

    def __str__(self):
        return f"{self.name}: {self.value}"

    def __repr__(self):
        return f"Attribute({self.name}): {self.value}"

    @property
    def name(self) -> str:
        return self.__name

    @name.setter
    def name(self, name: str):
        self.__name = name

    @property
    def value(self) -> str:
        return self.__value

    @value.setter
    def value(self, value: str):
        self.__value = value

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "value": self.value,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Attribute":
        return cls(data["name"], data["value"])

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Attribute):
            return False

        return self.name == other.name and self.value == other.value


class AttributesCollection(Collection):
    __map: dict

    def __init__(self, values: Optional[Iterable[Attribute]] = None):
        super().__init__(values)

        self.__map = {}
        if values:
            self.__map = {attribute.name: attribute for attribute in values}

    def get(self, name: str) -> Optional[Attribute]:
        return self.__map.get(name)

    def set(self, name: str, value: str):
        if self.read_only:
            raise ValueError("Cannot set attribute values in a read-only collection")

        current_attribute = self.get(name)
        if current_attribute:
            current_attribute.value = value
        else:
            self.append(Attribute(name, value))

    def to_dicts(self) -> List[dict]:
        return [attribute.to_dict() for attribute in self]

    @classmethod
    def from_dicts(cls, data: List[dict]) -> "AttributesCollection":
        return cls([Attribute.from_dict(d) for d in data])

    def append(self, __object: Any):
        if not isinstance(__object, Attribute):
            raise ValueError(
                "Only Attribute objects can be appended to an AttributesCollection"
            )

        super().append(__object)
        self.__map[__object.name] = __object

    def extend(self, __iterable: Iterable):
        if any(not isinstance(item, Attribute) for item in __iterable):
            raise ValueError(
                "Only lists of Attribute objects can be used "
                "to extend an AttributesCollection"
            )

        super().extend(__iterable)

        for item in __iterable:
            self.__map[item.name] = item

    def insert(self, __index: SupportsIndex, __object: Any):
        if not isinstance(__object, Attribute):
            raise ValueError(
                "Only Attribute objects can be inserted into an AttributesCollection"
            )
        super().insert(__index, __object)
        self.__map[__object.name] = __object

    def clone(self, read_only: bool = False) -> "Collection":
        result = AttributesCollection(self)
        result.read_only = read_only

        return result

    def delete(self, __index: SupportsIndex):
        attribute = self[__index]
        super().delete(__index)
        del self.__map[attribute.name]

    def pop(self, __index: Optional[SupportsIndex] = None) -> Any:
        result = super().pop(__index)
        del self.__map[result.name]

        return result
