from typing import Iterable, List, Optional

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


class AttributesCollection(Collection):
    __map: dict = {}

    def __init__(self, values: Optional[Iterable[Attribute]] = None):
        super().__init__(values)

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
