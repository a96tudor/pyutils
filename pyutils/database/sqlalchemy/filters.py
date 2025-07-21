import abc
from abc import ABCMeta
from typing import Any, Iterable, List, Optional, Tuple

from sqlalchemy import Column, and_, func, or_, tuple_

from pyutils.helpers.errors import BadArgumentsError
from pyutils.helpers.uuid import validate_uuid, validate_uuids


class Filter:
    def __init__(self, column: Column, value: Any):
        self.column = column
        self.value = value

    @abc.abstractmethod
    def process(self): ...

    def should_use(self) -> bool:
        return self.value is not None


class EqualityFilter(Filter):
    def __init__(self, column: Column, value: Any, is_uuid: Optional[bool] = False):
        super().__init__(column, value)
        self.is_uuid = is_uuid

    def process(self):
        if self.is_uuid:
            validate_uuid(self.value)

        return self.column == self.value


class InListFilter(EqualityFilter):
    def __init__(
        self,
        column: Column,
        values: List[Any],
        is_uuid: Optional[bool] = False,
        negated: Optional[bool] = False,
        can_be_empty: Optional[bool] = False,
    ):
        super().__init__(column, values, is_uuid)
        if not isinstance(values, Iterable):
            raise BadArgumentsError(
                f"Expected values to be an iterable, got {type(values)}"
            )
        self.negated = negated
        self.can_be_empty = can_be_empty

    def process(self):
        if self.is_uuid:
            validate_uuids(self.value)
        if not self.negated:
            return self.column.in_(self.value)
        else:
            return self.column.not_in(self.value)

    def should_use(self) -> bool:
        return super().should_use() and (len(self.value) > 0 or self.can_be_empty)


class ComparisonFilter(Filter, metaclass=ABCMeta):
    def __init__(self, column: Column, value: Any, or_equal: Optional[bool] = False):
        super().__init__(column, value)
        self.or_equal = or_equal


class GreaterThanFilter(ComparisonFilter):
    def process(self):
        if not self.or_equal:
            return self.column > self.value
        else:
            return self.column >= self.value


class LessThanFilter(ComparisonFilter):
    def process(self):
        if not self.or_equal:
            return self.column < self.value
        else:
            return self.column <= self.value


class CountFilter(Filter):
    def __init__(self, column: Column, value: int):
        super().__init__(column, value)
        if not isinstance(value, int) and self.should_use():
            raise BadArgumentsError(
                f"Expected CountFilter value to be int, got {type(value)}"
            )

    def process(self):
        return func.count(self.column) == self.value


class TupleInFilter(InListFilter):
    def __init__(self, columns: Tuple, values: List[Tuple]):
        super().__init__(columns, values)
        if not isinstance(columns, tuple):
            raise BadArgumentsError(
                f"Expected columns to be a tuple, got {type(columns)}."
            )
        if not all(isinstance(v, tuple) for v in values):
            raise BadArgumentsError("Expected values to be a list of tuples")

    def process(self):
        return tuple_(*self.column).in_(self.value)


class BooleanFilter(Filter):
    def __init__(self, column: Column, value: bool, negated: Optional[bool] = False):
        super().__init__(column, value)
        self.negated = negated

    def process(self):
        if self.negated:
            return self.column.is_not(self.value)
        return self.column.is_(self.value)


class NoneFilter(BooleanFilter):
    def __init__(self, column: Column, negated: Optional[bool] = False):
        super().__init__(column, None, negated)

    def should_use(self) -> bool:
        return True


class CompositionFilter(Filter):
    def __init__(self, filters: List[Filter]):
        self.filters = filters

    @abc.abstractmethod
    def process(self): ...

    def should_use(self) -> bool:
        return all(f.should_use() for f in self.filters)


class AndFilter(CompositionFilter):
    def process(self):
        return and_(*[f.process() for f in self.filters])


class OrFilter(CompositionFilter):
    def process(self):
        return or_(*[f.process() for f in self.filters])


class LikeFilter(EqualityFilter):
    def process(self):
        return self.column.like(f"%{self.value}%")

    def should_use(self) -> bool:
        return super().should_use() and isinstance(self.value, str)


class EndsWithFilter(LikeFilter):
    def process(self):
        return self.column.like(f"%{self.value}")


class StartsWithFilter(LikeFilter):
    def process(self):
        return self.column.like(f"{self.value}%")
