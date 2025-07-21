from typing import Optional

from sqlalchemy import Column
from sqlalchemy.orm import DeclarativeBase, Relationship
from sqlalchemy.orm.query import Query


class Join:
    def __init__(
        self,
        model_class: DeclarativeBase,
        first_column: Optional[Column] = None,
        second_column: Optional[Column] = None,
    ):
        self.model_class = model_class
        self.first_column = first_column
        self.second_column = second_column

    def apply_to_query(self, query: Query) -> Query:
        if self.first_column is not None and self.second_column is not None:
            return self._apply_join(
                query, self.model_class, self.first_column == self.second_column
            )

        if self.first_column is not None:
            return self._apply_join(query, self.model_class, self.first_column)

        return self._apply_join(query, self.model_class)

    def _apply_join(self, query: Query, *args) -> Query:
        return query.join(*args)


class OuterJoin(Join):
    def _apply_join(self, query: Query, *args) -> Query:
        return query.outerjoin(*args)


class RelationshipJoin(Join):
    def __init__(self, relationship: Relationship):
        super().__init__(None, None, None)
        self.relationship = relationship

    def apply_to_query(self, query: Query) -> Query:
        return query.join(self.relationship)


class OuterRelationshipJoin(RelationshipJoin):
    def apply_to_query(self, query: Query) -> Query:
        return query.outerjoin(self.relationship)
