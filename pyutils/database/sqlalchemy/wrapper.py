from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, List, Optional, Type, Union

from psycopg2 import OperationalError
from sqlalchemy import Column
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.session import Session

from pyutils.config.providers import ConfigProvider
from pyutils.database.sqlalchemy.db_factory import DBFactory, get_session
from pyutils.database.sqlalchemy.errors import SqlAlchemyError
from pyutils.database.sqlalchemy.filters import CountFilter, Filter, TupleInFilter
from pyutils.database.sqlalchemy.joins import Join
from pyutils.datatools.attributes import Attribute
from pyutils.helpers.errors import BadArgumentsError, DataValidationError
from pyutils.logging.logger import Logger


class DBWrapper:
    def __init__(self, logger: Logger):
        self.logger = logger

    class GetResultType(Enum):
        ALL = "ALL"
        ONE = "ONE"
        ONE_OR_NONE = "ONE_OR_NONE"
        FIRST = "FIRST"
        QUERY_ONLY = "QUERY_ONLY"
        DISTINCT = "DISTINCT"

    RETURN_FUNCTIONS = {
        GetResultType.ALL: lambda query: query.all(),
        GetResultType.ONE: lambda query: query.one(),
        GetResultType.ONE_OR_NONE: lambda query: query.one_or_none(),
        GetResultType.FIRST: lambda query: query.first(),
        GetResultType.QUERY_ONLY: lambda query: query,
        GetResultType.DISTINCT: lambda query: query.distinct(),
    }

    @property
    def current_datetime(self) -> datetime:
        return datetime.now(timezone.utc)

    @property
    def db_name(self) -> str:
        with self._config_provider.provide(
            self._config_secret_route
        ).unlock() as config:
            return config.secret.get("database")

    @property
    @abstractmethod
    def _expire_db_factory(self) -> DBFactory:
        raise NotImplementedError()

    @property
    @abstractmethod
    def _no_expire_db_factory(self) -> DBFactory:
        raise NotImplementedError()

    @property
    @abstractmethod
    def _config_secret_route(self) -> [str]:
        raise NotImplementedError

    @property
    def _schema(self) -> str:
        with self._config_provider.provide(
            self._config_secret_route
        ).unlock() as config:
            return config.secret.get("schema")

    @property
    @abstractmethod
    def _config_provider(self) -> ConfigProvider:
        raise NotImplementedError()

    def _get_db_factory(self, expire_on_commit: bool) -> DBFactory:
        if expire_on_commit:
            return self._expire_db_factory

        return self._no_expire_db_factory

    def _run_query_safe(
        self,
        return_func: Callable,
        query: Query,
        retry_on_error: Optional[Type[Exception]] = None,
    ) -> Union[DeclarativeBase, List[DeclarativeBase]]:
        if retry_on_error:
            try:
                return return_func(query)
            except retry_on_error as e:
                self.logger.warning(
                    f"Error running query {query} with return function "
                    f"{return_func}: {e}",
                    exc_info=True,
                )
                return return_func(query)
        else:
            return return_func(query)

    @contextmanager
    def session_scope(self, expire_on_commit: bool = False) -> Session:
        session = get_session(
            self.logger,
            self._config_secret_route,
            self._config_provider,
            db_name=self.db_name,
            session_args={"expire_on_commit": expire_on_commit},
            # schema=self._schema,
        )
        if session is None:
            raise SqlAlchemyError("Failed to establish a database connection.")
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise

    @contextmanager
    def safe_session_scope(
        self, message: Optional[str] = None, expire_on_commit: bool = False
    ) -> Session:
        try:
            with self.session_scope(expire_on_commit) as session:
                yield session
        except Exception as e:

            self.logger.error(e, exc_info=True)
            # Use the beppy error rather than specific DB errors to prevent leaking
            # DB specifics to users
            raise SqlAlchemyError(message)

    def _initiate_query(
        self, session: Session, model_class: DeclarativeBase, columns: [Column]
    ) -> Query:
        if len(columns) > 0:
            query = session.query()
            query = query.with_entities(*columns)
            query = query.select_from(model_class)
        else:
            query = session.query(model_class)

        return query

    def _get_with_filters(
        self,
        model_class: DeclarativeBase,
        filters: List[Filter],
        columns: Optional[List[Column]] = None,
        joins: Optional[List[Join]] = None,
        order_by: Optional[dict] = None,
        error_message: Optional[str] = None,
        at_least_one_filter: Optional[bool] = False,
        limit: Optional[int] = None,
        return_type: Optional[GetResultType] = GetResultType.FIRST,
    ) -> Union[Query, DeclarativeBase, List[DeclarativeBase]]:
        processed_parameters = self._process_get_with_filters_parameters(
            model_class,
            filters,
            at_least_one_filter,
            columns,
            joins,
            error_message,
            order_by,
        )
        filters, columns, joins, order_by, error_message = processed_parameters

        with self.safe_session_scope(error_message) as session:
            query = self._initiate_query(session, model_class, columns)
            query = self._complete_query(query, joins, filters, order_by, limit)
            result = self._run_query_safe(
                self.RETURN_FUNCTIONS[return_type], query, OperationalError
            )

        return result

    def _complete_query(
        self, query: Query, joins: List[Join], filters: list, order_by: Any, limit: int
    ) -> Query:
        for join in joins:
            query = join.apply_to_query(query)
        if len(filters) > 0:
            query = query.filter(*filters)
        if order_by:
            query = query.order_by(order_by())
        if limit:
            query = query.limit(limit)

        return query

    def _process_get_with_filters_parameters(
        self,
        model_class: DeclarativeBase,
        filters: List[Filter],
        at_least_one_filter: bool,
        columns: Optional[List[Column]] = None,
        joins: Optional[List[Join]] = None,
        error_message: Optional[str] = None,
        order_by: Optional[dict] = None,
    ):
        if error_message is None:
            error_message = f"Error getting {model_class}"
        if joins is None:
            joins = []
        if columns is None:
            columns = []

        filters = self._process_filters(filters, error_message, at_least_one_filter)

        if order_by:
            order_by = self._process_order_by(model_class, order_by)

        return filters, columns, joins, order_by, error_message

    def _process_filters(
        self,
        filters: List[Filter],
        error_message: str,
        at_least_one_filter: Optional[bool] = False,
    ) -> list:
        processed_filters = []
        for f in filters:
            if not f.should_use():
                continue
            processed_filters.append(f.process())

        if at_least_one_filter and len(processed_filters) == 0:
            raise DataValidationError(
                f"{error_message} At least one filter must be specified"
            )

        return processed_filters

    def _process_order_by(self, model_class, order_by):
        raw_field = order_by.get("field")
        raw_direction = order_by.get("direction")
        if not raw_field or not raw_direction:
            raise BadArgumentsError(
                "Invalid arguments for order_by field. "
                "Must pass both field and direction"
            )
        field = getattr(model_class, raw_field, None)
        direction = str(raw_direction).lower()
        if not field:
            raise BadArgumentsError(f"Invalid argument for order_by.field: {raw_field}")
        # TODO: Add check for direction as well, to be valid
        return getattr(field, direction)

    def _create_and_upsert_model(
        self, model_class: DeclarativeBase, **kwargs
    ) -> DeclarativeBase:
        model = model_class(**kwargs)
        return self._upsert_model(model)

    def _upsert_model(
        self, model: DeclarativeBase, error_message: Optional[str] = None
    ) -> DeclarativeBase:
        return self._upsert_models([model], error_message)[0]

    def _upsert_models(
        self, models: [DeclarativeBase], error_message: Optional[str] = None
    ) -> [DeclarativeBase]:
        if error_message is None or error_message == "":
            error_message = f"Error upserting {models}"
        with self.safe_session_scope(error_message) as session:
            # TODO: Take a look at using session.add()
            result = []
            for model in models:
                result.append(session.merge(model))

        return result

    def _create_models(
        self, models: [DeclarativeBase], error_message: Optional[str] = None
    ) -> [DeclarativeBase]:
        """Create multiple models in one session.

        Parameters
        ----------
        models: list[DeclarativeBase]
            List of model instances to create.
        error_message: str, optional
            Custom error message used when the creation fails.

        Returns
        -------
        list[DeclarativeBase]
            The created models.
        """

        if error_message is None or error_message == "":
            error_message = f"Error creating {models}"
        with self.safe_session_scope(error_message) as session:
            session.add_all(models)

        return models

    def _delete_model(
        self, model: DeclarativeBase, error_message: Optional[str] = None
    ) -> int:
        """Delete a single model from the session.

        Parameters
        ----------
        model: DeclarativeBase
            The model instance to delete.
        error_message: str, optional
            Custom error message used when the delete fails.

        Returns
        -------
        int
            Number of deleted models. Always ``1`` if no exception is raised.
        """

        return self._delete_models([model], error_message)

    def _delete_models(
        self, models: [DeclarativeBase], error_message: Optional[str] = None
    ) -> int:
        """Delete multiple models in one session.

        Parameters
        ----------
        models: list[DeclarativeBase]
            List of model instances to delete.
        error_message: str, optional
            Custom error message used when the delete fails.

        Returns
        -------
        int
            Number of deleted models.
        """

        if error_message is None or error_message == "":
            error_message = f"Error deleting {models}"
        with self.safe_session_scope(error_message) as session:
            deleted = 0
            for model in models:
                session.delete(model)
                deleted += 1

        return deleted


class DBWrapperWithSubQueries(DBWrapper, ABC):
    def _get_attributes_filters(self, model: DeclarativeBase, attributes: [Attribute]):
        tuple_filter = TupleInFilter(
            (model.name, model.value), [(attr.name, attr.value) for attr in attributes]
        )
        # TODO: Check why we are doing this count here.
        count_filter = CountFilter(model.name, len(attributes))

        tuple_filter, count_filter = self._process_filters(
            [tuple_filter, count_filter], "Error building subquery filters"
        )

        return tuple_filter, count_filter

    def _get_attributes_group_by_subquery(
        self,
        session: Session,
        model: DeclarativeBase,
        column: Column,
        attributes: [Attribute],
    ) -> Query:
        tuple_filter, count_filter = self._get_attributes_filters(model, attributes)
        return (
            session.query(column)
            .filter(tuple_filter)
            .group_by(column)
            .having(count_filter)
            .subquery()
        )

    def _get_with_filters_and_attributes(
        self,
        model_class: DeclarativeBase,
        subquery_column: Column,
        attributes: [Attribute],
        filters: List[Filter],
        subquery_model: Optional[DeclarativeBase] = None,
        columns: Optional[List[Column]] = None,
        joins_before_subquery: Optional[List[Join]] = None,
        joins: Optional[List[Join]] = None,
        order_by: Optional[dict] = None,
        error_message: Optional[str] = None,
        at_least_one_filter: Optional[bool] = False,
        limit: Optional[int] = None,
        return_type: Optional[DBWrapper.GetResultType] = None,
        expire_on_commit: Optional[bool] = True,
    ):
        processed_parameters = self._process_get_with_filters_parameters(
            model_class,
            filters,
            at_least_one_filter,
            columns,
            joins,
            error_message,
            order_by,
        )
        filters, columns, joins, order_by, error_message = processed_parameters
        if joins_before_subquery is None:
            joins_before_subquery = []
        if subquery_model is None:
            subquery_model = model_class
        if return_type is None:
            return_type = self.GetResultType.FIRST

        with self.safe_session_scope(
            error_message, expire_on_commit=expire_on_commit
        ) as session:
            query = self._initiate_query(session, model_class, columns)
            if attributes:
                for join in joins_before_subquery:
                    query = join.apply_to_query(query)
                query = query.join(
                    self._get_attributes_group_by_subquery(
                        session, subquery_model, subquery_column, attributes
                    )
                )
            query = self._complete_query(query, joins, filters, order_by, limit)

            result = self._run_query_safe(
                self.RETURN_FUNCTIONS[return_type], query, OperationalError
            )

        return result
