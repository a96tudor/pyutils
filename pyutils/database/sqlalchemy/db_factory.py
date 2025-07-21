import time
import urllib.parse
import uuid
from typing import Any, Callable, Optional, Union
from urllib.parse import quote

import sqlalchemy
import sqlalchemy.engine
import sqlalchemy.orm
import sqlalchemy.pool
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm.query import Query as OrmQuery

from pyutils.config.providers import ConfigProvider
from pyutils.config.secrets import SecretValues
from pyutils.database.sqlalchemy.errors import SqlAlchemyConnectionError
from pyutils.logging.logger import LoggerBase


def DBFactory(logger_: LoggerBase):
    class DB:
        """intended to be replacement for Flask-SQLAlchemy's db (SQLAlchemy())"""

        Model = declarative_base()
        metadata = Model.metadata
        session: Optional[sqlalchemy.orm.scoping.ScopedSession] = None
        engine: Union[sqlalchemy.engine.Engine, sqlalchemy.engine.Connection, None] = (
            None
        )
        logger: LoggerBase = logger_

        @classmethod
        def get_tables_for_bind(cls, bind):
            return cls.Model.metadata.tables.values()

        @classmethod
        def _execute_for_all_tables(cls, operation, skip_tables=False):
            bind = cls.engine
            extra = {}

            if not skip_tables:
                tables = cls.get_tables_for_bind(bind)
                extra["tables"] = tables
            op = getattr(cls.Model.metadata, operation)
            op(bind=bind, **extra)

        @classmethod
        def create_all(cls):
            cls._execute_for_all_tables("create_all")

        @classmethod
        def drop_all(cls):
            cls._execute_for_all_tables("drop_all")

        @classmethod
        def set_engine(
            cls,
            config_path: list,
            provider: ConfigProvider,
            db_name: str = None,
            engine_args: Optional[dict] = None,
            session_args: Optional[dict] = None,
        ):
            try:
                cls.session = get_session(
                    config_path,
                    provider=provider,
                    db_name=db_name,
                    engine_args=engine_args,
                    session_args=session_args,
                )
                cls.engine = cls.session.bind
            except Exception as err:
                raise SqlAlchemyConnectionError(error=err)

        @classmethod
        def teardown_session(cls, exc: Optional[Any] = None):
            if not cls.session:
                return

            ident_db_session = ""
            ident_db_config = ""
            ident_db_name = ""
            try:
                ident_info = cls.session.info or {}
                ident_db_session = ident_info.get("ident_db_session") or ""
                ident_db_config = ident_info.get("ident_db_config") or ""
                ident_db_name = ident_info.get("ident_db_name") or ""
            except Exception:
                pass

            cls.logger.debug(
                "Attempting to Teardown DB Session created"
                f" for '{ident_db_config}', DB_NAME: '{ident_db_name}'."
                f" DB Session Obj Ident: '{ident_db_session}'"
            )
            if exc:
                # Roll back this session, so we can Reconnect to the DB
                cls.session.rollback()

            # Expire all objects in this session is being torndown
            try:
                cls.session.expire_all()
            except Exception:
                pass

            # Attempt to close the session since its being torndown
            try:
                cls.session.close()
            except Exception:
                pass

            # Remove the session since its being torndown
            try:
                cls.session.remove()
            except Exception:
                pass

        @classmethod
        def shutdown_engine(cls):
            ident_db_session = ""
            ident_db_config = ""
            ident_db_name = ""
            try:
                ident_info = cls.session.info or {}
                ident_db_session = ident_info.get("ident_db_session") or ""
                ident_db_config = ident_info.get("ident_db_config") or ""
                ident_db_name = ident_info.get("ident_db_name") or ""
            except Exception:
                pass
            cls.logger.debug(
                "Attempting to Shutdown DB Session Engine created"
                f" for '{ident_db_config}', DB_NAME: '{ident_db_name}'."
                f" DB Session Obj Ident: '{ident_db_session}'"
            )

            # Dispose the connections from the connection pool, thus releasing resources
            try:
                cls.engine.dispose(close=True)
            except Exception:
                pass

            # Unset the variables
            cls.session = None
            cls.engine = None

    # Get attributes from sqlalchemy that don't already exist in DBFactory
    sql_attrs = getattr(sqlalchemy, "__all__", [])
    if not sql_attrs:
        # SQLAlchemy v2 doesn't have an __all__ attribute so use dir to assign attrs
        sql_attrs = [
            attr
            for attr in dir(sqlalchemy)
            if not attr.startswith("_") and attr not in dir(DB)
        ]

    # Get attributes from sqlalchemy.orm that don't already exist in DBFactory
    orm_attrs = getattr(sqlalchemy.orm, "__all__", [])
    if not orm_attrs:
        # SQLAlchemy v2 doesn't have an __all__ attribute so use dir to assign attrs
        orm_attrs = [
            attr
            for attr in dir(sqlalchemy.orm)
            if not attr.startswith("_") and attr not in dir(DB)
        ]

    # Set attributes from sqlalchemy/sqlalchemy.orm to match Flask-SQLAlchemy
    for attr in sql_attrs:
        setattr(DB, attr, getattr(sqlalchemy, attr))
    for attr in orm_attrs:
        setattr(DB, attr, getattr(sqlalchemy.orm, attr))

    return DB


class _SessionManager:
    def __init__(self, *session_args, **session_kwargs):
        super().__init__()
        self.factory = DBFactory()
        self._session_args = session_args
        self._session_kwargs = session_kwargs

    def __getattr__(self, attr):
        if attr not in dir(self.__class__) and attr in dir(self.factory):
            return getattr(self.factory, attr)
        return self.__getattribute__(attr)

    @property
    def has_session(self):
        return True if getattr(self.factory, "session", None) else False

    @property
    def session(self):
        if not self.has_session:
            self.init_session()
        return self.factory.session

    def update_session_args(self, *session_args, **session_kwargs):
        """Updates session args on the next time session is created"""
        self._session_args = session_args
        self._session_kwargs = session_kwargs

    def init_session(self):
        self.factory.set_engine(*self._session_args, **self._session_kwargs)

    def expire_all_sessions(self):
        if not self.has_session:
            return
        try:
            self.factory.session.expire_all()
        except Exception:
            pass

    def teardown_session(self, exc: Optional[Any] = None):
        if not self.has_session:
            return

        self.factory.teardown_session(exc=exc)

    def shutdown_engine(self):
        if not getattr(self.factory, "engine", None):
            return

        self.factory.shutdown_engine()


def get_connection_string(
    config_path: list, provider: ConfigProvider, db_name: str = None
) -> Optional[str]:
    config: SecretValues = provider.provide(config_path)
    connection_string = None
    with config.unlock() as db_config:
        dialect_arg = (
            "+".join(
                [v for v in [db_config.get("dialect"), db_config.get("driver")] if v]
            )
            or "postgresql"
        )
        connect_string_args = {
            k: v for k, v in db_config.items() if k not in ["dialect", "driver"]
        }
        connect_string_args["dialect"] = dialect_arg

        # XXX: CRITICAL: Escape special characters otherwise connection will fail
        _pass = connect_string_args["password"]
        connect_string_args["password"] = quote(_pass)

        connection_string = ("{dialect}://{username}:{password}@{host}:{port}/").format(
            **connect_string_args
        )

        connection_string += db_name if db_name else db_config.get("database", "")

        query_params = connect_string_args.get("query_params")
        if query_params:
            # URL encode query_params, so the configs storing them are easily readable
            query_params = urllib.parse.urlencode(query_params)
            connection_string = f"{connection_string}?{query_params}"

    return connection_string
