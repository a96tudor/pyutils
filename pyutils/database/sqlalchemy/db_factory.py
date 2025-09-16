import time
import urllib.parse
import uuid
from typing import Any, Optional, Union
from urllib.parse import quote

import sqlalchemy
import sqlalchemy.engine
import sqlalchemy.orm
import sqlalchemy.pool
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm.scoping import ScopedSession

from pyutils.config.providers import ConfigProvider
from pyutils.config.secrets import SecretValues
from pyutils.database.sqlalchemy.errors import (
    SqlAlchemyConnectionError,
    SQLAlchemySessionError,
)
from pyutils.database.sqlalchemy.query import RetryingBaseQuery
from pyutils.decorators.retry_connection import retry_connection
from pyutils.decorators.singleton import singleton
from pyutils.logging.logger import Logger


def DBFactory(logger_: Logger):
    class DB:
        """intended to be replacement for Flask-SQLAlchemy's db (SQLAlchemy())"""

        Model = declarative_base()
        metadata = Model.metadata
        session: Optional[sqlalchemy.orm.scoping.ScopedSession] = None
        engine: Union[sqlalchemy.engine.Engine, sqlalchemy.engine.Connection, None] = (
            None
        )
        logger: Logger = logger_

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
                    cls.logger,
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


@singleton
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


def __get_connection_string(
    config_path: list, provider: ConfigProvider, db_name: str = None
) -> Optional[str]:
    config: SecretValues = provider.provide(config_path)
    connection_string = None
    with config.unlock() as db_config:
        db_config = db_config.secret
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
        connect_string_args["username"] = db_config.get("username")

        # XXX: CRITICAL: Escape special characters otherwise connection will fail
        _pass = connect_string_args["password"]
        connect_string_args["password"] = quote(_pass)
        connect_string_args["host"] = quote(db_config.get("host"))

        connection_string = "{dialect}://{username}:{password}@{host}:{port}/".format(
            **connect_string_args
        )

        connection_string += db_name if db_name else db_config.get("database", "")

        query_params = connect_string_args.get("query_params")
        if query_params:
            # URL encode query_params, so the configs storing them are easily readable
            query_params = urllib.parse.urlencode(query_params)
            connection_string = f"{connection_string}?{query_params}"

    return connection_string


@retry_connection(retry_count=3, delay=5)
def get_session(
    logger: Logger,
    config_path: list,
    provider: ConfigProvider = None,
    db_name: str = None,
    engine_args: Optional[dict] = None,
    session_args: Optional[dict] = None,
) -> ScopedSession:
    if engine_args is None:
        engine_args = {}
    if session_args is None:
        session_args = {}

    _session_args = {
        # Default List
        "autoflush": False,
        "autocommit": False,
        "expire_on_commit": True,
        "query_cls": RetryingBaseQuery,  # Uncomment to use DB Query retry method
        "info": {},  # arbitrary data to be associated with this Session Obj
    }
    _session_args.update(session_args)

    curr_time = time.time()
    invoke_id = str(uuid.uuid4())
    invoke_ref = f"{curr_time}_{invoke_id}"
    _session_args["info"]["ident_db_session"] = invoke_ref
    _session_args["info"]["ident_db_config"] = str(config_path)
    _session_args["info"]["ident_db_name"] = db_name

    # Crtical this is needed for tracing to the source, if the DB session keeps failing
    #   even after rollback. Essentially all new connection may have a error of
    #   `Can't reconnect until invalid transaction is rolled back`
    logger.debug(
        {
            "ident_db_sess_obj": invoke_ref,
            "ident_db_config": str(config_path),
            "ident_db_name": db_name,
            "message": (
                f"Creating new DB Session for '{config_path}', DB_NAME: '{db_name}'."
                f" DB Session Obj Ident: '{invoke_ref}'"
            ),
        }
    )

    connection_string = __get_connection_string(
        config_path,
        provider=provider,
        db_name=db_name,
    )
    engine = sqlalchemy.create_engine(
        connection_string, paramstyle="format", **engine_args
    )
    factory = sqlalchemy.orm.sessionmaker(bind=engine, **_session_args)
    session = sqlalchemy.orm.scoped_session(factory)
    return session


def get_session_manager(session_name: str) -> _SessionManager:
    session_manager = _SessionManager(identifier=session_name)

    return session_manager


def shutdown_engine(sess_mgr_obj: _SessionManager, exc):
    # Teardown old session
    teardown_session(sess_mgr_obj, exc)

    shutdown_exc = []
    try:
        sess_mgr_obj.shutdown_engine()
    except Exception as exc:
        shutdown_exc.append(exc)

    if shutdown_exc:
        raise SQLAlchemySessionError(
            "Exception ocurred while Shutting down"
            f" SessionManager DB Session Engine: {shutdown_exc}"
        )


def teardown_session(sess_mgr_obj: _SessionManager, exc):
    try:
        # Things should have already been committed before closing the connection
        # If no transaction is in progress, this method is a pass-through.
        sess_mgr_obj.session.rollback()
    except Exception:
        pass

    teardown_exc = []
    try:
        sess_mgr_obj.teardown_session(exc=exc)
    except Exception as exc:
        teardown_exc.append(exc)

    if teardown_exc:
        raise SQLAlchemySessionError(
            "Exception ocurred while Tearing down"
            f" SessionManager DB Session: {teardown_exc}"
        )
