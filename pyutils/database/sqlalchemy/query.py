import time

from sqlalchemy.exc import OperationalError
from sqlalchemy.orm.query import Query as OrmQuery

from pyutils.database.sqlalchemy.constants import KNOWN_DB_ERRORS


class RetryingBaseQuery(OrmQuery):
    """
    Adapted from:
        https://stackoverflow.com/questions/53287215/
        retry-failed-sqlalchemy-queries/60614707#60614707
    """

    __MAX_RETRIES__ = 3

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __iter__(self):
        # Only compatible with SQLAlchemy version <= 1.3.xx
        iter_func = getattr(super(), "__iter__")
        resp = self._execute_transaction(iter_func)
        return resp

    def _iter(self):
        # Only compatible with SQLAlchemy version >= 1.4.xx
        # new style execution.
        iter_func = getattr(super(), "_iter")
        resp = self._execute_transaction(iter_func)
        return resp

    def _execute_transaction(self, iter_func):
        attempts = 0
        max_attempts = self.__MAX_RETRIES__

        # DB Session object identifier
        _db_sess_info = {}
        try:
            if isinstance(self.session.info, dict):
                _db_sess_info = self.session.info
        except Exception:
            pass
        for attempts in range(1, max_attempts + 1):
            try:
                return iter_func()
            except Exception as exc:
                exc_str = ""
                try:
                    exc_str = str(exc).lower()
                except Exception:
                    pass

                if (
                    isinstance(exc, OperationalError)
                    and attempts < max_attempts
                    and (
                        "server closed the connection unexpectedly" in exc_str
                        or "ssl connection has been closed unexpectedly" in exc_str
                        or (
                            # XXX: TODO: is this safe way to catch similar errors?
                            "connection" in exc_str
                            and "closed" in exc_str
                            and "unexpectedly" in exc_str
                        )
                    )
                ):
                    sleep_for = 2**attempts  # exponential backoff, power of 2

                    time.sleep(sleep_for)
                    continue

                # No retries left, act accordingly
                should_rollback = any(err in exc_str for err in KNOWN_DB_ERRORS)

                if should_rollback:
                    # XXX: Should we roll back and raise the error, to be ACID complaint
                    #   this could happen if DB connection got lost while the
                    #   transaction was running
                    #   OR
                    #   Roll back session and continue forward?

                    # Roll back this session, so we can Reconnect to the DB
                    try:
                        self.session.rollback()
                    except Exception:
                        pass

                # Expire all objects in this session since transaction failed
                try:
                    self.session.expire_all()
                except Exception:
                    pass

                # Attempt to close the session since transaction failed
                try:
                    self.session.close()
                except Exception:
                    pass

                # Remove the session since transaction failed
                try:
                    self.session.remove()
                except Exception:
                    pass

                # Unset the session since transaction failed
                try:
                    self.session = None
                except Exception:
                    pass

                # Raise the error, since the transaction may be incomplete
                raise
