import sys
import types
from contextlib import contextmanager
from unittest.mock import call

import pytest

# Stub external dependencies required by DBWrapper
dummy_module = types.ModuleType("database_factory")
dummy_module.DB = types.SimpleNamespace(Model=object, Column=object)
sys.modules["bepfo.clients.db.fo_store.database_factory"] = dummy_module
dummy_session_module = types.ModuleType("session_handling")
dummy_session_module.get_db_session = lambda *args, **kwargs: None
sys.modules["bepfo.clients.db.utils.session_handling"] = dummy_session_module
dummy_rds_module = types.ModuleType("rds")
dummy_rds_module.DBFactory = object
sys.modules["beppy.helpers.db.rds"] = dummy_rds_module

from pyutils.database.sqlalchemy.wrapper import DBWrapper


def _context_manager(session):
    @contextmanager
    def _cm(*args, **kwargs):
        yield session

    return _cm()


def test_delete_model(mocker):
    wrapper = DBWrapper(mocker.Mock())
    session = mocker.Mock()
    mocked_scope = mocker.patch.object(wrapper, "safe_session_scope")
    mocked_scope.return_value = _context_manager(session)
    model = mocker.Mock()

    result = wrapper._delete_model(model, "err")

    mocked_scope.assert_called_once_with("err")
    session.delete.assert_called_once_with(model)
    assert result == 1


def test_delete_models(mocker):
    wrapper = DBWrapper(mocker.Mock())
    session = mocker.Mock()
    mocked_scope = mocker.patch.object(wrapper, "safe_session_scope")
    mocked_scope.return_value = _context_manager(session)
    models = [mocker.Mock(), mocker.Mock()]

    result = wrapper._delete_models(models, "err")

    mocked_scope.assert_called_once_with("err")
    session.delete.assert_has_calls([call(models[0]), call(models[1])])
    assert result == len(models)

