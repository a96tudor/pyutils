from unittest.mock import Mock, call

import pytest

from pyutils.database.mongo.driver import MongoDriver

IMPORT_PATH = "pyutils.database.mongo.driver"


@pytest.fixture
def mocked_parameters():
    return [Mock(), Mock(), Mock()]


@pytest.fixture
def mocked_client_handler(mocker):
    return mocker.patch(f"{IMPORT_PATH}.MongoClientHandler")


@pytest.fixture
def mocked_connect_to(mocker):
    return mocker.patch.object(MongoDriver, "connect_to")


def test_init(mocked_connect_to, mocked_parameters):
    driver = MongoDriver(*mocked_parameters)

    mocked_connect_to.assert_called_once_with(*mocked_parameters)
    assert driver._connection_string == mocked_parameters[0]
    assert driver._database_name == mocked_parameters[1]
    assert driver._collection_name == mocked_parameters[2]


@pytest.mark.parametrize(
    "client_handler",
    [None, Mock()],
    ids=["Without pre-existing client handler", "With pre-existing client handler"],
)
def test_connect_to(mocked_parameters, mocked_client_handler, client_handler):
    driver = MongoDriver(*mocked_parameters)
    driver.client_handler = client_handler

    new_parameters = [Mock(), Mock(), Mock()]
    driver.connect_to(*new_parameters)

    if client_handler is None:
        mocked_client_handler.assert_has_calls(
            [call(*mocked_parameters), call(*new_parameters)],
        )
        driver.client_handler.connect.assert_not_called()
    else:
        mocked_client_handler.assert_called_once_with(*mocked_parameters)
        driver.client_handler.connect.assert_called_once_with(*new_parameters)

    assert driver._connection_string == new_parameters[0]
    assert driver._database_name == new_parameters[1]
    assert driver._collection_name == new_parameters[2]
