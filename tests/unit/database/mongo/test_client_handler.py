from unittest.mock import Mock

import pytest

from pyutils.database.mongo.client_handler import MongoClientHandler
from pyutils.database.mongo.errors import (
    MongoDbCollectionDroppedError,
    MongoDbDeleteError,
    MongoDbInsertBatchError,
    MongoDbInsertOneError,
    MongoDbUpdateError,
)

IMPORT_PATH = "pyutils.database.mongo.client_handler"


@pytest.fixture
def mocked_error_json(mocker):
    return mocker.patch("pyutils.database.mongo.errors.json")


@pytest.fixture
def mocked_parameters():
    return [Mock(), Mock(), Mock()]


@pytest.fixture
def mocked_mongo_client(mocker, mocked_parameters):
    [_, database_name, collection_name] = mocked_parameters

    return mocker.patch(
        f"{IMPORT_PATH}.MongoClient",
        return_value={database_name: {collection_name: Mock()}},
    )


@pytest.fixture
def mocked_collection_handler(
    mocked_mongo_client, mocked_parameters, mock_operation_result
):
    [_, database_name, collection_name] = mocked_parameters

    mock = mocked_mongo_client.return_value[database_name][collection_name]

    mock.insert_one.return_value = mock_operation_result
    mock.insert_many.return_value = mock_operation_result
    mock.update_one.return_value = mock_operation_result
    mock.update_many.return_value = mock_operation_result
    mock.delete_one.return_value = mock_operation_result
    mock.delete_many.return_value = mock_operation_result

    return mock


@pytest.fixture
def mocked_certifi(mocker):
    return mocker.patch(f"{IMPORT_PATH}.certifi")


@pytest.fixture
def mock_operation_result():
    mock = Mock()
    mock.acknowledged = True

    return mock


@pytest.fixture
def mocked_handler_connect(mocker):
    return mocker.patch.object(MongoClientHandler, "connect")


@pytest.fixture
def client_handler(
    mocked_parameters, mocked_handler_connect, mocked_collection_handler
):
    client_handler = MongoClientHandler(*mocked_parameters)
    client_handler.collection_handler = mocked_collection_handler

    return client_handler


@pytest.fixture
def mocked_check_if_operation_can_be_run(mocker):
    return mocker.patch.object(MongoClientHandler, "_check_if_operation_can_be_run")


def test_client_handler_init(mocked_parameters, mocked_handler_connect, client_handler):
    [connection_string, database_name, collection_name] = mocked_parameters

    assert client_handler.database_name == database_name
    assert client_handler.collection_name == collection_name
    mocked_handler_connect.assert_called_once_with(
        connection_string, database_name, collection_name
    )


def test_client_handler_connect(mocked_certifi, mocked_mongo_client, mocked_parameters):
    [connection_string, database_name, collection_name] = mocked_parameters

    client_handler = MongoClientHandler(*mocked_parameters)

    mocked_mongo_client.assert_called_once_with(
        connection_string,
        tlsCAFile=mocked_certifi.where.return_value,
    )

    assert (
        client_handler.database_handler
        == mocked_mongo_client.return_value[database_name]
    )
    assert client_handler.collection_handler == (
        mocked_mongo_client.return_value[database_name][collection_name]
    )

    assert client_handler.collection_dropped is False


def test_insert_one(client_handler, mocked_collection_handler):
    data = Mock()

    result = client_handler.insert_one(data)

    mocked_collection_handler.insert_one.assert_called_once_with(data)

    assert result == str(mocked_collection_handler.insert_one.return_value.inserted_id)


def test_insert_one_error(
    client_handler, mocked_collection_handler, mock_operation_result
):
    data = Mock()
    mock_operation_result.acknowledged = False

    with pytest.raises(MongoDbInsertOneError) as err:
        _ = client_handler.insert_one(data)

    mocked_collection_handler.insert_one.assert_called_once_with(data)
    assert err.value.database_name == client_handler.database_name
    assert err.value.collection_name == client_handler.collection_name


def test_insert_batch(client_handler, mocked_collection_handler):
    data = [Mock(), Mock()]
    mocked_collection_handler.insert_many.return_value.inserted_ids = [Mock(), Mock()]

    result = client_handler.insert_batch(data)

    mocked_collection_handler.insert_many.assert_called_once_with(data)

    assert result == [
        str(x) for x in mocked_collection_handler.insert_many.return_value.inserted_ids
    ]


def test_insert_batch_error(
    client_handler, mocked_collection_handler, mock_operation_result
):
    data = [Mock(), Mock()]
    mock_operation_result.acknowledged = False

    with pytest.raises(MongoDbInsertBatchError) as err:
        _ = client_handler.insert_batch(data)

    mocked_collection_handler.insert_many.assert_called_once_with(data)
    assert err.value.database_name == client_handler.database_name
    assert err.value.collection_name == client_handler.collection_name


def test_find_many(
    client_handler,
    mocked_collection_handler,
    mocked_check_if_operation_can_be_run,
):
    query = Mock()
    limit = Mock()
    projection = Mock()

    result = client_handler.find_many(query, limit, projection)

    mocked_check_if_operation_can_be_run.assert_called_once()
    mocked_collection_handler.find.assert_called_once_with(
        filter=query,
        limit=limit,
        projection=projection,
    )

    assert result == mocked_collection_handler.find.return_value


def test_find_one(
    client_handler,
    mocked_collection_handler,
    mocked_check_if_operation_can_be_run,
):
    query = Mock()
    projection = Mock()

    result = client_handler.find_one(query, projection)

    mocked_check_if_operation_can_be_run.assert_called_once()
    mocked_collection_handler.find_one.assert_called_once_with(
        filter=query,
        projection=projection,
    )

    assert result == mocked_collection_handler.find_one.return_value


def test_update_one(
    client_handler,
    mocked_collection_handler,
    mocked_check_if_operation_can_be_run,
):
    query = Mock()
    new_values = Mock()

    result = client_handler.update_one(query, new_values)

    mocked_check_if_operation_can_be_run.assert_called_once()
    mocked_collection_handler.update_one.assert_called_once_with(
        query,
        {"$set": new_values},
    )

    assert result == mocked_collection_handler.update_one.return_value.modified_count


def test_update_one_error(
    client_handler,
    mocked_collection_handler,
    mock_operation_result,
    mocked_check_if_operation_can_be_run,
    mocked_error_json,
):
    query = Mock()
    new_values = Mock()
    mock_operation_result.acknowledged = False

    with pytest.raises(MongoDbUpdateError) as err:
        _ = client_handler.update_one(query, new_values)

    mocked_check_if_operation_can_be_run.assert_called_once()
    mocked_collection_handler.update_one.assert_called_once_with(
        query,
        {"$set": new_values},
    )
    assert err.value.database_name == client_handler.database_name
    assert err.value.collection_name == client_handler.collection_name
    assert err.value.query == query
    assert err.value.new_values == new_values


def test_update_all(
    client_handler,
    mocked_collection_handler,
    mocked_check_if_operation_can_be_run,
):
    query = Mock()
    new_values = Mock()

    result = client_handler.update_all(query, new_values)

    mocked_check_if_operation_can_be_run.assert_called_once()
    mocked_collection_handler.update_many.assert_called_once_with(
        query,
        {"$set": new_values},
    )

    assert result == mocked_collection_handler.update_many.return_value.modified_count


def test_update_all_error(
    client_handler,
    mocked_collection_handler,
    mock_operation_result,
    mocked_check_if_operation_can_be_run,
    mocked_error_json,
):
    query = Mock()
    new_values = Mock()
    mock_operation_result.acknowledged = False

    with pytest.raises(MongoDbUpdateError) as err:
        _ = client_handler.update_all(query, new_values)

    mocked_check_if_operation_can_be_run.assert_called_once()
    mocked_collection_handler.update_many.assert_called_once_with(
        query,
        {"$set": new_values},
    )
    assert err.value.database_name == client_handler.database_name
    assert err.value.collection_name == client_handler.collection_name
    assert err.value.query == query
    assert err.value.new_values == new_values


def test_delete_one(
    client_handler,
    mocked_collection_handler,
    mocked_check_if_operation_can_be_run,
):
    query = Mock()

    result = client_handler.delete_one(query)

    mocked_check_if_operation_can_be_run.assert_called_once()
    mocked_collection_handler.delete_one.assert_called_once_with(query)

    assert result == mocked_collection_handler.delete_one.return_value.deleted_count


def test_delete_one_error(
    client_handler,
    mocked_collection_handler,
    mock_operation_result,
    mocked_check_if_operation_can_be_run,
    mocked_error_json,
):
    query = Mock()
    mock_operation_result.acknowledged = False

    with pytest.raises(MongoDbDeleteError) as err:
        _ = client_handler.delete_one(query)

    mocked_check_if_operation_can_be_run.assert_called_once()
    mocked_collection_handler.delete_one.assert_called_once_with(query)

    assert err.value.database_name == client_handler.database_name
    assert err.value.collection_name == client_handler.collection_name
    assert err.value.query == query


def test_delete_all(
    client_handler,
    mocked_collection_handler,
    mocked_check_if_operation_can_be_run,
):
    query = Mock()

    result = client_handler.delete_all(query)

    mocked_check_if_operation_can_be_run.assert_called_once()
    mocked_collection_handler.delete_many.assert_called_once_with(query)

    assert result == mocked_collection_handler.delete_many.return_value.deleted_count


def test_delete_all_error(
    client_handler,
    mocked_collection_handler,
    mock_operation_result,
    mocked_check_if_operation_can_be_run,
    mocked_error_json,
):
    query = Mock()
    mock_operation_result.acknowledged = False

    with pytest.raises(MongoDbDeleteError) as err:
        _ = client_handler.delete_all(query)

    mocked_check_if_operation_can_be_run.assert_called_once()
    mocked_collection_handler.delete_many.assert_called_once_with(query)

    assert err.value.database_name == client_handler.database_name
    assert err.value.collection_name == client_handler.collection_name
    assert err.value.query == query


@pytest.mark.parametrize(
    "already_dropped",
    [True, False],
    ids=["Connection already dropped", "Connection not dropped previously"],
)
def test_drop_collection(client_handler, mocked_collection_handler, already_dropped):
    client_handler.collection_dropped = already_dropped

    client_handler.drop_collection()

    assert client_handler.collection_dropped is True
    if already_dropped:
        mocked_collection_handler.drop.assert_not_called()
    else:
        mocked_collection_handler.drop.assert_called_once()


@pytest.mark.parametrize(
    "collection_dropped",
    [True, False],
    ids=["Collection dropped", "Collection not dropped"],
)
def test_check_if_operation_can_be_run(client_handler, collection_dropped):
    client_handler.collection_dropped = collection_dropped

    if not collection_dropped:
        client_handler._check_if_operation_can_be_run()
    else:
        with pytest.raises(MongoDbCollectionDroppedError) as err:
            client_handler._check_if_operation_can_be_run()
        assert err.value.database_name == client_handler.database_name
        assert err.value.collection_name == client_handler.collection_name
