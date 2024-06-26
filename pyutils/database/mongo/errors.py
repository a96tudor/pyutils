import json


class MongoDbError(Exception):
    """Base class for a mongodb-related error."""

    def __init__(self, database_name: str, collection_name: str, message: str):
        """Base class for a mongodb-related error.

        Parameters
        ----------
        database_name: str
            The name of the database where the error occurred.
        collection_name: str
            The name of the collection where the error occurred.
        message: str
            An error message describing the error.

        """
        super().__init__(
            f"{message} when connected to database {database_name}, "
            f"collection {collection_name}"
        )
        self.database_name = database_name
        self.collection_name = collection_name


class MongoDbInsertOneError(MongoDbError):
    """Error occurring when the `insert_one` operation failed."""

    def __init__(self, database_name: str, collection_name: str):
        """Raise when the `insert_one` operation failed.

        Parameters
        ----------
        database_name: str
            The name of the database in which the client handler failed
            to insert.
        collection_name: str
            The name of the collection in which the client handler failed
            to insert.

        """
        super().__init__(
            database_name,
            collection_name,
            "Failed to insert one item in the collection",
        )


class MongoDbInsertBatchError(MongoDbError):
    """Error occurring when the `insert_batch` operation failed."""

    def __init__(
        self,
        database_name: str,
        collection_name: str,
        number_of_items: int,
    ):
        """Raise when the `insert_batch` operation failed.

        Parameters
        ----------
        database_name: str
            The name of the database in which the client handler failed
            to insert.
        collection_name: str
            The name of the collection in which the client handler failed
            to insert.
        number_of_items: int
            The number of items that were attempted to insert.

        """
        super().__init__(
            database_name,
            collection_name,
            f"Failed to insert {number_of_items} data points in the collection",
        )

        self.number_of_items = number_of_items


class MongoDbUpdateError(MongoDbError):
    """Error occurring when the `update` operation failed."""

    def __init__(
        self,
        database_name: str,
        collection_name: str,
        query: dict,
        new_values: dict,
    ):
        """Raise when the `update` operation failed.

        Parameters
        ----------
        database_name: str
            The name of the database where the client handler failed
            to update documents.
        collection_name: str
            The name of the collection where the client handler failed
            to update documents.
        query: dict
            Query used to identify the documents to update.
        new_values: dict
            Values to update.

        """
        super().__init__(
            database_name,
            collection_name,
            f"Could not update documents matching {json.dumps(query)} with "
            f"{json.dumps(new_values)}",
        )

        self.query = query
        self.new_values = new_values


class MongoDbDeleteError(MongoDbError):
    """Error occurring when the `delete` operation failed."""

    def __init__(self, database_name: str, collection_name: str, query: dict):
        """Raise when the `delete` operation failed.

        Parameters
        ----------
        database_name: str
            The database the client handler attempted to delete from.
        collection_name: str
            The collection the client handler attempted to delete from.
        query: dict
            Query used to identify the documents to delete.

        """
        super().__init__(
            database_name,
            collection_name,
            f"Failed to delete documents matching {json.dumps(query)}",
        )

        self.query = query


class MongoDbCollectionDroppedError(MongoDbError):
    """Error occurring when dropping a collection."""

    def __init__(self, database_name: str, collection_name: str):
        """Raise when dropping a collection fails.

        Parameters
        ----------
        database_name: str
            The database the client handler attempted to drop the collection
            from.
        collection_name: str
            The collection the client handler attempted to drop.

        """
        super().__init__(
            database_name,
            collection_name,
            "Cannot perform operation on dropped collection",
        )
