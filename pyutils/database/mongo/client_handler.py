from typing import List, Optional

import certifi
import pymongo
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.database import Database

import pyutils.database.mongo.errors as err


class MongoClientHandler:

    def __init__(
        self, connection_string: str, database_name: str, collection_name: str
    ):
        """Connect to a specific collection in the mongodb cluster.

        Parameters
        ----------
        connection_string: str
            The connection string used to connect to the mongodb cluser.
        database_name: str
            Name of the dabase to be used.
        collection_name: str
            Name of the collection to be used.

        """
        self.client: pymongo.MongoClient = None

        self.database_handler: Database = None
        self.collection_handler: Collection = None

        self.database_name = database_name
        self.collection_name = collection_name

        self.collection_dropped = False

        self.connect(connection_string, database_name, collection_name)

    def connect(self, connection_string: str, database_name: str, collection_name: str):
        """Connect to a specific collection in the mongodb cluster.

        Parameters
        ----------
        connection_string: str
            The connection string used to connect to the mongodb cluser.
        database_name: str
            Name of the dabase to be used.
        collection_name: str
            Name of the collection to be used.

        """
        self.client = pymongo.MongoClient(connection_string, tlsCAFile=certifi.where())

        self.database_handler = self.client[database_name]
        self.collection_handler = self.database_handler[collection_name]

        self.database_name = database_name
        self.collection_name = collection_name

        self.collection_dropped = False

    def insert_one(self, data: dict) -> str:
        """Insert one data point in the database.

        Parameters
        ----------
        data: dict
            The dictionary to be inserted into the collection

        Returns
        -------
        The id of the datapoint that was inserted.

        Raises
        ------
        err.MongoDbInsertOneError
            If insertion failed.

        """
        insert_result = self.collection_handler.insert_one(data)

        if not insert_result.acknowledged:
            raise err.MongoDbInsertOneError(
                self.database_name,
                self.collection_name,
            )

        return str(insert_result.inserted_id)

    def insert_batch(self, data: List[dict]) -> List[str]:
        """Insert a set of data points in the database.

        Parameters
        ----------
        data: list of dict
            A list of the datapoints to be inserted.

        Returns
        -------
        The list of ids of the datapoint that was inserted.

        Raises
        ------
        err.MongoDbInsertBatchError
            If insertion failed.

        """
        insert_result = self.collection_handler.insert_many(data)

        if not insert_result.acknowledged:
            raise err.MongoDbInsertBatchError(
                self.database_name, self.collection_name, len(data)
            )

        return [str(inserted_id) for inserted_id in insert_result.inserted_ids]

    def find_many(
        self,
        query: Optional[dict] = None,
        limit: Optional[int] = 0,
        projection: Optional[list] = None,
    ) -> Cursor:
        """Find all datapoints matching a given query.

        Parameters
        ----------
        query: dict
            Dictionary to be used as a query. If `None`, all items will be
            considered. Default `None`
        limit: integer
            Limit on the number of datapoints that should be extracted. If 0,
            all items that match `query` will be returned. Default 0.
        projection: list
            List of the document fields to be returned. If `None`, all will be
            considered. Default `None`.

        Returns
        -------
        pymongo.cursor.Cursor
            With the result of the query.

        Raises
        ------
        err.MongoDbCollectionDroppedError
            If the collection the handler is pointing to has been dropped.

        """
        self._check_if_operation_can_be_run()

        return self.collection_handler.find(
            filter=query,
            limit=limit,
            projection=projection,
        )

    def find_one(
        self,
        query: Optional[dict] = None,
        projection: Optional[list] = None,
    ) -> Cursor:
        """Find one element matching `query`.

        Parameters
        ----------
        query: dict
            Dictionary to be used as a query. If `None`, all items will be
            considered. Default `None`.
        projection: list
            List of the document fields to be returned. If `None`, all will be
            considered. Default `None`.

        Returns
        -------
        dict
            With the resulting document. If there's no document matching
            `query`, will return `None`.

        Raises
        ------
        err.MongoDbCollectionDroppedError
            If the collection the handler is pointing to has been dropped.

        """
        self._check_if_operation_can_be_run()

        return self.collection_handler.find_one(
            filter=query,
            projection=projection,
        )

    def update_one(self, query: dict, new_values: dict) -> int:
        """Update one document matching `query` in the collection.

        Example:
        collection = "Customers"
        query = { "address": { "$regex": "^S" } }
        newvalues = { "name": "Minnie" }
        This method will update one entry in the "Customers" collection,
        where the "address" field does not contain the "S" letter.
        For the first document where the match occured, it will update the
        "name" field to "Minnie".

        Parameters
        ----------
        query: dict
            Dictionary used to identify the documents to update.
        new_values: dict
            <key, value> pairs of what needs to be updated.

        Returns
        -------
        int
            The number of items that were modified

        Raises
        ------
        err.MongoDbUpdateError
            If the update failed.

        err.MongoDbCollectionDroppedError
            If the collection the handler is pointing to has been dropped.

        """
        self._check_if_operation_can_be_run()

        update_result = self.collection_handler.update_one(
            query,
            {"$set": new_values},
        )

        if not update_result.acknowledged:
            raise err.MongoDbUpdateError(
                self.database_name, self.collection_name, query, new_values
            )

        return update_result.modified_count

    def update_all(self, query: dict, new_values: dict) -> int:
        """Update all documents matching `query` in the collection.

        Example:
        collection = "Customers"
        query = { "address": { "$regex": "^S" } }
        newvalues = { "name": "Minnie" }
        This method will update one entry in the "Customers" collection,
        where the "address" field does not contain the "S" letter.
        For all documents where the match occured, it will update the
        "name" field to "Minnie".

        Parameters
        ----------
        query: dict
            Dictionary used to identify the document to update.
        new_values: dict
            <key, value> pairs of what needs to be updated.

        Returns
        -------
        int
            The number of items that were modified

        Raises
        ------
        err.MongoDbUpdateError
            If the update failed.
        err.MongoDbCollectionDroppedError
            If the collection the handler is pointing to has been dropped.

        """
        self._check_if_operation_can_be_run()

        update_result = self.collection_handler.update_many(
            query,
            {"$set": new_values},
        )

        if not update_result.acknowledged:
            raise err.MongoDbUpdateError(
                self.database_name, self.collection_name, query, new_values
            )

        return update_result.modified_count

    def delete_one(self, query: dict) -> int:
        """Delete one document in the collection, matching `query`.

        Parameters
        ----------
        query: dict
            Dictionary used to identify the document to delete.

        Returns
        -------
        int
            Number of elements that were deleted

        Raises
        ------
        err.MongoDbDeleteError
            If the deletion failed.
        err.MongoDbCollectionDroppedError
            If the collection the handler is pointing to has been dropped.

        """
        self._check_if_operation_can_be_run()

        deletion_result = self.collection_handler.delete_one(query)

        if not deletion_result.acknowledged:
            raise err.MongoDbDeleteError(
                self.database_name,
                self.collection_name,
                query,
            )

        return deletion_result.deleted_count

    def delete_all(self, query: dict) -> int:
        """Delete all documents in the collection matching `query`.

        Parameters
        ----------
        query: dict
            Dictionary used to identify the document to delete.

        Returns
        -------
        int
            Number of elements that were deleted

        Raises
        ------
        err.MongoDbDeleteError
            If the deletion failed.
        err.MongoDbCollectionDroppedError
            If the collection the handler is pointing to has been dropped.

        """
        self._check_if_operation_can_be_run()

        deletion_result = self.collection_handler.delete_many(query)

        if not deletion_result.acknowledged:
            raise err.MongoDbDeleteError(
                self.database_name,
                self.collection_name,
                query,
            )

        return deletion_result.deleted_count

    def drop_collection(self):
        """Drop the collection with all the documents inside it."""
        self.collection_handler.drop()
        self.collection_dropped = True

    def _check_if_operation_can_be_run(self):
        if self.collection_dropped:
            raise err.MongoDbCollectionDroppedError(
                self.database_name, self.collection_name
            )
