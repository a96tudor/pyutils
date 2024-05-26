from pyutils.database.mongo.client_handler import MongoClientHandler


class MongoDriverNewsfeelService:
    """Driver intended to be used by `newsfeel-service`."""

    def __init__(
        self, connection_string: str, database_name: str, collection_name: str,
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
        self._connection_string = connection_string
        self._database_name = database_name
        self._collection_name = collection_name

        self.client_handler = None
        self.connect_to(connection_string, database_name, collection_name)

    def connect_to(
        self, connection_string: str, database_name: str, collection_name: str,
    ):
        """Connect to a MongoDB collection.

        Parameters
        ----------
        connection_string: str
            The connection string used to connect to the mongodb cluser.
        database_name: str
            Name of the database to be used.
        collection_name: str
            Name of the collection to be used.

        """
        self._connection_string = connection_string
        self._database_name = database_name
        self._collection_name = collection_name

        if self.client_handler is None:
            self.client_handler = MongoClientHandler(
                connection_string,
                database_name,
                collection_name
            )

            return

        self.client_handler.connect(
            connection_string, database_name, collection_name
        )
