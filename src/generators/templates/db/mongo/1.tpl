
from motor.motor_asyncio import AsyncIOMotorClient
from beanie import init_beanie
import logging

{{db_imports}}

class Database:
    _client = None  # Private variable for MongoDB client
    _db_name = None  # Private variable for the database name

    @staticmethod
    async def init(uri: str, db_name: str):
        """
        Initialize the database connection and Beanie models.
        """
        if Database._client is not None:
            logging.warning("Database is already initialized. Skipping reinitialization.")
            return

        Database._client = AsyncIOMotorClient(uri)
        Database._db_name = db_name
        db = Database._client[db_name]

        # Initialize Beanie models
        await init_beanie(database=db, document_models=[ {{models}} ])
        logging.info(f"Database initialized with Beanie models for {db_name} at {uri}")

    @staticmethod
    def get_db():
        """
        Get a direct connection to the MongoDB database.
        """
        if Database._client is None or Database._db_name is None:
            logging.error("Database is not initialized. Call Database.init() first.")
            raise Exception("Database is not initialized. Call Database.init() first.")
        return Database._client[Database._db_name]

    @staticmethod
    def get_client():
        """
        Get the raw MongoDB client (if needed).
        """
        if Database._client is None:
            logging.error("Database client is not initialized. Call Database.init() first.")
            raise Exception("Database client is not initialized. Call Database.init() first.")
        return Database._client


