# app/db.py

import os
from elasticsearch import AsyncElasticsearch

_ES_URL = os.getenv("ES_URL", "http://localhost:9200")

# one global client instance
_es_client: AsyncElasticsearch | None = None


def get_es_client() -> AsyncElasticsearch:
    """
    Return a singleton AsyncElasticsearch client, connecting to ES_URL
    if not already initialised.
    """
    global _es_client
    if _es_client is None:
        _es_client = AsyncElasticsearch(_ES_URL)
    return _es_client
