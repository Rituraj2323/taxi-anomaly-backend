"""
MongoDB connection helper using pymongo.
All apps import `db` from here to access collections.
"""
import pymongo
from django.conf import settings

_client = None
_db = None


def get_db():
    global _client, _db
    if _db is None:
        _client = pymongo.MongoClient(settings.MONGODB_URI)
        _db = _client[settings.MONGODB_DB]
    return _db


def get_collection(name: str):
    return get_db()[name]
