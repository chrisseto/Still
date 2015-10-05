import abc

import asyncio_mongo
from asyncio_mongo import _pymongo
from asyncio_mongo import exceptions
from asyncio_mongo import filter as qf

from wdim import exceptions


class DatabaseLayer(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def insert(self, inst):
        raise NotImplemented

    @abc.abstractmethod
    async def load(self, pk):
        raise NotImplemented

    @abc.abstractmethod
    async def ensure_index(self, name, keys):
        raise NotImplemented


class MongoLayer(DatabaseLayer):

    @classmethod
    async def connect(cls, host='127.0.0.1', port=27017, name='wdim20150921'):
        connection = await asyncio_mongo.Connection.create(host, port)
        return cls(connection[name])

    def __init__(self, connection):
        self.connection = connection

    async def insert(self, inst):
        try:
            return await self.connection[inst._collection_name].insert(inst.to_document(), safe=True)
        except _pymongo.errors.DuplicateKeyError:
            raise exceptions.UniqueViolation

    async def load(self, cls, _id):
        return await self.connection[cls._collection_name].find_one({'_id': _id})

    async def find_one(self, cls, query):
        doc = await self.connection[cls._collection_name].find_one(query)
        if not doc:
            raise exceptions.NotFound(query)
        return doc

    async def find(self, cls, query, limit=0, skip=0, sort=None):
        filter = sort and qf.sort(sort)
        return await self.connection[cls._collection_name].find(
            query,
            limit=limit,
            skip=skip,
            filter=filter
        )

    async def ensure_index(self, name, indices):
        for keys, opts in indices:
            await self.connection[name].ensure_index(
                qf.sort([(key, opts.get('order', 1)) for key in keys]),
                unique=opts.get('unique', False)
            )
