import abc

import asyncio_mongo
from asyncio_mongo import filter as qf


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
        return await self.connection[inst._collection_name].insert(await inst.to_document(), safe=True)

    async def load(self, cls, _id):
        return await self.connection[cls._collection_name].find({'_id': _id})

    async def find_one(self, cls, query):
        return await self.connection[cls._collection_name].find_one(query)

    async def ensure_index(self, name, indices):
        for index in indices:
            await self.connection[name].ensure_index(
                qf.sort([index['key'], 1]), unique=index['unique']
            )
