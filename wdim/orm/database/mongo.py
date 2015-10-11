from functools import reduce

import asyncio_mongo
from asyncio_mongo import _pymongo
from asyncio_mongo import exceptions
from asyncio_mongo import filter as qf

from wdim.orm import sort
from wdim.orm import query
from wdim.orm import exceptions
from wdim.orm.database.base import DatabaseLayer
from wdim.orm.database.translation import Translator


class MongoTranslator(Translator):

    @classmethod
    def translate_query(cls, q: query.BaseQuery):
        assert isinstance(q, query.BaseQuery), 'q must be an instance of BaseQuery'

        try:
            return {
                query.Equals: lambda: {q.name: q.value},
                query.Or: lambda: {'$or': [cls.translate_query(qu) for qu in q.queries]},
                query.And: lambda: reduce(lambda x, y: {**x, **y}, (cls.translate_query(qu) for qu in q.queries), {}),
            }[q.__class__]()
        except KeyError:
            raise exceptions.UnsupportedOperation(q)

    @classmethod
    def translate_sorting(self, sorting):
        try:
            return qf.sort({
                sort.Ascending: qf.DESCENDING,
                sort.Descending: qf.ASCENDING,
            }[sorting.__class__](sorting.field._name))
        except KeyError:
            raise exceptions.UnsupportedOperation(sorting)


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

    async def upsert(self, inst):
        await self.connection[inst._collection_name].update({'_id': inst._id}, inst.to_document(), safe=True, upsert=True)
        return inst._id

    async def load(self, cls, _id):
        return await self.find_one(cls, cls._id == _id)

    async def drop(self, cls):
        return await self.connection[cls._collection_name].drop(safe=True)

    async def find_one(self, cls, query):
        doc = await self.connection[cls._collection_name].find_one(
            MongoTranslator.translate_query(query)
        )
        if not doc:
            raise exceptions.NotFound(query)
        return doc

    async def find(self, cls, query=None, limit=0, skip=0, sort=None):
        filter = sort and MongoTranslator.translate_sorting(sort)
        query = (query or {}) and MongoTranslator.translate_query(query)
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
