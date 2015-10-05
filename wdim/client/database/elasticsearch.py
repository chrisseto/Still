import abc
from functools import reduce

import elasticsearch_dsl

from wdim import exceptions
from wdim.client import sort
from wdim.client import query
from wdim.client.database.base import DatabaseLayer


class ElasticSearchLayer(DatabaseLayer):

    @classmethod
    def connect(cls, address, index_name):
        pass

    @classmethod
    def translate_query(cls, q: query.BaseQuery):
        assert isinstance(q, query.BaseQuery), 'q must be an instance of BaseQuery'

        try:
            return {
                query.Equals: lambda: {q.name: q.value},
                query.And: lambda: reduce(lambda x, y: {**x, **y}, (cls.translate_query(qu) for qu in q.querys), {})
            }[q.__class__]()
        except KeyError:
            raise exceptions.UnsupportedOperation(q)

    @classmethod
    def translate_sorting(self, sorting):
        try:
            return qf.sort({
                sort.Ascending: qf.DESCENDING,
                sort.Descending: qf.ASCENDING,
            }[sorting.__class__](sorting.fields._name))
        except KeyError:
            raise exceptions.UnsupportedOperation(sorting)

    def __init__(self, address, index_name):
        self.furl = furl.furl(address)
        self.furl.path.append(index_name)

    async def load(self, cls, _id):
        pass

    async def insert(self, cls, inst):
        resp = await aiohttp.request(
            'PUT',
            self.host + '',
            data=json.dumps(inst.to_document())
        )

    async def insert(self, inst):
        try:
            return await self.connection[inst._collection_name].insert(inst.to_document(), safe=True)
        except _pymongo.errors.DuplicateKeyError:
            raise exceptions.UniqueViolation
