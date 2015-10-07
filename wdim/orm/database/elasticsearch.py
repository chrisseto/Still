import abc
import json
import operator
import datetime
from functools import reduce

import furl

import aiohttp

import elasticsearch_dsl

from wdim.orm import sort
from wdim.orm import query
from wdim.orm import fields
from wdim.orm import exceptions
from wdim.orm.database.base import DatabaseLayer
from wdim.orm.database.translation import Translator


class ElasticSearchTranslator(Translator):

    @classmethod
    def translate_query(cls, q):
        if isinstance(q, query.Query):
            q._value = cls.translate_value(q.value)

        try:
            return {
                query.Or: lambda: reduce(operator.or_, (cls.translate_query(x) for x in q.queries)),
                query.And: lambda: reduce(operator.and_, (cls.translate_query(x) for x in q.queries)),
                query.Equals: lambda: elasticsearch_dsl.Q('match', **{q.name: q.value})
            }[q.__class__]()
        except KeyError:
            raise exceptions.UnsupportedOperation(q)

    @classmethod
    def translate_sorting(cls, sorting):
        try:
            return {
                sort.Ascending: sorting.field._name,
                sort.Descending: '-' + sorting.field._name
            }[sorting.__class__]
        except KeyError:
            raise exceptions.UnsupportedOperation(sorting)

    @classmethod
    def translate_field(cls, field, value):
        try:
            # TODO should probably just match on type
            return {
                fields.DatetimeField: lambda: value.isoformat(),
                fields.ObjectIdField: lambda: str(value),
                fields.ForeignField: lambda: str(value)
            }[field.__class__]()
        except KeyError:
            return field.to_document(value)

    @classmethod
    def translate_value(cls, value):
        try:
            # TODO should probably just match on type
            return {
                fields.bson.ObjectId: lambda: str(value),
                datetime.datetime: lambda: value.isoformat(),
            }[value.__class__]()
        except KeyError:
            return value


class ElasticSearchLayer(DatabaseLayer):

    @classmethod
    async def connect(cls, host='localhost', index_name='wdim20150921', port=9200):
        return cls(host, port, index_name)

    def __init__(self, address, port, index_name):
        self.furl = furl.furl()
        self.furl.port = port
        self.furl.host = address
        self.furl.scheme = 'http'
        self.furl.path.add(index_name)

    async def load(self, cls, _id):
        return await self.find_one(cls, cls._id == _id)

    async def find_one(self, cls, query):
        search = elasticsearch_dsl.Search().query(ElasticSearchTranslator.translate_query(query))[:1]
        response = await self._send_request('GET', cls._collection_name, query=search)

        if len(response.hits) == 0:
            raise exceptions.NotFound(query)

        return response.hits[0].to_dict()

    async def find(self, cls, query=None, limit=None, skip=0, sort=None):
        search = elasticsearch_dsl.Search()

        if query:
            search = search.query(ElasticSearchTranslator.translate_query(query))
        if limit or skip:
            search = search[skip:skip + limit]
        if sort:
            search = search.sort(ElasticSearchTranslator.translate_sorting(sort))

        response = await self._send_request('GET', cls._collection_name, query=search)

        if len(response.hits) == 0:
            raise exceptions.NotFound()

        return (result.to_dict() for result in response.hits)

    async def ensure_index(self, cls, indices):
        pass

    async def drop(self, cls):
        # TODO validate return
        resp = await self._send_request('DELETE', cls._collection_name)

    async def insert(self, inst):
        copied = self.furl.copy()
        copied.path.segments.append(inst.__class__._collection_name)
        if inst._id:
            copied.path.segments.append(str(inst._id))

        resp = await aiohttp.request(
            'PUT',
            copied.url,
            data=json.dumps(inst.to_document(ElasticSearchTranslator))
        )
        assert resp.status in (200, 201)

        try:
            return (await resp.json())['_id']
        finally:
            resp.close()

    async def _send_request(self, method, _type, query=None, _id=None):
        copied = self.furl.copy()
        copied.path.segments.append(_type)
        if _id:
            copied.path.segments.append(_id)
        else:
            copied.path.segments.append('_search')

        resp = await aiohttp.request(
            method,
            copied.url,
            data=json.dumps(query.to_dict()) if query else None
        )

        return elasticsearch_dsl.result.Response(await resp.json())
