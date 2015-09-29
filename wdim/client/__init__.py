from datetime import datetime

from bson.objectid import ObjectId
from tornado.options import options

from wdim.client.schema import get_schema
from wdim.versionedstorage import VersionedStorage


class WdimClient:

    DEFAULT_SCHEMA_TYPE = 'jsonschema'

    @property
    def namespace(self):
        return self._namespace

    def __init__(self, namespace, database):
        self.db = database
        self._namespace = namespace
        self.collection = VersionedStorage(self.db.schema)

    async def set_schema(self, key, schema, type_=None):
        schema_type = get_schema(type_ or self.DEFAULT_SCHEMA_TYPE)

        schema_type.validate_schema(schema)

        _id = await self.collection.set(
            '::'.join([self.namespace, key]),
            {'schema': schema, 'type': schema_type.name}
        )

        return schema_type(_id, schema)

    async def get_schema(self, key, version=None, raw=False):
        schema = await self.collection.get('::'.join([self.namespace, key]), version=version)

        if not schema:
            return None

        if raw:
            return schema

        return get_schema(schema['data']['type'])('::'.join([self.namespace, key]), schema['data']['schema'], version=version)

    def get_collection(self, name):
        return WdimCollection('::'.join([self.namespace, name]), self.db)


class WdimCollection:

    @property
    def namespace(self):
        return self._namespace

    @property
    def collection(self):
        return self._collection

    def __init__(self, namespace, db):
        self.db = db
        self._namespace = namespace
        self._collection = VersionedStorage(self.db.collection)

    async def get(self, key, version=None):
        return await self.collection.get('::'.join([self.namespace, key]), version=version)

    async def set(self, key, data):
        schema = await self.get_schema()

        if schema:
            # Success if no exceptions
            schema.validate(data)

        key = key or str(ObjectId())

        return await self.collection.set(
            '::'.join([self.namespace, key]),
            {'data': data, 'schema': schema._id if schema else None}
        )

    async def update(self, key, data):
        current = await self.get(key) or {}
        return await self.set(key, {**current.get('data', {}), **data})

    async def get_schema(self, version=None):
        blob = await self.collection.get(self.namespace)

        if not blob:
            return None

        namespace, *tail = blob['data']['schema'].split('::')

        if len(tail) > 1:
            tail[1] = int(tail[1])

        return await WdimClient(namespace, self.db).get_schema(*tail)

    async def set_schema(self, schema):
        return await self.collection.update(self.namespace, {
            'schema': schema._id
        })
