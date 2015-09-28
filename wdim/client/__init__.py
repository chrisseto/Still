from datetime import datetime

from bson.objectid import ObjectId
from tornado.options import options

from wdim.client.schema import get_schema


class WdimClient:

    DEFAULT_SCHEMA_TYPE = 'jsonschema'

    @property
    def namespace(self):
        return self._namespace

    def __init__(self, namespace, database):
        self.db = database
        self._namespace = namespace

    async def set_schema(self, key, schema, type_=None):
        key = key or ObjectId()
        schema_type = get_schema(type_ or self.DEFAULT_SCHEMA_TYPE)

        schema_type.validate_schema(schema)

        # No exceptions mean success
        await self.db.schema.update({
            '_id': '{}::{}'.format(self.keyspace, key)
        }, {
            '$inc': {'version': 1},
            '$push': {'_data': {
                'type': type_,
                'schema': schema,
                'timestamp': datetime.utcnow()
            }}
        }, upsert=True)

        return schema_type(key, schema)

    async def get_schema(self, key, version=-1, raw=False):
        schema = await self.db.schema.find_one({
            '_id': '{}::{}'.format(self.namespace, key)
        }, {
            'version': True,
            'data': {'$slice': version}
        })

        if not schema:
            raise Exception

        schema['_id'].replace('{}::{}'.format(self.namespace, key), '', 1)

        if raw:
            return {
                '_id': schema['_id'],
                'version': schema['version'],
                'type': schema['data'][0]['type'],
                'schema': schema['data'][0]['schema'],
            }

        return get_schema(schema['data'][0]['type'])(schema['data'][0]['schema'])

    def get_collection(self, name):
        return WdimCollection('{}::{}'.format(self.namespace, name), self.db)


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
        self._collection = self.db.collection

    async def get(self, key, version=1):
        blob = await self.collection.find_one({
            '_id': '{}::{}'.format(self.namespace, key)
        }, {
            'version': True,
            'data': {'$slice': [-1 * version, 1]}
        })

        return {
            '_id': blob['_id'].replace('{}::{}'.format(self.namespace, key), '', 1),
            'data': blob['data'][0],
            'version': blob['version']
        }

    async def set(self, key, data):
        key = key or ObjectId()

        # Success if no exceptions
        await self.collection.update({
            '_id': '{}::{}'.format(self.namespace, key)
        }, {
            '$inc': {'version': 1},
            '$push': {'data': {
                'data': data,
                'schema': None,
                'permissions': None,
                'timestamp': datetime.utcnow()
            }}
        }, upsert=True)

        return str(key)
