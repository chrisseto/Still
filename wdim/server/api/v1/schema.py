import http.client
from typing import Optional

import jsonschema
import tornado.web
import tornado.gen

from wdim.server.api.v1.base import BaseAPIHandler


class SchemaHandler(BaseAPIHandler):
    PATTERN = r'/(?P<namespace>(?:\w|\d)+)/schemas/(?P<collection>(?:\w|\d)+)?/?'

    @tornado.gen.coroutine
    def prepare(self):
        if self.path_kwargs['collection'] is None:
            self.namespace = '{}::schema'.format(self.path_kwargs['namespace'])
        else:
            self.namespace = '{}::schema::{}'.format(self.path_kwargs['namespace'], self.path_kwargs['collection'])

    def get(self, namespace: str, collection: Optional[str]=None) -> None:
        if collection is not None:
            return self.write(self.get_schema())

        collections = self.collection.find({'_id': {'$regex': '^{}.'.format(self.namespace)}})

        return self.write({
            'collections': [
                col['_id'].replace(self.namespace + '::', '')
                for col in collections
            ]
        })

    @tornado.web.removeslash
    def put(self, namespace: str, collection: Optional[str]=None) -> None:
        if collection is None:
            return self.set_status(http.client.NOT_FOUND)

        jsonschema.Draft4Validator.check_schema(self.json)

        self.collection.update({
            '_id': self.namespace
        }, {
            '$inc': {'version': 1},
            '$push': {'schema': self.json}
        })

    def get_schema(self) -> dict:
        schema = self.collection.find_one({
            '_id': self.namespace
        }, {
            'version': True,
            'schema.0': True
        })

        if schema is not None:
            return {
                'schema': schema['schema'][0],
                'version': schema['version'],
            }

        self.collection.insert({
            'version': 0,
            'schema': [{}],
            '_id': self.namespace
        })

        return {
            'schema': {},
            'version': 0
        }
