import jwt
import http.client
from typing import Optional

import furl
import jsonschema
import tornado.web
import tornado.gen
from asyncio_mongo import bson

from wdim import client
from wdim.orm import exceptions
from wdim.client.permissions import Permissions
from wdim.orm.database import elasticsearch
from wdim.server.api.v1 import url
from wdim.server.api.v1.base import BaseAPIHandler


class DocumentsHandler(BaseAPIHandler):
    PATTERN = '/namespaces/(?P<namespace>\w+?)/collections/(?P<collection>\w+?)/?'

    @tornado.gen.coroutine
    def prepare(self):
        self.user = jwt.decode(self.get_cookie('cookie'), 'TestKey', option={'require_exp': True})['sub']
        self.namespace = yield from client.Namespace.get_by_name(self.path_kwargs['namespace'])
        self.collection = yield from self.namespace.get_collection(self.path_kwargs['collection'])
        self.permissions = Permissions.get_permissions(self.user, self.namespace, self.collection)

        method = self.request.method.lower()

        if method == 'get':
            if not self.permissions & Permissions.READ_WRITE:
                raise tornado.web.HTTPError(status_code=403)
        else:
            if not self.permissions & Permissions.from_method(method):
                raise tornado.web.HTTPError(status_code=403)

    @tornado.gen.coroutine
    def get(self, namespace, collection):
        limit = 50
        page = int(self.get_query_argument('page', default=0))

        query = client.Document.collection == self.collection._id
        if not self.permissions & Permissions.READ:
            query &= client.Document.created_by == self.user

        data = []
        for document in (yield from client.Document.find(query, limit=limit, skip=page * limit)):
            data.append({
                'id': document.record_id,
                'type': self.collection.name,
                'attributes': (yield from document.blob).data,
            })

        self_url = furl.furl(self.request.full_url()).set(args={})

        self.write({
            'data': data,
            'links': {
                'first': self_url.url,
                'next': self_url.copy().set({'page': page + 1}).url,
                'prev': self_url.copy().set({'page': page - 1}).url if page > 1 else self_url.url if page > 0 else None,
                'last': None,
                # 'meta': {
                #     'total': 8537,
                #     'per_page': 10
                # }
            }
        })

    @tornado.gen.coroutine
    def post(self, namespace, collection):
        data = self.json['data']
        assert data['type'] == collection

        _id = data.get('id') or str(bson.ObjectId())

        document = yield from self.collection.create(_id, data['attributes'], self.user)

        base_url = furl.furl(self.request.full_url().rstrip('/')).set(args={})

        serialized = yield from document.serialize(base_url)

        self.set_header('Location', serialized['data']['links']['self'])
        self.set_status(201)
        self.write(serialized)


class DocumentHandler(BaseAPIHandler):
    PATTERN = '/namespaces/(?P<namespace>\w+?)/collections/(?P<collection>\w+?)/(?P<record_id>(?:\w|-)+?)/?'

    @tornado.gen.coroutine
    def prepare(self):
        try:
            decoded = jwt.decode(self.get_cookie('cookie'), 'TestKey', option={'require_exp': True})
            self.user = decoded['sub']
        except jwt.ExpiredSignatureError:
            self.user = None
        self.namespace = yield from client.Namespace.get_by_name(self.path_kwargs['namespace'])
        self.collection = yield from self.namespace.get_collection(self.path_kwargs['collection'])
        self.document = yield from self.collection.read(self.path_kwargs['record_id'])

        self.permissions = Permissions.get_permissions(self.user, self.namespace, self.collection, self.document)

        if not self.permissions & Permissions.from_method(self.request.method):
            raise tornado.web.HTTPError(status_code=403)

    @tornado.gen.coroutine
    def get(self, namespace, collection, record_id):
        document = yield from self.collection.read(record_id)

        base_url = furl.furl(self.request.full_url().rstrip('/')).set(args={})
        self.write(document.serialize(base_url))

    @tornado.gen.coroutine
    def put(self, namespace, collection, record_id):
        data = self.json['data']
        assert data['id'] == record_id
        assert data['type'] == collection

        document = yield from self.collection.update(data['id'], data['attributes'], self.user)

        base_url = furl.furl(self.request.full_url().rstrip('/')).set(args={})
        self.write((yield from document.serialize(base_url)))

    @tornado.gen.coroutine
    def patch(self, namespace, collection, record_id):
        data = self.json['data']
        assert data['id'] == record_id
        assert data['type'] == collection

        document = yield from self.collection.update(data['id'], data['attributes'], self.user, merge=True)

        base_url = furl.furl(self.request.full_url().rstrip('/')).set(args={})
        self.write((yield from document.serialize(base_url)))

    @tornado.gen.coroutine
    def delete(self, namespace, collection, record_id):
        yield from self.collection.delete(record_id)
        self.set_status(204)


class HistoryHandler(BaseAPIHandler):
    PATTERN = '/namespaces/(?P<namespace>\w+?)/collections/(?P<collection>\w+?)/(?P<record_id>(?:\w|-)+?)/history/?'

    @tornado.gen.coroutine
    def prepare(self):
        try:
            decoded = jwt.decode(self.get_cookie('cookie'), 'TestKey', option={'require_exp': True})
            self.user = decoded['sub']
        except jwt.ExpiredSignatureError:
            self.user = None
        self.namespace = yield from client.Namespace.get_by_name(self.path_kwargs['namespace'])
        self.collection = yield from self.namespace.get_collection(self.path_kwargs['collection'])

        if not Permissions.get_permissions(self.user, self.namespace, self.collection) & Permissions.ADMIN:
            raise tornado.web.HTTPError(status_code=403)

    @tornado.gen.coroutine
    def get(self, namespace, collection, record_id):
        data = []
        limit = 50
        page = int(self.get_query_argument('page', default=0))

        self_url = furl.furl(self.request.full_url()).set(args={})

        for entry in (yield from client.Journal.find((client.Journal.collection == self.collection._id) & (client.Journal.record_id == record_id), limit=limit, skip=page * limit)):
            data.append((yield from entry.serialize(self_url)))

        if not data:
            return self.set_status(404)

        self.write({
            'data': data,
            'links': {
                'first': self_url.url,
                'next': self_url.copy().set({'page': page + 1}).url,
                'prev': self_url.copy().set({'page': page - 1}).url if page > 1 else self_url.url if page > 0 else None,
                'last': None,
                # 'meta': {
                #     'total': 8537,
                #     'per_page': 10
                # }
            }
        })
