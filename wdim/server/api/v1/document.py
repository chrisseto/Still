import uuid
import http.client
from typing import Optional

import furl
import jsonschema
import tornado.web
import tornado.gen

from wdim import client
from wdim.orm import exceptions
from wdim.orm.database import elasticsearch
from wdim.server.api.v1 import url
from wdim.server.api.v1.base import BaseAPIHandler


class DocumentsHandler(BaseAPIHandler):
    PATTERN = '/namespaces/(?P<namespace>\w+?)/collections/(?P<collection>\w+?)/?'

    @tornado.gen.coroutine
    def prepare(self):
        try:
            self.namespace = yield from client.Namespace.get_by_name(self.path_kwargs['namespace'])
            self.collection = yield from self.namespace.get_collection(self.path_kwargs['collection'])
        except exceptions.NotFound:
            raise tornado.web.HTTPError(status_code=404)

    @tornado.gen.coroutine
    def get(self, namespace, collection):
        limit = 50
        page = int(self.get_query_argument('page', default=0))

        data = []
        for document in (yield from client.Document.find(client.Document.collection == self.collection._id, limit=limit, skip=page * limit)):
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
        entry = yield from self.collection.set(data.get('id') or uuid.uuid4(), data['attributes'])

        self_url = furl.furl(self.request.full_url()).set(args={})
        self_url.path.segments.append(entry.record_id)

        self.set_status(201)
        self.set_header('Location', self_url.url)
        self.write({'data': {
            'id': entry.record_id,
            'type': self.collection.name,
            'attributes': data['attributes']
        }})


class DocumentHandler(BaseAPIHandler):
    PATTERN = '/namespaces/(?P<namespace>\w+?)/collections/(?P<collection>\w+?)/(?P<record_id>(?:\w|-)+?)/?'

    @tornado.gen.coroutine
    def prepare(self):
        try:
            self.namespace = yield from client.Namespace.get_by_name(self.path_kwargs['namespace'])
            self.collection = yield from self.namespace.get_collection(self.path_kwargs['collection'])
        except exceptions.NotFound:
            raise tornado.web.HTTPError(status_code=404)

    @tornado.gen.coroutine
    def get(self, namespace, collection, record_id):
        try:
            document = yield from client.Document.load('{}::{}'.format(self.collection._id, record_id))
            # document = yield from client.Document.find_one(client.Document.record_id == record_id)

            self.write({'data': {
                'id': document.record_id,
                'type': self.collection.name,
                'attributes': (yield from document.blob).data,
            }})

        except exceptions.NotFound:
            self.set_status(http.client.NOT_FOUND)

    @tornado.gen.coroutine
    def put(self, namespace, collection, record_id):
        data = self.json['data']
        assert data['id'] == record_id
        assert data['type'] == collection

        entry = yield from self.collection.set(data['id'], data['attributes'])

        self_url = furl.furl(self.request.full_url()).set(args={})
        self_url.path.segments.append(data['id'])

        self.set_status(200)
        self.set_header('Location', self_url.url)
        self.write({'data': {
            'id': data['id'],
            'type': self.collection.name,
            'attributes': data['attributes']
        }})

    @tornado.gen.coroutine
    def patch(self, namespace, collection, record_id):
        data = self.json['data']
        assert data['id'] == record_id
        assert data['type'] == collection

        try:
            entry = yield from self.collection.get(data['id'])
        except exceptions.NotFound:
            previous = {}
        else:
            previous = (yield from entry.blob).data

        entry = yield from self.collection.set(data['id'], {**previous, **data['attributes']})

        self_url = furl.furl(self.request.full_url()).set(args={})
        self_url.path.segments.append(data['id'])

        self.set_status(200)
        self.set_header('Location', self_url.url)
        self.write({'data': {
            'id': data['id'],
            'type': self.collection.name,
            'attributes': data['attributes']
        }})

    # @tornado.gen.coroutine
    # def delete(self, doc_id, **_):
    #     yield from self.collection.remove(doc_id)
    #     self.set_status(204)


class HistoryHandler(BaseAPIHandler):
    PATTERN = '/namespaces/(?P<namespace>\w+?)/collections/(?P<collection>\w+?)/(?P<record_id>(?:\w|-)+?)/history/?'

    @tornado.gen.coroutine
    def prepare(self):
        try:
            self.namespace = yield from client.Namespace.get_by_name(self.path_kwargs['namespace'])
            self.collection = yield from self.namespace.get_collection(self.path_kwargs['collection'])
        except exceptions.NotFound:
            raise tornado.web.HTTPError(status_code=404)

    @tornado.gen.coroutine
    def get(self, namespace, collection, record_id):
        data = []
        limit = 50
        page = int(self.get_query_argument('page', default=0))

        for journal in (yield from client.Journal.find((client.Journal.collection == self.collection._id) & (client.Journal.record_id == record_id), limit=limit, skip=page * limit)):
            data.append({
                'id': str(journal._id),
                'type': '{}:journal'.format(self.collection.name),
                'attributes': {
                    'action': journal.action,
                    'data': (yield from journal.blob).data,
                    'timestamp': journal.timestamp.isoformat(),
                }
            })

        if not data:
            return self.set_status(404)

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
