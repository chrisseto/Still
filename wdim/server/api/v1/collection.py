import http.client
from typing import Optional

import jsonschema
import tornado.web
import tornado.gen

from wdim.client import WdimClient
from wdim.server.api.v1 import url
from wdim.server.api.v1.base import BaseAPIHandler


class CollectionListing(BaseAPIHandler):
    PATTERN = url.build_pattern(url.NAMESPACES, url.NAMESPACE_RE, url.COLLECTIONS)

    def prepare(self):
        self.client = WdimClient(self.url_kwargs['namespace'], self.database)

    @tornado.gen.coroutine
    def get(self):
        pass


class CollectionHandler(BaseAPIHandler):
    PATTERN = url.build_pattern(url.NAMESPACES, url.NAMESPACE_RE, url.COLLECTIONS, url.COLLECTION_RE)

    def prepare(self):
        self.client = WdimClient(self.path_kwargs['namespace'], self.database)
        self.collection = self.client.get_collection(self.path_kwargs['collection'])

    @tornado.gen.coroutine
    def get(self, **_):
        self.write({
            'data': (yield from self.collection.list())
        })

        # data = yield from self.collection.get_metadata()
        # self.write(data or {})

    @tornado.gen.coroutine
    def post(self, **_):
        _id = yield from self.collection.set(None, self.json)
        self.write((yield from self.collection.get(_id)))
        self.set_status(201)
