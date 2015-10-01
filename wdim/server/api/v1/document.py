import http.client
from typing import Optional

import jsonschema
import tornado.web
import tornado.gen

from wdim.client import WdimClient
from wdim.server.api.v1 import url
from wdim.server.api.v1.base import BaseAPIHandler


class DocumentHandler(BaseAPIHandler):
    PATTERN = url.build_pattern(
        url.NAMESPACES,
        url.NAMESPACE_RE,
        url.COLLECTIONS,
        url.COLLECTION_RE,
        url.DOC_ID_RE
    )

    def prepare(self):
        self.client = WdimClient(self.path_kwargs['namespace'], self.database)
        self.collection = self.client.get_collection(self.path_kwargs['collection'])

    @tornado.gen.coroutine
    def get(self, doc_id, **_):
        doc = yield from self.collection.get(doc_id)
        if doc is None:
            return self.set_status(404)
        self.write(doc)

    @tornado.gen.coroutine
    def post(self, doc_id, **_):
        yield from self.collection.set(doc_id, self.json)
        self.write((yield from self.collection.get(doc_id)))

    @tornado.gen.coroutine
    def patch(self, doc_id, **_):
        yield self.put(doc_id)

    @tornado.gen.coroutine
    def put(self, doc_id, **_):
        return self.write((
            yield from self.collection.get((
                yield from self.collection.update(doc_id, self.json)
            ))
        ))

    @tornado.gen.coroutine
    def delete(self, doc_id, **_):
        yield from self.collection.remove(doc_id)
        self.set_status(204)
