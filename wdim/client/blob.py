import json
import hashlib

from wdim.client.storable import Storable


class Blob(Storable):

    HASH_METHOD = 'sha1'

    @classmethod
    def _create(cls, data):
        sha = hashlib(cls.HASH_METHOD, json.dumps(data))
        return cls(sha, data)

    @classmethod
    def _from_document(cls, document):
        return cls(document['data'])

    @property
    def hash(self):
        return self._id

    def __init__(self, data):
        self.data = data

    def to_document(self):
        return {
            '_id': self.hash,
            'data': self.data
        }
