import json
import hashlib

from wdim import exceptions
from wdim.client import fields
from wdim.client.storable import Storable


class Blob(Storable):

    HASH_METHOD = 'sha256'

    _id = fields.StringField(unique=True)
    data = fields.DictField()

    @classmethod
    async def create(cls, data):
        sha = hashlib.new(cls.HASH_METHOD, json.dumps(data).encode('utf-8')).hexdigest()
        try:
            # Classmethod supers need arguments for some reason
            return await super(Blob, cls).create(_id=sha, data=data)
        except exceptions.UniqueViolation:
            return await cls.load(sha)

    @property
    def hash(self):
        return self._id
