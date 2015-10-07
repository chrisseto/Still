import json
import hashlib
from typing import Any, Dict

from wdim import orm
from wdim.orm import fields
from wdim.orm import exceptions


class Blob(orm.Storable):

    HASH_METHOD = 'sha256'

    _id = fields.StringField(unique=True)
    data = fields.DictField()

    @classmethod
    async def create(cls, data: Dict[str, Any]) -> 'Blob':
        sha = hashlib.new(cls.HASH_METHOD, json.dumps(data).encode('utf-8')).hexdigest()
        try:
            # Classmethod supers need arguments for some reason
            return await super(Blob, cls).create(_id=sha, data=data)
        except exceptions.UniqueViolation:
            return await cls.load(sha)

    @property
    def hash(self) -> str:
        return self._id

    def __getitem__(self, key):
        return self.data[key]
