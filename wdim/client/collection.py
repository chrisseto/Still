from asyncio_mongo import filter as qf

from wdim.util import pack
from wdim.client import fields
from wdim.client.storable import Storable


class Collection(Storable):

    _INDEX = (
        pack(
            qf.sort([('namespace', 1), ('collection', 1)]),
            unique=True
        ),
    )

    _id = fields.ObjectIdField()
    namespace = fields.ForeignField(Storable.ClassGetter('Namespace'))

    def __init__(self, collection, namespace):
        super().__init__()
        self.namespace = namespace
        self.collection = collection

    async def set(self, key, data):
        blob = await Blob.create(key, data)
