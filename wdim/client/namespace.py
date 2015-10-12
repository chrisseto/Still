from wdim.orm import fields
from wdim.orm import Storable
from wdim.client.collection import Collection


class Namespace(Storable):

    _id = fields.ObjectIdField()
    name = fields.StringField(unique=True)
    permissions = fields.DictField()

    @classmethod
    async def get_by_name(cls, name):
        return await cls.find_one(Namespace.name == name)

    async def get_collection(self, collection_name):
        assert self._id, 'Namespace must be saved before use'

        return await Collection.find_one(
            (Collection.namespace == self._id) &
            (Collection.name == collection_name)
        )

    async def create_collection(self, collection_name):
        assert self._id, 'Namespace must be saved before use'

        return await Collection.create(name=collection_name, namespace=self)
