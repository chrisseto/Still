from wdim.client import fields
from wdim.client.storable import Storable
from wdim.client.collection import Collection


class Namespace(Storable):

    _id = fields.ObjectIdField()
    namespace = fields.StringField(unique=True)

    @classmethod
    async def get_by_namespace(cls, name):
        return await cls.find_one(Namespace.namespace == name)

    async def get_collection(self, collection_name):
        assert self._id, 'Namespace must be saved before use'

        return await Collection.find_one(
            (Collection.namespace == self._id) &
            (Collection.collection == collection_name)
        )

    async def create_collection(self, collection_name):
        assert self._id, 'Namespace must be saved before use'

        return await Collection.create(collection=collection_name, namespace=self)
