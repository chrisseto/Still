from wdim import util
from wdim.orm import sort
from wdim.orm import fields
from wdim.orm import Storable
from wdim.orm import exceptions
from wdim.client.blob import Blob
from wdim.client.actions import Action
from wdim.client.journal import Journal
from wdim.client.document import Document
from wdim.client.schema import get_schema


class Collection(Storable):

    SCHEMA_RESERVED = 'system:schema'

    _id = fields.ObjectIdField()
    name = fields.StringField()
    namespace = fields.ForeignField(Storable.ClassGetter('Namespace'))

    class Meta:
        indexes = [
            util.pack('name', 'collection', order=1, unique=True)
        ]

    async def get_schema(self):
        try:
            entry = next(await Journal.find(
                (Journal.collection == self._id) &
                (Journal.namespace == self.namespace) &
                (Journal.record_id == self.SCHEMA_RESERVED) &
                ((Journal.action == Action.update_schema.value) | (Journal.action == Action.delete_schema.value)),
                limit=1, sort=sort.Descending(Journal.timestamp)
            ))
        except StopIteration:
            return None

        if entry.action == Action.delete_schema:
            return None

        blob = await entry.blob

        return get_schema(blob['type'])(blob['schema'])

    async def set_schema(self, schema):
        try:
            get_schema(schema['type']).validate_schema(schema['schema'])
        except KeyError:
            pass  # TODO Exception

        blob = await Blob.create(schema)
        return await Journal.create(
            blob=blob._id,
            collection=self._id,
            namespace=self.namespace,
            action=Action.update_schema,
            record_id=self.SCHEMA_RESERVED,
        )

    async def delete_schema(self):
        return await Journal.create(
            blob=None,
            record_id=key,
            collection=self._id,
            action=Action.delete,
            namespace=self.namespace,
        )

    async def set(self, key: str, data: dict) -> 'ObjectId':
        schema = await self.get_schema()
        if schema:
            schema.validate(data)

        blob = await Blob.create(data)
        entry = await Journal.create(
            action=Action.update,
            record_id=key,
            blob=blob._id,
            namespace=self.namespace,
            collection=self._id,
        )
        doc = await Document.create(
            record_id=key,
            blob=blob._id,
            namespace=self.namespace,
            collection=self._id,
        )
        return entry

    async def get(self, key):
        try:
            return next(await Journal.find(
                (Journal.record_id == key) &
                (Journal.namespace == self.namespace) &
                (Journal.collection == self._id),
                limit=1,
                sort=sort.Descending(Journal.timestamp)
            ))
        except StopIteration:
            raise exceptions.NotFound()

    async def delete(self, key):
        return await Journal.create(
            blob=None,
            record_id=key,
            collection=self._id,
            action=Action.delete,
            namespace=self.namespace,
        )
