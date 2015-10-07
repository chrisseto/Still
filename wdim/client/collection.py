from wdim import util
from wdim.orm import sort
from wdim.orm import fields
from wdim.orm import Storable
from wdim.client.blob import Blob
from wdim.client.actions import Action
from wdim.client.journal import JournalEntry


class Collection(Storable):

    SCHEMA_RESERVED = 'system:schema'

    _id = fields.ObjectIdField()
    collection = fields.StringField()
    namespace = fields.ForeignField(Storable.ClassGetter('Namespace'))

    class Meta:
        indexes = [
            util.pack('namespace', 'collection', order=1, unique=True)
        ]

    async def get_schema(self):
        try:
            entry = next(await JournalEntry.find(
                (JournalEntry.collection == self._id) &
                (JournalEntry.record_id == self.schema) &
                (JournalEntry.namespace == self.namespace) &
                ((JournalEntry.action == Action.update_schema) | (JournalEntry.action == Action.delete_schema)),
                limit=1, sort=sort.Descending(JournalEntry.timestamp)
            ))
        except StopIteration:
            return None

        if entry.action == Action.delete_schema:
            return None

        blob = await entry.blob

        return get_schema(blob['type'])(blob['schema'])

    async def set_schema(self, schema):
        try:
            get_schema(blob['type']).validate_schema(blob['schema'])
        except KeyError:
            pass  # TODO Exception

        blob = await Blob.create(schema)
        return await JournalEntry.create(
            action=Action.schema_update,
            record_id=key,
            blob=blob._id,
            namespace=self.namespace,
            collection=self._id,
        )

    async def delete_schema(self):
        return await JournalEntry.create(
            blob=None,
            record_id=key,
            collection=self._id,
            action=Action.delete,
            namespace=self.namespace,
        )

    async def set(self, key: str, data: dict) -> 'ObjectId':
        if self.schema:
            return None

        blob = await Blob.create(data)
        return await JournalEntry.create(
            action=Action.update,
            record_id=key,
            blob=blob._id,
            namespace=self.namespace,
            collection=self._id,
        )

    async def get(self, key):
        try:
            return next(await JournalEntry.find(
                (JournalEntry.record_id == key) &
                (JournalEntry.namespace == self.namespace) &
                (JournalEntry.collection == self._id),
                limit=1,
                sort=sort.Descending(JournalEntry.timestamp)
            ))
        except StopIteration:
            return None

    async def delete(self, key):
        return await JournalEntry.create(
            blob=None,
            record_id=key,
            collection=self._id,
            action=Action.delete,
            namespace=self.namespace,
        )
