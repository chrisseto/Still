from wdim.util import pack
from wdim.client import sort
from wdim.client import fields
from wdim.client.blob import Blob
from wdim.client.actions import Action
from wdim.client.storable import Storable
from wdim.client.journal import JournalEntry


class Collection(Storable):

    _id = fields.ObjectIdField()
    collection = fields.StringField()
    namespace = fields.ForeignField(Storable.ClassGetter('Namespace'))

    class Meta:
        indexes = [
            pack('namespace', 'collection', order=1, unique=True)
        ]

    async def set(self, key, data):
        blob = await Blob.create(data)
        return await JournalEntry.create(
            action=Action.update,
            record_id=key,
            blob=blob._id,
            namespace=self.namespace,
            collection=self._id,
        )

    async def get(self, key):
        return next(await JournalEntry.find(
            (JournalEntry.record_id == key) &
            (JournalEntry.namespace == self.namespace) &
            (JournalEntry.collection == self._id),
            limit=1,
            sort=sort.Descending(JournalEntry.timestamp)
        ))
