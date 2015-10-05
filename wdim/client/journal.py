from datetime import datetime

from wdim.util import pack
from wdim.client import fields
from wdim.client.actions import Action
from wdim.client.storable import Storable


class JournalEntry(Storable):

    _id = fields.ObjectIdField()

    action = fields.EnumField(Action)
    record_id = fields.StringField()
    timestamp = fields.DatetimeField()

    blob = fields.ForeignField(Storable.ClassGetter('Blob'))
    schema = fields.ForeignField(Storable.ClassGetter('Blob'), required=False)
    namespace = fields.ForeignField(Storable.ClassGetter('Namespace'))
    collection = fields.ForeignField(Storable.ClassGetter('Collection'))

    class Meta:
        indexes = [
            pack('timestamp', order=1),
            pack('namespace', 'collection', 'record_id', order=1),
            pack('namespace', 'collection', 'record_id', 'timestamp', order=1, unique=True),
        ]

    @classmethod
    async def create(cls, *, timestamp=None, **kwargs):
        assert timestamp is None, 'Cannot manually add timestamp'

        # Classmethod supers need arguments for some reason
        return await super(JournalEntry, cls).create(timestamp=datetime.utcnow(), **kwargs)
