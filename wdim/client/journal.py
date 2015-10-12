from datetime import datetime

from wdim.util import pack
from wdim.orm import fields
from wdim.orm import Storable
from wdim.client.actions import Action


class Journal(Storable):

    _id = fields.ObjectIdField()

    action = fields.EnumField(Action)
    record_id = fields.StringField()

    created_by = fields.StringField()
    modified_by = fields.StringField()

    created = fields.DatetimeField()
    modified = fields.DatetimeField()


    blob = fields.ForeignField(Storable.ClassGetter('Blob'))
    schema = fields.ForeignField(Storable.ClassGetter('Blob'), required=False)
    namespace = fields.ForeignField(Storable.ClassGetter('Namespace'))
    collection = fields.ForeignField(Storable.ClassGetter('Collection'))

    class Meta:
        indexes = [
            pack('modified', order=1),
            pack('namespace', 'collection', 'record_id', order=1),
            pack('namespace', 'collection', 'record_id', 'modified', order=1, unique=True),
            pack('namespace', 'collection', 'record_id', 'modified', 'action', order=1, unique=True),
        ]

    @classmethod
    async def create(cls, *, modified=None, created=None, **kwargs):
        assert kwargs.get('created_by') is not None, 'created_by must be specified'
        modified = datetime.utcnow()

        if kwargs.get('action') == Action.CREATE:
            created = modified
            kwargs['modified_by'] = kwargs['created_by']

        assert created is not None, 'created must be specified'
        assert kwargs.get('modified_by') is not None, 'modified_by must be specified'

        # Classmethod supers need arguments for some reason
        return await super(Journal, cls).create(modified=modified, created=created, **kwargs)

    async def serialize(self, furl):
        self_url = furl.copy()
        self_url.path.segments.append(str(self._id))

        return {
            'data': {
                'id': str(self._id),
                'type': '{}:journal'.format((await self.collection).name),
                'attributes': {
                    'action': self.action,
                    'data': (await self.blob).data,
                    'created_by': self.created_by,
                    'modified_by': self.modified_by,
                    'created': self.created.isoformat(),
                    'modified': self.modified.isoformat()
                },
                'links': {
                    'self': self_url.url
                }
            }
        }
