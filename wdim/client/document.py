from wdim import util
from wdim.orm import fields
from wdim.orm import Storable


class Document(Storable):

    _id = fields.StringField()

    created_by = fields.StringField()
    modified_by = fields.StringField()

    created = fields.DatetimeField()
    modified = fields.DatetimeField()

    record_id = fields.StringField()
    blob = fields.ForeignField(Storable.ClassGetter('Blob'))
    namespace = fields.ForeignField(Storable.ClassGetter('Namespace'))
    collection = fields.ForeignField(Storable.ClassGetter('Collection'))

    class Meta:
        indexes = [util.pack('namespace', 'collection', 'record_id')]

    @classmethod
    async def create(cls, *, _id=None, **kwargs):
        _id = _id or '{}::{}'.format(kwargs['collection'], kwargs['record_id'])
        inst = cls(_id=_id, **kwargs)

        db_id = await cls._DATABASE.upsert(inst)

        assert _id == db_id, 'Database _id did not match given _id'

        cls._fields['_id'].__set__(inst, db_id, override=True)

        return inst

    @classmethod
    async def create_from_entry(cls, entry):
        return await Document.create(
            record_id=entry.record_id,
            blob=entry.blob,
            namespace=entry.namespace,
            collection=entry.collection,
            created=entry.created,
            modified=entry.modified,
            created_by=entry.created_by,
            modified_by=entry.modified_by,
        )

    async def serialize(self, furl):
        self_url = furl.copy()
        self_url.path.segments.append(self.record_id)

        return {
            'data': {
                'id': self.record_id,
                'type': (await self.collection).name,
                'attributes': (await self.blob).data,
                'meta': {
                    'created_by': self.created_by,
                    'modified_by': self.modified_by,
                    'created': self.created.isoformat(),
                    'modified': self.modified.isoformat() if self.modified else None
                },
                'links': {
                    'self': self_url.url
                }
            }
        }
