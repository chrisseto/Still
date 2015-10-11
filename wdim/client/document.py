from wdim import util
from wdim.orm import fields
from wdim.orm import Storable


class Document(Storable):

    _id = fields.StringField()

    record_id = fields.StringField()
    blob = fields.ForeignField(Storable.ClassGetter('Blob'))
    namespace = fields.ForeignField(Storable.ClassGetter('Namespace'))
    collection = fields.ForeignField(Storable.ClassGetter('Collection'))

    class Meta:
        indexes = [util.pack('namespace', 'collection', 'record_id')]

    @classmethod
    async def create(cls, *, _id=None, **kwargs):
        _id= _id or '{}::{}'.format(kwargs['collection'], kwargs['record_id'])
        inst = cls(_id=_id, **kwargs)

        db_id = await cls._DATABASE.upsert(inst)

        assert _id == db_id, 'Database _id did not match given _id'

        cls._fields['_id'].__set__(inst, db_id, override=True)

        return inst
