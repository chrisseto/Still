from typing import Optional

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
    permissions = fields.DictField()
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
                ((Journal.action == Action.UPDATE_SCHEMA.value) | (Journal.action == Action.DELETE_SCHEMA.value)),
                limit=1, sort=sort.Descending(Journal.modified_by)
            ))
        except StopIteration:
            return None

        if entry.action == Action.DELETE_SCHEMA:
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
            action=Action.UPDATE_SCHEMA,
            record_id=self.SCHEMA_RESERVED,
        )

    async def delete_schema(self):
        return await Journal.create(
            blob=None,
            record_id=key,
            collection=self._id,
            action=Action.DELETE,
            namespace=self.namespace,
        )

    @util.combomethod
    async def create(self, key, data, user):
        schema = await self.get_schema()
        if schema:
            schema.validate(data)

        try:
            await self.read(key)
        except exceptions.NotFound:
            pass
        else:
            raise exceptions.UniqueViolation(key)

        blob = await Blob.create(data)

        entry = await Journal.create(
            record_id=key,
            action=Action.CREATE,
            created_by=user,
            blob=blob._id,
            namespace=self.namespace,
            collection=self._id,
        )

        return await Document.create_from_entry(entry)

    @util.combomethod
    async def read(self, key):
        return await Document.load('{}::{}'.format(self._id, key))

    @util.combomethod
    async def update(self, key, data, user, merge=False):
        previous = await self.read(key)

        if merge:
            data = util.merge((await previous.blob).data, data)

        schema = await self.get_schema()
        if schema:
            schema.validate(data)

        blob = await Blob.create(data)

        entry = await Journal.create(
            modified_by=user,
            created=previous.created,
            created_by=previous.created_by,
            action=Action.UPDATE,
            record_id=key,
            blob=blob._id,
            namespace=self.namespace,
            collection=self._id,
        )

        return await Document.create_from_entry(entry)

    @util.combomethod
    async def delete(self, key, user):
        previous = await self.read(key)

        entry = await Journal.create(
            modified_by=user,
            created=previous.created,
            created_by=previous.created_by,
            blob=None,
            record_id=key,
            collection=self._id,
            action=Action.DELETE,
            namespace=self.namespace,
        )

        previous.delete()

        return None
