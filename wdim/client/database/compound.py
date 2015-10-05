from wdim.client.database.base import DatabaseLayer


class CompoundWriteLayer(DatabaseLayer):

    def __init__(self, base, write):
        self.base_layer = base
        self.write_layer = write

    async def insert(self, inst):
        write_id = await self.write_layer.insert(inst)

        inst.__class__._fields['_id'].__set__(inst, write_id, override=True)

        base_id = await self.base_layer.insert(inst)

        assert str(base_id) == str(write_id), 'Base layer and write layer id did not match, ({} != {})'.format(base_id, write_id)

        return write_id

    async def load(self, cls, _id):
        return await self.base_layer.load(cls, _id)

    async def migrate(self, model):
        await self.base_layer.drop(model)

        for inst in await self.write_layer.find(model):
            await self.base_layer.insert(model.from_document(inst))

    async def drop(self, cls):
        return await self.base_layer.drop() and await self.write_layer.drop()

    async def ensure_index(self, *args, **kwargs):
        return (
            await self.base_layer.ensure_index(*args, **kwargs) and
            await self.write_layer.ensure_index(*args, **kwargs)
        )

    async def find(self, *args, **kwargs):
        return await self.base_layer.find(*args, **kwargs)

    async def find_one(self, *args, **kwargs):
        return await self.base_layer.find_one(*args, **kwargs)
