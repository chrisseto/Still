from datetime import datetime


class VersionedStorage:

    def __init__(self, collection):
        self.collection = collection

    async def get(self, _id, version=None):
        if version is None:
            version = -1

        blob = await self.collection.find_one({
            '_id': _id
        }, {
            '_version': True,
            '_versions': {'$slice': [version, 1]}
        })

        if not blob:
            # TODO
            return None

        return {
            '_id': blob['_id'],
            'version': blob['_version'],
            'data': blob['_versions'][0]['_data']
        }

    async def set(self, _id, data):
        _id = _id or str(ObjectId())

        await self.collection.update({
            '_id': _id
        }, {
            '$inc': {'_version': 1},
            '$push': {'_versions': {
                '_data': data,
                '_timestamp': datetime.utcnow()
            }}
        }, upsert=True)

        return _id

    async def update(self, _id, data):
        current = await self.get(_id) or {}
        return await self.set(_id, {**current.get('data', {}), **data})
