import asyncio

from wdim import client
from wdim.client.database import MongoLayer
from wdim.client.database import ElasticSearchLayer


async def main():
    mongo_layer = await MongoLayer.connect()
    es_layer = await ElasticSearchLayer.connect()

    connection = es_layer >> mongo_layer
    # connection = CompoundWriteLayer(es_layer, mongo_layer)

    assert await client.Storable.connect(connection)

    await connection.migrate(client.Blob)
    await connection.migrate(client.Namespace)
    await connection.migrate(client.Collection)
    await connection.migrate(client.JournalEntry)


if __name__ == '__main__':
    loop = asyncio.get_event_loop().run_until_complete(main())
