import asyncio

from wdim import orm
from wdim import client
from wdim.orm.database import MongoLayer
from wdim.orm.database import EmbeddedElasticSearchLayer


async def main():
    mongo_layer = await MongoLayer.connect()
    es_layer = await EmbeddedElasticSearchLayer.connect()

    connection = es_layer >> mongo_layer
    # connection = CompoundWriteLayer(es_layer, mongo_layer)

    assert await orm.Storable.connect(connection)

    print('Migrating Blobs')
    await connection.migrate(client.Blob)
    print('Migrating Namespaces')
    await connection.migrate(client.Namespace)
    print('Migrating Collections')
    await connection.migrate(client.Collection)
    print('Migrating JournalEntries')
    await connection.migrate(client.Journal)


if __name__ == '__main__':
    loop = asyncio.get_event_loop().run_until_complete(main())
