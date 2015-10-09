import os
import json
import asyncio

from wdim import client
from wdim.orm import Storable
from wdim.orm import exceptions
from wdim.orm.database import MongoLayer
from wdim.orm.database import ElasticSearchLayer


schema_loc = os.path.join(os.path.split(__file__)[0], 'permissions_schema.json')
admin_blob = {
    'namespace': 'badges',
    'collection': 'badgeclass',
    'permissions': {
        'read': True,
        'write': True,
        'delete': True
    }
}


async def main():
    mongo_layer = await MongoLayer.connect()
    es_layer = await ElasticSearchLayer.connect()

    connection = es_layer >> mongo_layer
    # connection = CompoundWriteLayer(es_layer, mongo_layer)

    assert await Storable.connect(connection)

    try:
        ns = await client.Namespace.create(namespace='system')
    except exceptions.UniqueViolation:
        ns = await client.Namespace.find_one(client.Namespace.namespace == 'system')

    try:
        collection = await ns.create_collection('permissions')
    except exceptions.UniqueViolation:
        collection = await ns.get_collection('permissions')

    with open(schema_loc) as schema:
        await collection.set_schema({
            'type': 'jsonschema',
            'schema': json.load(schema)
        })

    try:
        admin = await collection.get('admin')
    except exceptions.NotFound:
        await collection.set('admin', admin_blob)
    else:
        if (await admin.blob).data != admin_blob:
            await collection.set('admin', admin_blob)


if __name__ == '__main__':
    loop = asyncio.get_event_loop().run_until_complete(main())
