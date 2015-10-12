import os
import json
import asyncio

from wdim import client
from wdim.orm import Storable
from wdim.orm import exceptions
from wdim.orm.database import MongoLayer
from wdim.orm.database import EmbeddedElasticSearchLayer


cards_loc = os.path.join(os.path.split(__file__)[0], 'cards.json')


async def main():
    mongo_layer = await MongoLayer.connect()
    es_layer = await EmbeddedElasticSearchLayer.connect()

    assert await Storable.connect(mongo_layer)
    assert await client.Document.connect(es_layer)
    assert await client.Journal.connect(es_layer >> mongo_layer)

    try:
        ns = await client.Namespace.create(name='system')
    except exceptions.UniqueViolation:
        ns = await client.Namespace.get_by_name('system')

    try:
        collection = await ns.create_collection('users')
    except exceptions.UniqueViolation:
        pass

    try:
        ns = await client.Namespace.create(name='cardapp')
    except exceptions.UniqueViolation:
        ns = await client.Namespace.get_by_name('cardapp')

    try:
        collection = await ns.create_collection('placements')
    except exceptions.UniqueViolation:
        collection = await ns.get_collection('placements')

    try:
        collection = await ns.create_collection('cards')
    except exceptions.UniqueViolation:
        collection = await ns.get_collection('cards')

    with open(cards_loc) as cards:
        for card in json.load(cards)['cards']:
            try:
                entry = await collection.read(card['id'])
                blob = await entry.blob
                assert blob.data['content'] == card['content']
            except AssertionError:
                await collection.update(card['id'], {'content': card['content']}, 'sys')
            except exceptions.NotFound:
                await collection.create(card['id'], {'content': card['content']}, 'sys')


if __name__ == '__main__':
    loop = asyncio.get_event_loop().run_until_complete(main())
