import os
import asyncio
import logging

import tornado.web
import tornado.httpserver
import tornado.platform.asyncio
from tornado.options import define
from tornado.options import options

import asyncio_mongo

from wdim.server.api import v1


logger = logging.getLogger(__name__)

define('debug', default=True, help='Debug mode')
define('port', default=1212, help='The port to listen on')
define('host', default='127.0.0.01', help='The host to listen on')
define('db', default='127.0.0.1:27017', help='TokuMX URI')
define('collection', default='wdim', help='The Mongo collection to use')
define('dbname', default='wdim20150921', help='The Mongo database to use')


def api_to_handlers(api, **kwargs):
    logger.info('Loading api module {}'.format(api))
    return [
        (os.path.join('/', api.PREFIX, pattern.lstrip('/')), handler, kwargs)
        for (pattern, handler) in api.HANDLERS
    ]


def make_app(debug):
    db, port = options.db.split(':')
    database = asyncio.get_event_loop().run_until_complete(
        asyncio_mongo.Connection.create(db, int(port))
    )[options.dbname]

    collection = database[options.collection]

    return tornado.web.Application(
        api_to_handlers(v1, database=database, collection=collection),
        debug=options.debug,
    )


def serve():
    tornado.platform.asyncio.AsyncIOMainLoop().install()

    app = make_app(options.debug)  # Debug mode

    logger.info('Listening on {}:{}'.format(options.host, options.port))
    app.listen(options.port, options.host)

    asyncio.get_event_loop().set_debug(options.debug)
    asyncio.get_event_loop().run_forever()
