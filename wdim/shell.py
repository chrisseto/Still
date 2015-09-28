import asyncio

import asyncio_mongo
from tornado.options import define, options

from wdim import client


try:
    from IPython import embed
except ImportError:
    raise ImportError('Shell requires IPython >= 0.12.0')

define('port', default=27017, help='TokuMX port')
define('db', default='127.0.0.1', help='TokuMX IP')
define('dbname', default='wdim20150921', help='The Mongo database to use')
define('namespace', default='sys_shell', help='The namespace to use')


async def get_context():
    connection = await asyncio_mongo.Connection.create(options.db, options.port)
    database = connection[options.dbname]
    _client = client.WdimClient(options.namespace, database)

    return {
        'client': _client,
        'database': database,
        'mongo_connection': connection,
        'collection': _client.get_collection('ShellCollection')
    }


def main(loop):
    embed(user_ns={
        'loop': loop,
        'run': lambda x: loop.run_until_complete(x),
        **loop.run_until_complete(get_context())
    })

if __name__ == '__main__':
    options.parse_command_line()
    loop = asyncio.get_event_loop()
    main(loop)
