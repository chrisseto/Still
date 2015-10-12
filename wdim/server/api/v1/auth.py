import uuid
import asyncio
import datetime

import jwt
import aiohttp
import tornado.web

from wdim.orm import exceptions
from wdim.client import Blob
from wdim.client import Namespace
from wdim.client import Journal
from wdim.server.api.v1.base import BaseAPIHandler


class AuthHandler(BaseAPIHandler):
    PATTERN = '/namespaces/(?P<namespace>\w+?)/auth/?'

    @asyncio.coroutine
    def _osf(self, data):
        resp = yield from aiohttp.request('GET', 'https://staging-accounts.osf.io/oauth2/profile', headers={
            'Authorization': 'Bearer {}'.format(data['access_token'])
        })

        assert resp.status == 200
        return (yield from resp.json())['id']

    @asyncio.coroutine
    def _anon(self, data):
        return str(uuid.uuid4()).replace('-', '')

    @tornado.gen.coroutine
    def post(self, namespace):
        data = self.json['data']
        assert data['type'] == 'users'
        provider = data['attributes'].pop('provider')

        uid = yield from getattr(self, '_' + provider)(data['attributes'])

        signed_jwt = jwt.encode({
            'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=1),
            'sub': '{}-{}'.format(provider, uid)
        }, 'TestKey')

        self.write({'data': {
            'id': '{}-{}'.format(provider, uid),
            'type': 'users',
            'attributes': {
                'token': signed_jwt.decode()
            }
        }})

        # Everyone (world)
        # Anon ("Tracked")
        # Authenticated via 3rd party
        #   send provider and access key
        #   We translate to a persistant ID
