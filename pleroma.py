# SPDX-License-Identifier: AGPL-3.0-only

import sys
import yarl
import json
import hashlib
import aiohttp
from http import HTTPStatus

def http_session_factory(headers={}):
	py_version = '.'.join(map(str, sys.version_info))
	user_agent = (
		'pleroma-ebooks (https://github.com/ioistired/pleroma-ebooks); '
		'aiohttp/{aiohttp.__version__}; '
		'python/{py_version}'
	)
	return aiohttp.ClientSession(
		headers={'User-Agent': user_agent, **headers},
	)

class BadRequest(Exception):
	pass

class BadResponse(Exception):
	pass

class Pleroma:
	def __init__(self, *, api_base_url, access_token):
		self.api_base_url = api_base_url.rstrip('/')
		self.access_token = access_token
		self._session = http_session_factory({'Authorization': 'Bearer ' + access_token})
		self._logged_in_id = None

	async def __aenter__(self):
		self._session = await self._session.__aenter__()
		return self

	async def __aexit__(self, *excinfo):
		return await self._session.__aexit__(*excinfo)

	async def request(self, method, path, **kwargs):
		# blocklist of some horrible instances
		if hashlib.sha256(
			yarl.URL(self.api_base_url).host.encode()
			+ bytes.fromhex('d590e3c48d599db6776e89dfc8ebaf53c8cd84866a76305049d8d8c5d4126ce1')
		).hexdigest() in {
			'56704d4d95b882e81c8e7765e9079be0afc4e353925ba9add8fd65976f52db83',
			'1932431fa41a0baaccce7815115b01e40e0237035bb155713712075b887f5a19',
			'a42191105a9f3514a1d5131969c07a95e06d0fdf0058f18e478823bf299881c9',
		}:
			raise RuntimeError('stop being a chud')

		async with self._session.request(method, self.api_base_url + path, **kwargs) as resp:
			if resp.status == HTTPStatus.BAD_REQUEST:
				raise BadRequest((await resp.json())['error'])
			if resp.status == HTTPStatus.INTERNAL_SERVER_ERROR:
			    raise BadResponse((await resp.json()))
			#resp.raise_for_status()
			return await resp.json()

	async def verify_credentials(self):
		return await self.request('GET', '/api/v1/accounts/verify_credentials')

	me = verify_credentials

	async def _get_logged_in_id(self):
		if self._logged_in_id is None:
			self._logged_in_id = (await self.me())['id']
		return self._logged_in_id

	async def following(self, account_id=None):
		account_id = account_id or await self._get_logged_in_id()
		return await self.request('GET', f'/api/v1/accounts/{account_id}/following')

	@staticmethod
	def _unpack_id(obj):
		if isinstance(obj, dict) and 'id' in obj:
			return obj['id']
		return obj

	async def status_context(self, id):
		id = self._unpack_id(id)
		try:
			return await self.request('GET', f'/api/v1/statuses/{id}/context')
		except BadResponse as exc:
			raise exc

	async def post(self, content, *, in_reply_to_id=None, cw=None, visibility=None):
		if visibility not in {None, 'private', 'public', 'unlisted', 'direct'}:
			raise ValueError('invalid visibility', visibility)

		in_reply_to_id = self._unpack_id(in_reply_to_id)

		data = dict(status=content, in_reply_to_id=in_reply_to_id)
		if visibility is not None:
			data['visibility'] = visibility
		if cw is not None:
			data['spoiler_text'] = cw

		return await self.request('POST', '/api/v1/statuses', data=data)

	async def reply(self, to_status, content, *, cw=None):
		user_id = await self._get_logged_in_id()

		mentioned_accounts = {}
		mentioned_accounts[to_status['account']['id']] = to_status['account']['acct']
		for account in to_status['mentions']:
			if account['id'] != user_id and account['id'] not in mentioned_accounts:
				mentioned_accounts[account['id']] = account['acct']

		content = ''.join('@' + x + ' ' for x in mentioned_accounts.values()) + content

		visibility = 'unlisted' if to_status['visibility'] == 'public' else to_status['visibility']
		if cw is None and 'spoiler_text' in to_status and to_status['spoiler_text']:
			cw = 're: ' + to_status['spoiler_text']

		return await self.post(content, in_reply_to_id=to_status['id'], cw=cw, visibility=visibility)

	async def favorite(self, id):
		id = self._unpack_id(id)
		return await self.request('POST', f'/api/v1/statuses/{id}/favourite')

	async def unfavorite(self, id):
		id = self._unpack_id(id)
		return await self.request('POST', f'/api/v1/statuses/{id}/unfavourite')

	async def react(self, id, reaction):
		id = self._unpack_id(id)
		return await self.request('PUT', f'/api/v1/pleroma/statuses/{id}/reactions/{reaction}')

	async def remove_reaction(self, id, reaction):
		id = self._unpack_id(id)
		return await self.request('DELETE', f'/api/v1/pleroma/statuses/{id}/reactions/{reaction}')

	async def pin(self, id):
		id = self._unpack_id(id)
		return await self.request('POST', f'/api/v1/statuses/{id}/pin')

	async def unpin(self, id):
		id = self._unpack_id(id)
		return await self.request('POST', f'/api/v1/statuses/{id}/unpin')

	async def stream(self, stream_name, *, target_event_type=None):
		async with self._session.ws_connect(
			self.api_base_url + f'/api/v1/streaming?stream={stream_name}&access_token={self.access_token}'
		) as ws:
			async for msg in ws:
				if msg.type == aiohttp.WSMsgType.TEXT:
					event = msg.json()
					# the only event type that doesn't define `payload` is `filters_changed`
					if event['event'] == 'filters_changed':
						yield event
					elif target_event_type is None or event['event'] == target_event_type:
						# don't ask me why the payload is also JSON encoded smh
						yield json.loads(event['payload'])

	async def stream_notifications(self):
		async for notif in self.stream('user:notification', target_event_type='notification'):
			yield notif

	async def stream_mentions(self):
		async for notif in self.stream_notifications():
			if notif['type'] == 'mention':
				yield notif
