# SPDX-License-Identifier: AGPL-3.0-only

import anyio
import contextlib
from functools import wraps
from datetime import datetime, timezone

def as_corofunc(f):
	@wraps(f)
	async def wrapped(*args, **kwargs):
		# can't decide if i want an `anyio.sleep(0)` here.
		return f(*args, **kwargs)
	return wrapped

def as_async_cm(cls):
	@wraps(cls, updated=())  # cls.__dict__ doesn't support .update()
	class wrapped(cls, contextlib.AbstractAsyncContextManager):
		__aenter__ = as_corofunc(cls.__enter__)
		__aexit__ = as_corofunc(cls.__exit__)
	return wrapped

suppress = as_async_cm(contextlib.suppress)

def shield(f):
	@wraps(f)
	async def shielded(*args, **kwargs):
		with anyio.CancelScope(shield=True):
			return await f(*args, **kwargs)
	return shielded

def removeprefix(s, prefix):
	try:
		return s.removeprefix(prefix)
	except AttributeError:
		# compatibility for pre-3.9
		return s[len(prefix):] if s.startswith(prefix) else s

async def sleep_until(dt):
	await anyio.sleep((dt - datetime.now(timezone.utc)).total_seconds())

class HandleRateLimits:
	def __init__(self, http):
		self.http = http

	def request(self, *args, **kwargs):
		return _RateLimitContextManager(self.http, args, kwargs)

class _RateLimitContextManager(contextlib.AbstractAsyncContextManager):
	def __init__(self, http, args, kwargs):
		self.http = http
		self.args = args
		self.kwargs = kwargs

	async def __aenter__(self):
		self._request_cm = self.http.request(*self.args, **self.kwargs)
		return await self._do_enter()

	async def _do_enter(self):
		resp = await self._request_cm.__aenter__()
		if resp.headers.get('X-RateLimit-Remaining') not in {'0', '1'}:
			return resp

		await sleep_until(datetime.fromisoformat(resp.headers['X-RateLimit-Reset'].replace('Z', '+00:00')))
		await self._request_cm.__aexit__(*(None,)*3)
		return await self.__aenter__()

	async def __aexit__(self, *excinfo):
		return await self._request_cm.__aexit__(*excinfo)
