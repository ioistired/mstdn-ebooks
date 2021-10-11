#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only

import re
import anyio
import pleroma
import contextlib
from third_party import utils

def parse_args():
	return utils.arg_parser_factory(description='Reply service. Leave running in the background.').parse_args()

class ReplyBot:
	def __init__(self, cfg):
		self.cfg = cfg
		self.pleroma = pleroma.Pleroma(access_token=cfg['access_token'], api_base_url=cfg['site'])

	async def run(self):
		async with self.pleroma as self.pleroma:
			self.me = (await self.pleroma.me())['id']
			self.follows = frozenset(user['id'] for user in await self.pleroma.following(self.me))
			async for notification in self.pleroma.stream_mentions():
				await self.process_notification(notification)

	async def process_notification(self, notification, retry_count=0):
		acct = "@" + notification['account']['acct']  # get the account's @
		post_id = notification['status']['id']

		# catch HTTP 500 and backoff on requests
		retry_count = retry_count + 1
		try:
			context = await self.pleroma.status_context(post_id)
		except pleroma.BadResponse as exc:
			if retry_count < 3:
				await anyio.sleep(2**retry_count)
				await self.process_notification(notification, retry_count)
			else:
				# failed too many times in a row, logging
				print(f"Received HTTP 500 {retry_count} times in a row, aborting reply attempt.")
			return

		# check if we've already been participating in this thread
		if self.check_thread_length(context):
			return

		content = self.extract_toot(notification['status']['content'])
		if content in {'pin', 'unpin'}:
			await self.process_command(context, notification, content)
		else:
			await self.reply(notification)

	def check_thread_length(self, context) -> bool:
		"""return whether the thread is too long to reply to"""
		posts = 0
		for post in context['ancestors']:
			if post['account']['id'] == self.me:
				posts += 1
			if posts >= self.cfg['max_thread_length']:
				return True

		return False

	async def process_command(self, context, notification, command):
		post_id = notification['status']['id']
		if notification['account']['id'] not in self.follows: # this user is unauthorized
			await self.pleroma.react(post_id, '❌')
			return

		# find the post the user is talking about
		for post in context['ancestors']:
			if post['id'] == notification['status']['in_reply_to_id']:
				target_post_id = post['id']

		try:
			await (self.pleroma.pin if command == 'pin' else self.pleroma.unpin)(target_post_id)
		except pleroma.BadRequest as exc:
			async with anyio.create_task_group() as tg:
				tg.start_soon(self.pleroma.react, post_id, '❌')
				tg.start_soon(self.pleroma.reply, notification['status'], 'Error: ' + exc.args[0])
		else:
			await self.pleroma.react(post_id, '✅')

	async def reply(self, notification):
		toot = await utils.make_post(self.cfg)  # generate a toot
		await self.pleroma.reply(notification['status'], toot, cw=self.cfg['cw'])

	@staticmethod
	def extract_toot(toot):
		text = utils.extract_post_content(toot)
		text = re.sub(r"^@\S+\s", r"", text)  # remove the initial mention
		text = text.lower()  # treat text as lowercase for easier keyword matching (if this bot uses it)
		return text

async def amain():
	args = parse_args()
	cfg = utils.load_config(args.cfg)
	await ReplyBot(cfg).run()

if __name__ == '__main__':
	with contextlib.suppress(KeyboardInterrupt):
		anyio.run(amain)
