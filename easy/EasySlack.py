import slack
from slack.errors import SlackApiError


class EasySlack:
	def __init__(self, app_name, token):
		self.client = slack.WebClient(run_async=False, token=token)
		self.app_name = app_name
		self.no_msg_cnt = 0

	def send_message(self, channel, text):
		self.client.chat_postMessage(
			channel=channel,
			text=f'*{self.app_name}*: {text}'
		)

	def track_message(self, logger_func, channel, text):
		logger_func(text)
		try:
			if self.no_msg_cnt == 0:
				self.send_message(channel, text)
			else:
				self.no_msg_cnt -= 1
		except SlackApiError as e:
			if e.response['error'] == 'ratelimited':
				self.no_msg_cnt = 100
		except Exception:
			pass
