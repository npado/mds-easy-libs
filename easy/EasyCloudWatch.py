import boto3
import time


class EasyCloudWatch:
	def __init__(self, log_group, log_stream, retention_in_days=365, profile_name=None):
		self.log_group = log_group
		self.log_stream = log_stream

		if profile_name:
			session = boto3.Session(profile_name=profile_name)
			self.logs = session.client('logs')
		else:
			self.logs = boto3.client('logs')

		self.next_token = None
		try:
			self.logs.create_log_group(logGroupName=self.log_group)
		except self.logs.exceptions.ResourceAlreadyExistsException:
			pass
		else:
			self.logs.put_retention_policy(
				logGroupName=self.log_group,
				retentionInDays=retention_in_days
			)

		try:
			self.logs.create_log_stream(logGroupName=self.log_group, logStreamName=self.log_stream)
		except self.logs.exceptions.ResourceAlreadyExistsException:
			log_stream_list = self.logs.describe_log_streams(
				logGroupName=self.log_group,
				logStreamNamePrefix=self.log_stream
			)['logStreams']
			for ls in log_stream_list:
				if ls['logStreamName'] == self.log_stream:
					self.next_token = ls.get('uploadSequenceToken')
					break

	def put_log_events(self, message):
		timestamp = int(round(time.time() * 1000))
		datetime_str = time.strftime('%Y-%m-%d %H:%M:%S')

		put_log_args = dict(
			logGroupName=self.log_group,
			logStreamName=self.log_stream,
			logEvents=[
					{
						'timestamp': timestamp,
						'message': f'{datetime_str}\t{message}'
					}
				],
			sequenceToken=self.next_token
		)

		response = self.logs.put_log_events(**{k: v for k, v in put_log_args.items() if v is not None})
		self.next_token = response['nextSequenceToken']
		return response
