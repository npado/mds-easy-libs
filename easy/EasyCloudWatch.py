import boto3
import time


class EasyCloudWatch:
	def __init__(self, log_group, log_stream, profile_name=None):
		self.log_group = log_group
		self.log_stream = log_stream

		if profile_name:
			session = boto3.Session(profile_name=profile_name)
			self.logs = session.client('logs')
		else:
			self.logs = boto3.client('logs')

		try:
			log_group_description = self.logs.describe_log_streams(
				logGroupName=self.log_group,
				logStreamNamePrefix=self.log_stream
			)
			self.next_token = log_group_description['logStreams'][0]['uploadSequenceToken']
		except self.logs.exceptions.ResourceNotFoundException:
			self.next_token = None

	def put_log_events(self, message):
		timestamp = int(round(time.time() * 1000))
		datetime_str = time.strftime('%Y-%m-%d %H:%M:%S')
		response = self.logs.put_log_events(
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
		self.next_token = response['nextSequenceToken']
		return response
