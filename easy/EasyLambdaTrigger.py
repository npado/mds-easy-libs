import boto3
import time


class EasyLambdaTrigger:
	def __init__(self, lambda_name, region_name='eu-west-1'):
		self.ENABLING = 'Enabling'
		self.ENABLED = 'Enabled'
		self.DISABLING = 'Disabling'
		self.DISABLED = 'Disabled'
		self._state_values = [
			self.ENABLING, self.ENABLED, self.DISABLING, self.DISABLED
		]
		self.lambda_name = lambda_name
		self.lambda_client = boto3.client(service_name='lambda', region_name=region_name)

	def get_event_sources(self):
		event_source_mapping = self.lambda_client.list_event_source_mappings(FunctionName=self.lambda_name)
		return event_source_mapping['EventSourceMappings']

	def get_event_sources_uuid(self):
		return [e['UUID'] for e in self.get_event_sources()]

	def get_event_source_state(self, trigger_uuid):
		return self.lambda_client.get_event_source_mapping(UUID=trigger_uuid)['State']

	def update_trigger(self, trigger_uuid, enable, timeout=180, wait=False):
		if enable:
			wait_if_before_state = self.DISABLING
			wait_before_state = self.DISABLED
			return_states = [self.ENABLED, self.ENABLING]
			wait_state = self.ENABLED
		else:
			wait_if_before_state = self.ENABLING
			wait_before_state = self.ENABLED
			return_states = [self.DISABLED, self.DISABLING]
			wait_state = self.DISABLED

		current_state = self.get_event_source_state(trigger_uuid)

		if current_state == wait_if_before_state:
			self.wait_state(trigger_uuid, wait_before_state, timeout)
		elif current_state in return_states:
			if wait:
				self.wait_state(trigger_uuid, wait_state, timeout)
			return self.get_event_source_state(trigger_uuid)

		self.lambda_client.update_event_source_mapping(
			UUID=trigger_uuid, FunctionName=self.lambda_name,
			Enabled=enable
		)
		if wait:
			self.wait_state(trigger_uuid, wait_state, timeout)

		return self.get_event_source_state(trigger_uuid)

	def wait_state(self, trigger_uuid, state, timeout=180):
		if state not in self._state_values:
			error_msg = f"The only state values are: {','.join(self._state_values)}"
			raise ValueError(error_msg)

		start_time = time.time()
		while True:
			current_state = self.get_event_source_state(trigger_uuid)
			if current_state == state:
				return current_state
			elif time.time() - start_time > timeout:
				error_msg = f'Timeout {timeout} secs reached. Cannot wait anymore: increase timeout value'
				raise TimeoutError(error_msg)
