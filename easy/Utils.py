class Utils:
	@staticmethod
	def ordered(obj):
		if isinstance(obj, dict):
			return sorted((k, Utils.ordered(v)) for k, v in obj.items())
		if isinstance(obj, list):
			return sorted(Utils.ordered(x) for x in obj)
		else:
			return obj

	@staticmethod
	def requests_retry_session(
			retries=3,
			backoff_factor=0.3,
			status_forcelist=(500, 502, 504),
			session=None,
	):
		session = session or requests.Session()
		retry = Retry(
			total=retries,
			read=retries,
			connect=retries,
			backoff_factor=backoff_factor,
			status_forcelist=status_forcelist,
		)
		adapter = HTTPAdapter(max_retries=retry)
		session.mount('http://', adapter)
		session.mount('https://', adapter)
		return session

	@staticmethod
	def read_s3_file(s3, bucket, key):
		"""
		:return a json from s3 as a dictionary
		:param s3: s3 object
		:param bucket: s3 bucket
		:param key: s3 key (with the specified json)
		:return: a dictionary created from the json
		"""
		try:
			s3_obj = s3.Object(bucket, key).get()
			file_str = s3_obj['Body'].read().decode('utf-8')
		except Exception as e:
			error_msg = f'exception: {e} - file {bucket}/{key} not found'
			raise FileNotFoundError(error_msg)

		return file_str

