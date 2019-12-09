from botocore.exceptions import ClientError
import json


class S3Utils:
	@staticmethod
	def write_s3_file(s3, bucket, key, str_file):
		"""
		:param s3:
		:param bucket:
		:param key:
		:param str_file:
		:return:

		"""
		w_obj = s3.Object(bucket, key)
		return w_obj.put(Body=str_file, ensure_ascii=False)

	@staticmethod
	def read_s3_file(s3, bucket, key):
		"""
		return a json from s3 as a dictionary
		:param s3: s3 object
		:param bucket: s3 bucket
		:param key: s3 key (with the specified json)
		:return: a dictionary created from the json
		"""
		try:
			s3_obj = s3.Object(bucket, key).get()
			js_string = s3_obj['Body'].read().decode('utf-8')
		except Exception as e:
			error_msg = f'exception: {e} - file {bucket}/{key} not found'
			raise FileNotFoundError(error_msg)

		return js_string

	@staticmethod
	def content_card_already_exists(s3, json_result, s3_bucket, s3_key, drop=None):
		current_ordered_js = Utils.get_ordered_json(json_result, drop=drop)

		try:
			last_ordered_js = Utils.get_ordered_json(
				f's3://{s3_bucket}/{s3_key}',
				s3,
				drop=drop
			)
			return current_ordered_js == last_ordered_js
		except FileNotFoundError:
			return False


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
	def get_ordered_json(js, s3=None, drop=None):
		if type(js) == str:
			if js.startswith('s3'):
				s3_path = js.replace('s3://', '').split('/')
				try:
					s3_obj = s3.Object(s3_path[0], '/'.join(s3_path[1:])).get()
				except ClientError as e:
					raise FileNotFoundError(e)
				js_string = s3_obj['Body'].read().decode('utf-8')
				js = json.loads(js_string)
			else:
				with open(js) as json_file:
					js = json.load(json_file)
		elif type(js) == dict:
			pass
		else:
			raise AttributeError(f'No valid js parameter. Parameter type: {type(js)} ')

		if drop is not None:
			js = {k: v for k, v in js.items() if k not in drop}

		return Utils.ordered_obj(js)