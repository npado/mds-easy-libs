import boto3
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection


class EasyElasticSearch:
	def __init__(
			self,
			host,
			port,
			boto_session=None,
			region='eu-west-1',
			profile_name=None,
			maxsize=30,
			**kwargs
	):
		boto_session = boto3.Session(profile_name=profile_name) if boto_session is None else boto_session
		credentials = boto_session.get_credentials()

		awsauth = AWS4Auth(
			credentials.access_key,
			credentials.secret_key,
			region,
			'es',
			session_token=credentials.token
		)
		self.host = host
		self.port = port

		self.es = Elasticsearch(
			hosts=[{'host': host, 'port': port}],
			http_auth=awsauth,
			use_ssl=True,
			verify_certs=True,
			connection_class=RequestsHttpConnection,
			maxsize=maxsize,
			**kwargs
		)

	def get_elastic_client(self):
		return self.es

	def get(self, index, identifier, doc_type=None, metadata=True):
		result = self.es.get(index=index, id=identifier, doc_type=doc_type)
		return result if metadata else result['_source']

	def index(self, index, body, identifier, doc_type=None):
		return self.es.index(
			index=index,
			body=body,
			id=identifier,
			doc_type=doc_type,
		)
