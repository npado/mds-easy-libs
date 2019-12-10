from botocore.exceptions import ClientError
import json


class Utils:
	@staticmethod
	def ordered_obj(obj):
		if isinstance(obj, dict):
			return sorted((k, Utils.ordered_obj(v)) for k, v in obj.items())
		if isinstance(obj, list):
			return sorted(Utils.ordered_obj(x) for x in obj)
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
	def json_already_exists(s3, json_result, s3_bucket, s3_key, drop=None):
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

	@staticmethod
	def write_json_if_toupdate(s3, json_result, s3_bucket, s3_key):
		if not S3Utils.json_already_exists(
				s3,
				json_result,
				s3_bucket,
				s3_key,
				drop=None
		):
			S3Utils.write_s3_file(s3, s3_bucket, s3_key, json.dumps(json_result, ensure_ascii=False))
			return True
		return False


class MetadataUtils:
	@staticmethod
	def split_meta_value(string, sep='='):
		metadata = string[:string.index(sep)]
		value = string[string.index(sep) + 1:]

		return metadata, value

	@staticmethod
	def get_unique_metas(lst, sep='='):
		"""
		Given a list of metadata{sep}value returns the distinct list of metadata.
			>>> lst = ['attore=terence', 'attore=bud', 'regista=leone', 'genere=western']
			>>> MetadataUtils.get_unique_metas(lst, '=')
			>>> ['attore','regista','genere']

		:param lst: the list of metadata=key
		:param sep: a separator
		:return: list of distinct metadata
		"""
		return list(set([MetadataUtils.split_meta_value(meta, sep)[0] for meta in lst]))

	@staticmethod
	def get_meta_value(name, lst, sep='='):
		"""
		Given a list of metadata{sep}value and given the name of a certain metadata return all values
		of that metadata name
			>>> lst = ['attore=terence', 'attore=bud', 'regista=leone', 'genere=western']
			>>> MetadataUtils.get_meta_value('attore', lst, '=')
			>>> ['terence', 'bud']
		:param name:
		:param lst:
		:param sep:
		:return:
		"""
		ret = [MetadataUtils.split_meta_value(l, sep)[1] for l in lst if MetadataUtils.split_meta_value(l, sep)[0] == name]
		if len(ret) > 1:
			return ret
		else:
			return ret[0]

	@staticmethod
	def get_clear_meta_dict(lst, blacklist=None, sep='='):
		"""
		TODO: questo metodo serve a "raggruppare" i metadati per chiavi creando
			una lista di valori. Il flusso MythematicsFingerprint, in realtà, già
			calcola questo raggruppamento ma non lo fa MCMFingerprint. Quindi nel
			flusso di merge è ancora necessario seppure sovrabbondante nella parte
			Mythematics. La modalità pulita è utilizzare questo metodo separatamente
			nel flusso MCMFingerprint e in quello MythematicsFingerprint evitando
			di farlo in questto flusso di Merge. La ragione è che inizialmente
			veniva fatto solo qui alla fine, ma è stato richiesto che MythematicsFingerprint
			scriva anche dati per Ares (a cui serve questo raggruppamento)
			ATTENZIONE: si può evitare di utilizzare questo metodo in questo flusso
			ma bisogna fare attenzione al fatto che ci sono metadati Mythematics
			(ad es. people-attore) che hanno "precedenza" rispetto a quelli MCM: questa
			precedenza viene calcolata nel metodo merge_list (parametro common): successivamente
			a questo merge_list e quindi a questa "normalizzazione" viene applicato get_clear_meta_dict
			Se lo vogliamo eliminare prima di fare una semplice UNIONE tra il clear_meta di Mythematics
			con quello di MCM bisogna fare attenzione al fatto che ci deve
			essere una precedenza di Mythematics rispetto a MCM.

		Given a list of metadata{sep}value return a dictionary whose keys are the
		unique metadata in list grouped by their values
			>>> lst = ['attore=terence', 'attore=bud', 'regista=leone', 'genere=western', 'anno=1970']
			>>> blacklist = ['genere', 'regista']
			>>> MetadataUtils.get_clear_meta_dict(lst, blacklist, '=')
			>>> { "attore": ['terence', 'bud'], "anno": "1970"}

		:param lst:
		:param blacklist:
		:param sep:
		:return:
		"""
		res_dict = {}
		metas = MetadataUtils.get_unique_metas(lst, sep=sep)
		if blacklist is not None:
			metas = [i for i in metas if i not in blacklist]

		cols_number = [
			"annoproduzione",
			"productionvalue",
			"realismocontenuto",
			"durataepisodi",
			"erotismo",
			"linguaggioverbalevolgare",
			"presenzaimmaginiforti",
			"umorismo",
			"violenza",
			"ritmodelracconto",
			"numeroepisodi"
		]
		for l in metas:
			val = MetadataUtils.get_meta_value(l, lst, sep=sep)
			if l in cols_number:
				val = int(val)

			res_dict[l] = val

		return res_dict

	@staticmethod
	def del_items_from_clearmeta(key, lst, sep='='):
		"""
		delete a key from clear_meta. Example:
			>>> clear_meta = ['attore=terence', 'attore=bud', 'regista=e.b clucher']
			>>> MetadataUtils.del_items_from_clearmeta('attore',clear_meta)
			>>> ['regista=e.b clucher']

		:param key:
		:param lst:
		:param sep:
		:return:
		"""
		return [l for l in lst if not l.startswith(key + sep)]

	@staticmethod
	def check_elem_in_clearmeta(key, lst, sep='='):
		"""
		check if a key is in lst. Example:
			>>> clear_meta = ['attore=terence', 'attore=bud', 'regista=e.b clucher']
			>>> MetadataUtils.check_elem_in_clearmeta('attore', clear_meta)
			>>> True
			>>> MetadataUtils.check_elem_in_clearmeta('sceneggiatore', clear_meta)
			>>> False
		:param key:
		:param lst:
		:param sep:
		:return:
		"""
		for l in lst:
			if l.startswith(key + sep):
				return True
		return False

	@staticmethod
	def merge_lst(common_metas, mcm_clearmeta, mythem_clearmeta, sep='='):
		"""
		Given a list of common_metas (i.e. metadata in common between mcm and mythematics),
		this method remove the item from MCM data if it is present also in mythematics data.
		At the end, return a sorted merged (mythem+mcm) list of clearmeta

		>>> common_metas = ['genere','anno']
		>>> mcm_clearmeta = ['genere=western', 'attore=terence', 'anno=1974']
		>>> mythem_clearmeta = ['genere=comedy', 'attore=terence', 'regista=leone']
		>>> MetadataUtils.merge_lst(common_metas, mcm_clearmeta, mythem_clearmeta)
		>>> ['genere=comedy', 'attore=terence', 'regista=leone', 'anno=1974']
		>>> # genere=comedy taken from Mythematics, attore=terence obtained from list(set(.))
		>>> # regista=leone taken from Mythematics, anno=1974 taken from MCM because
		>>> # there is no 'anno' in mythematics

		:param common_metas:
		:param mcm_clearmeta:
		:param mythem_clearmeta:
		:param sep:
		:return:
		"""
		# ci sono certi metadati che devono essere presi da Mythematics
		for t in common_metas:
			if MetadataUtils.check_elem_in_clearmeta(t, mcm_clearmeta, sep) and \
					MetadataUtils.check_elem_in_clearmeta(t, mythem_clearmeta, sep):
				mcm_clearmeta = MetadataUtils.del_items_from_clearmeta(t, mcm_clearmeta, sep)

		return sorted(list(set(mcm_clearmeta + mythem_clearmeta)))

	@staticmethod
	def hashing_meta(array, encoding='utf-8', sep='=', bl=None):
		"""
		In questo flusso di Merge il fingerprint viene completamente ricalcolato e non viene fatta una unione
		tra il fingerprint MCM e quello Mythematics perchè ci sono alcuni metadati in MCM (ad es. people-attore)
		che devono essere rimpiazzati dai metadati di Mythematics che hanno "precedenza"
		:param array:
		:param encoding:
		:param sep:
		:param bl: blacklist of metadata
		:return:
		"""
		bl = [] if bl is None else bl
		arr = [hashlib.sha256(r.replace(sep, '').encode(encoding)).hexdigest() for r in array if r[:r.index(sep)] not in bl]
		hashed_lst = sorted(arr)
		return ' '.join(hashed_lst)
		# hashed_lst = sorted([hashlib.sha256(r.replace(sep, '').encode(encoding)).hexdigest() for r in array])
		# return ' '.join(hashed_lst)


class MCMHelper:
	def __init__(self, s3, conf):
		self.data_conf = conf['data-conf']
		self.slack_conf = conf['slack']
		self.content_conf = conf['content-conf']

		self.s3 = s3
		self._logger = logging.getLogger(self.__class__.__name__)

	def read_mythem_js(self, content_id):
		"""
		Given an fcode return the dictionary (json) of the corresponding Mythematics len-10-Fcode content
		:param content_id:
		:return:
		"""

		mythem_key_file = f"{self.data_conf['s3_mythematics_fingerprint']}/{content_id}.json"
		s3_bucket = self.data_conf['s3_bucket']
		try:
			self._logger.info(f"Searching {content_id} in MYTHEMATICS from {s3_bucket}/{mythem_key_file}""")
			mythem_js = S3Utils.read_s3_json(self.s3, s3_bucket, mythem_key_file)
			self._logger.info(f'MYTHEMATICS {content_id} found!')
			return mythem_js
		except FileNotFoundError:
			self._logger.info(f'MYTHEMATICS {content_id} NOT found!')
			return None

	def merge_mcm_mythem(self, mythem_dict, mcm_dict, common_metas, sep='='):
		"""
		Given the dictionary of Mythematics and the dictionary of MCM
		this methods return the dictionary of MCM with the keys
		`fingerprint` and `clear_meta` 	augmented by Mythematics dict

		:param mythem_dict: Mythematics dictionary
		:param mcm_dict: MCM dictionary
		:param common_metas:
		:param sep:
		:return: MCM dict augmented by Mythematics dict
		"""
		clear_meta_name = self.content_conf['ClearMetadata']
		fing_name = self.content_conf['Fingerprint']

		mythem_clear_meta = mythem_dict[clear_meta_name]
		mcm_clear_meta = mcm_dict[clear_meta_name]

		clear_meta_merged = MetadataUtils.merge_lst(common_metas, mcm_clear_meta, mythem_clear_meta, sep=sep)

		mcm_dict[clear_meta_name] = clear_meta_merged
		mcm_dict[fing_name] = MetadataUtils.hashing_meta(clear_meta_merged, sep=sep, bl=self.content_conf['blacklist_metas'])
		return mcm_dict

	def search_mythematics_meta(self, fcode, mcm_js, common_metas, sep='='):
		"""

		:param fcode:
		:param mcm_js:
		:param common_metas:
		:param sep:
		:return:
		"""
		mythem_js = self.read_mythem_js(fcode)
		if mythem_js is None:
			mcm_js['mythematics-source'] = 'mcm'
			return mcm_js
		else:
			mcm_js['mythematics-source'] = 'mythematics'

		return self.merge_mcm_mythem(mythem_js, mcm_js, common_metas, sep=sep)

