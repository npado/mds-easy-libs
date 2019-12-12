import logging
import json

from easy.Utils import S3Utils, MetadataUtils


class MCMHelper:
	def __init__(self, s3, conf):
		self.data_conf = conf['data-conf']
		self.slack_conf = conf['slack']
		self.content_conf = conf['content-conf']

		self.s3 = s3
		self._logger = logging.getLogger(self.__class__.__name__)

	def read_mcm_content_season(self, content_id):
		mcm_key_file = f"{self.data_conf['s3_mcm_season']}/{content_id}.json"
		s3_bucket = self.data_conf['s3_bucket']

		try:
			self._logger.info(f"Searching {content_id} in MCM from {s3_bucket}/{mcm_key_file}""")
			mcm_js = json.loads(S3Utils.read_s3_file(self.s3, s3_bucket, mcm_key_file))
			self._logger.info(f'MCM {content_id} found!')
			return mcm_js
		except FileNotFoundError:
			self._logger.info(f'MCM {content_id} NOT found!')
			return None

	def read_mythem_series_js(self, series_id):
		mythem_key_file = f"{self.data_conf['s3_mythematics_fingerprint_coll']}/{series_id}.json"
		s3_bucket = self.data_conf['s3_bucket']

		try:
			self._logger.info(f"Searching {series_id} in MYTHEMATICS from {s3_bucket}/{mythem_key_file}""")
			mythem_js = json.loads(S3Utils.read_s3_file(self.s3, s3_bucket, mythem_key_file))
			self._logger.info(f'MYTHEMATICS SERIES {series_id} found!')
			return mythem_js
		except FileNotFoundError:
			self._logger.info(f'MYTHEMATICS SERIES {series_id} NOT found!')
			return None

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
			mythem_js = json.loads(S3Utils.read_s3_file(self.s3, s3_bucket, mythem_key_file))
			self._logger.info(f'MYTHEMATICS {content_id} found!')
			return mythem_js
		except FileNotFoundError:
			self._logger.info(f'MYTHEMATICS {content_id} NOT found!')
			return None

	def read_mythem_series_js_from_fcode(self, fcode):
		"""
		Given an Fcode this method find the corresponding id-serie retrieving the 8-len
		Fcode of collection.
		:param fcode:
		:return:
		"""
		s3_bucket = self.data_conf['s3_bucket']
		s3_fcode_serie_map = self.data_conf['fcode_serie_mapping_path']
		self._logger.info(
			f'Searching {fcode} in MYTHEMATICS collection from '
			f'{s3_bucket}/{s3_fcode_serie_map}'
		)
		try:
			fcode_serie_map = json.loads(S3Utils.read_s3_file(self.s3, s3_bucket, s3_fcode_serie_map))
			idserie = fcode_serie_map['idserie'].get(fcode)
		except FileNotFoundError:
			self._logger.info(f'MYTHEMATICS {fcode} collection NOT found: file {s3_bucket}/{s3_fcode_serie_map} not found')
			return None

		if idserie is not None:
			self._logger.info(f'IdSerie {idserie}-{fcode} match found')
			mythem_js = self.read_mythem_series_js(idserie)
			self._logger.info(f'MYTHEMATICS {fcode} collection Found!')
			return mythem_js
		else:
			self._logger.info(f'MYTHEMATICS {fcode} collection NOT found!')
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

		# idserie, numeroepisodi, numerostagioni non sono presenti in MCM e lo prendiamo da Mythematics
		# NOTA: MCM+Mythematics vengono logicamente divisi in due parti: una che contiene
		# dei metadati di descrizione del contenuto (id-video, id-serie, titolo, ecc)
		# e un'altra parte di metadati veri e propri (mood, generi) che si trova in clearmeta
		# In questo caso idserie non lo prendiamo da clearmeta ma dai metadati di descrizione
		# ci dovrebbe essere un metodo distinto che fa questo lavoro: ha in input una lista di
		# metadati di descrizione (in un file di descrizione) e li attacca a MCM
		idserie = mythem_dict.get('idserie')
		if not (idserie is None or idserie.upper() in ['NULL', 'NA']):
			mcm_dict['idserie'] = idserie

		if mythem_dict.get('numeroepisodi') is not None:
			mcm_dict['numeroepisodi'] = int(mythem_dict['numeroepisodi'])

		if mythem_dict.get('numerostagioni') is not None:
			mcm_dict['numerostagioni'] = int(mythem_dict['numerostagioni'])

		mcm_dict[fing_name] = MetadataUtils.hashing_meta(clear_meta_merged, sep=sep, bl=self.content_conf['blacklist_metas'])
		return mcm_dict

	def search_mcm_meta(self, fcode, mythem_js, common_metas, sep='='):
		mcm_js = self.read_mcm_content_season(fcode)
		if mcm_js is None:
			return None

		mcm_js['mythematics-source'] = 'mythematics'
		return self.merge_mcm_mythem(mythem_js, mcm_js, common_metas, sep=sep)

	def search_mythematics_series(self, series_id, mcm_js, common_metas, sep='='):
		mythem_js = self.read_mythem_series_js(series_id)
		if mythem_js is None:
			return None

		mcm_js['mythematics-source'] = 'mythematics-collection'
		return self.merge_mcm_mythem(mythem_js, mcm_js, common_metas, sep=sep)

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
			mythem_js = self.read_mythem_series_js_from_fcode(fcode)
			if mythem_js is None:
				mcm_js['mythematics-source'] = 'mcm'
				return mcm_js
			else:
				mcm_js['mythematics-source'] = 'mythematics-collection'
		else:
			mcm_js['mythematics-source'] = 'mythematics'

		return self.merge_mcm_mythem(mythem_js, mcm_js, common_metas, sep=sep)
