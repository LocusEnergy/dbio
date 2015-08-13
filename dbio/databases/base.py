# PyPI packages
import sqlalchemy
import unicodecsv

class Exportable():
	""" Designed to be the target of **query** operations. """

	def __init__(self, url):
		"""
			:param url: sqlalchemy engine creation url.
			
		"""
		self.url = url
	

	def get_export_engine(self):
		""" :return: sqlalchemy engine object. """
		return sqlalchemy.create_engine(self.url)


class Importable():
	""" Designed to be the target of **load** operations. """

	DEFAULT_CSV_PARAMS = {
						'delimiter' : ',', 
						'escapechar' : '\\',
						'lineterminator' : '\n',
						'encoding' : 'utf-8',
						'quoting' : unicodecsv.QUOTE_NONE
	}

	DEFAULT_NULL_STRING = 'NULL'

	def __init__(self, url):
		""" 
			:param url: sqlalchemy engine creation url.

		"""
		self.url = url


	def get_import_engine(self):
		""" :return: sqlalchemy engine object. """
		return sqlalchemy.create_engine(self.url)


	def execute_import(self, table, filename, append, csv_params, null_string, 
						analyze=False, disable_indices=False, create_staging=True):
		""" Database specific implementation of loading from a CSV

			:param table: destination for the load operation.
			:param data_source: Either a CSV file to be loaded or an INSERT command.
			:param csv_params: csv format info of the data_source.
			:param append: True if the data_source should add to table,
					False if table should only contain the contents of
					data_source after loading.
			:param analyze: If True, the table will be will be analyzed for 
					query optimization immediately after importing.
			:param disable_indices: If True, table will temporarily disable or drop indices
					in the attempts of speeding up the load. 
			:param null_string: String to replace NULL values with when importing.
			:param create_staging: If True, the old table will be replaced with a new, identical table.
					If False, there must be an existing table named "table_staging".

		"""
		raise NotImplementedError()