# PyPI packages
import sqlalchemy


class Exportable():

	def __init__(self, url):
		"""
			:param url: sqlalchemy engine creation url.
			
		"""
		self.url = url
	

	def get_export_engine(self):
		""" :return: an sqlalchemy engine object. """
		return sqlalchemy.create_engine(self.url)


class Importable():

	def __init__(self, url):
		""" 
			:param url: sqlalchemy engine creation url.
			:param pre_load: ordered list of commands to execute before loading. 
			:param load: command string to load data from a file.
			:param post_load: ordered list of commands to execute after loading.

		"""
		self.url = url


	def get_import_engine(self):
		""" :return: an sqlalchemy engine object. """
		return sqlalchemy.create_engine(self.url)


	def execute_import(self, table, filename, csv_params, append, analyze=False, null_string=''):
		""" Database specific implementation of loading from a CSV

			:param table: destination for the load operation.
			:param data_source: Either a CSV file to be loaded or an INSERT command.
			:param csv_params: csv format info of the data_source.
			:param append: True if the data_source should add to table,
					False if table should only contain the contents of
					data_source after loading.
			:param analyze: If True, the table will be will be analyzed for 
					query optimization immediately after importing.
			:param null_string: String to replace NULL values with when importing.

		"""
		raise NotImplementedError()