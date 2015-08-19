# stdlib
import logging

# PyPI packages
import sqlalchemy
import unicodecsv

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class Exportable():
	""" Designed to be the target of **query** operations. """

	SELECT_COUNT_CMD = "SELECT COUNT(*) FROM ({query}) AS query_count;"

	def __init__(self, url):
		"""
			:param url: sqlalchemy engine creation url.
			
		"""
		self.url = url
	

	def get_export_engine(self):
		""" :returns: sqlalchemy engine object. """
		return sqlalchemy.create_engine(self.url)


	def get_query_rowcount(self, query):
		""" Gets a row count for the given query.

			:param query: The query to get the row count for.

			:returns: The row count for the provided query.

		"""
		engine = self.get_export_engine()
		results = engine.execute(self.SELECT_COUNT_CMD.format(query=query))
		rowcount = results.fetchall()[0][0]
		results.close()
		logger.info("Query row count: {count}.".format(count=rowcount))
		return rowcount


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

	ROWCOUNT_QUERY = "SELECT COUNT(*) FROM {table};"

	def __init__(self, url):
		""" 
			:param url: sqlalchemy engine creation url.

		"""
		self.url = url


	def get_import_engine(self):
		""" :return: sqlalchemy engine object. """
		return sqlalchemy.create_engine(self.url)


	def do_rowcount_check(self, table, expected_rowcount):
		""" Checks if the given table has the expected row count.

			:param table: The table to check for row count.
			:param expected_rowcount: Number of rows to expect in the table.

			:raises: UnexpectedRowcountError upon row count mismatch

		"""
		engine = self.get_import_engine()
		results = engine.execute(self.ROWCOUNT_QUERY.format(table=table))
		rowcount = results.fetchall()[0][0]
		results.close()
		logger.info("Row count of {table}: {count}.".format(table=table, count=rowcount))
		if rowcount != expected_rowcount:
			raise self.UnexpectedRowcountError("Expected {expected} rows in {table}, found {actual}.".format(
											expected=expected_rowcount, table=table, actual=rowcount))


	def execute_import(self, table, filename, append, csv_params, null_string, 
						analyze=False, disable_indices=False, create_staging=True,
						expected_rowcount=None):
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
			:param expected_rowcount: The number of rows that are expected to be in the loaded table.
					If the count does not much, the loading transaction will raise an error and rollback if possible.
					If the count is set to None, no check will be made. 

		"""
		raise NotImplementedError()


	class UnexpectedRowcountError(Exception): pass