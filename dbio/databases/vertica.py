# PyPI packages
import unicodecsv

# Local modules
from base import Exportable, Importable


class Vertica(Exportable, Importable):

	CREATE_STAGING_CMD = "CREATE TABLE {staging} LIKE {table};"

	COPY_CMD = ("COPY {table} FROM STDIN "
				"DELIMITER E'\{delimiter}' "
				"NULL AS '{nullstring}' "
				"ESCAPE AS '{escapechar}' "
				"RECORD TERMINATOR '{lineterminator}' "
				"ENFORCELENGTH ABORT ON ERROR")

	SWAP_CMD = ("ALTER TABLE {table}, {staging}, {temp} "
			 	"RENAME TO {temp}, {table}, {staging};")

	ANALYZE_CMD = "SELECT ANALYZE_STATISTICS('{table}');"

	DROP_CMD = "DROP TABLE IF EXISTS {staging};"

	TRUNCATE_CMD = "TRUNCATE TABLE {staging};"

	DEFAULT_CSV_PARAMS = {
						'delimiter' : ',', 
						'escapechar' : '\\',
						'lineterminator' : '\n',
						'encoding' : 'utf-8',
						'quoting' : unicodecsv.QUOTE_NONE
	}

	DEFAULT_NULL_STRING = 'NULL'


	def __init__(self, url):
		Exportable.__init__(self, url)
		Importable.__init__(self, url)
		

	def execute_import(self, table, filename, append, csv_params, null_string, 
						analyze=False, disable_indices=False, create_staging=True,
						expected_rowcount=None):
		""" Vertica has no indices, so disable_indices doesn't apply """
		
		staging = table + '_staging'
		temp = table + '_temp'
		if append:
			copy_table = table
		else:
			copy_table = staging

		eng = self.get_import_engine()
		
		# Start transaction
		with eng.begin() as connection:
			if not append:
				if create_staging:
					# Pre drop table in case it already exists
					connection.execute(self.DROP_CMD.format(staging=staging))
					connection.execute(
						self.CREATE_STAGING_CMD.format(staging=staging, table=table))
				else:
					connection.execute(self.TRUNCATE_CMD.format(staging=staging))

			raw_cursor = connection.connection.cursor()
			with open(filename, 'r') as f:
				raw_cursor.copy(
					self.COPY_CMD.format(table=copy_table, nullstring=null_string, **csv_params), f)
				raw_cursor.close()

		with eng.begin() as connection:
			if expected_rowcount is not None:
				self.do_rowcount_check(copy_table, expected_rowcount)

			if analyze:
				connection.execute(self.ANALYZE_CMD.format(table=copy_table))

			if not append:
				connection.execute(
					self.SWAP_CMD.format(table=table, staging=staging, temp=temp))

				if create_staging:
					connection.execute(self.DROP_CMD.format(staging=staging))


class VerticaODBC(Exportable, Importable):

	CREATE_STAGING_CMD = "CREATE TABLE {staging} LIKE {table};"

	COPY_CMD = ("COPY {table} FROM LOCAL {filename} "
				"DELIMITER E'\{delimiter}' "
				"NULL AS '{nullstring}' "
				"ESCAPE AS '{escapechar}' "
				"RECORD TERMINATOR '{lineterminator}' "
				"ENFORCELENGTH ABORT ON ERROR")

	ANALYZE_CMD = "SELECT ANALYZE_STATISTICS('{table}');"

	SWAP_CMD = ("ALTER TABLE {table}, {staging}, {temp} "
			 			 "RENAME TO {temp}, {table}, {staging};")

	DROP_CMD = "DROP TABLE {staging};"

	TRUNCATE_CMD = "TRUNCATE TABLE {staging};"

	DEFAULT_CSV_PARAMS = {
						'delimiter' : ',', 
						'escapechar' : '\\',
						'lineterminator' : '\n',
						'encoding' : 'utf-8',
						'quoting' : unicodecsv.QUOTE_NONE
	}

	DEFAULT_NULL_STRING = 'NULL'

	def __init__(self, url):
		Exportable.__init__(self, url)
		Importable.__init__(self, url)
		

	def execute_import(self, table, filename, append, csv_params, null_string, 
						analyze=False, disable_indices=False, create_staging=True,
						expected_rowcount=None):
		staging = table + '_staging'
		temp = table + '_temp'
		if append:
			copy_table = table
		else:
			copy_table = staging

		eng = self.get_import_engine()
		
		# Start transaction
		with eng.begin() as connection:
			if not append and create_staging:
				connection.execute(
					self.CREATE_STAGING_CMD.format(staging=staging, table=table))

			connection.execute(
					self.COPY_CMD.format(table=copy_table, filename=filename, 
										nullstring=null_string, **csv_params))

		with eng.begin() as connection:
			if expected_rowcount is not None:
				self.do_rowcount_check(copy_table, expected_rowcount)

			if analyze:
				connection.execute(self.ANALYZE_CMD.format(table=copy_table))

			if not append:
				if create_staging:
					connection.execute(self.DROP_CMD.format(staging=staging))
				else:
					connection.execute(self.TRUNCATE_CMD.format(staging=staging))