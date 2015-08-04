# PyPI packages
import MySQLdb.cursors
import sqlalchemy

# Local modules
from base import Exportable, Importable


class MySQL(Exportable, Importable):

	NET_READ_TIMEOUT = 3600

	SET_NET_READ_TIMEOUT = "SET SESSION net_read_timeout=" + str(NET_READ_TIMEOUT)

	SET_TRANS_ISO_LVL = "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;"

	CREATE_STAGING_CMD = "CREATE TABLE {staging} LIKE {table};"

	LOAD_CMD = ("LOAD DATA LOCAL INFILE '{filename}' INTO TABLE {table} "
				"FIELDS TERMINATED BY '\{delimiter}' ENCLOSED BY '\{quotechar}' "
				"ESCAPED BY '\{escapechar}' LINES TERMINATED BY '\{lineterminator}';")

	ANALYZE_CMD = ("ANALYZE TABLE {table};")

	SWAP_AND_DROP_CMD = ("RENAME TABLE {table} TO {temp}, {staging} TO {table}, "
		 				 "{temp} TO {staging};"
		 				 "DROP TABLE {staging};")

	def __init__(self, url):
		self.url = url
		

	def get_export_engine(self):
		# SSCursor keeps the results on the server until a row is explicitly fetched
		# by the client's cursor.
		return sqlalchemy.create_engine(self.url, 
				connect_args={'cursorclass' : MySQLdb.cursors.SSCursor})


	def get_import_engine(self):
		# LOAD DATA LOCAL INFILE fails without the local_infile=1 arg.
		return sqlalchemy.create_engine(self.url, connect_args={'local_infile' : 1})


	def execute_import(self, table, filename, csv_params, append, analyze=False, null_string=''):
		staging = table + '_staging'
		temp = table + '_temp'
		if append:
			load_table = table
		else:
			load_table = staging

		eng = self.get_import_engine()
		
		# Start transaction
		with eng.begin() as connection:
			connection.execute(self.SET_NET_READ_TIMEOUT)
			connection.execute(self.SET_TRANS_ISO_LVL)

			if not append:
				connection.execute(
					self.CREATE_STAGING_CMD.format(staging=staging, table=table))

			connection.execute(
					self.LOAD_CMD.format(table=load_table, filename=filename, **csv_params))

			if analyze:
				connection.execute(self.ANALYZE_CMD.format(table=load_table))

			if not append:
				connection.execute(
					self.SWAP_AND_DROP_CMD.format(table=table, staging=staging, temp=temp))