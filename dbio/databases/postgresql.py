# Local modules
from base import Exportable, Importable





class PostgreSQL(Exportable, Importable):

	COPY_CMD = ("COPY {table} FROM STDIN CSV "
				"DELIMITER '{delimiter}' ESCAPE '{escapechar}' "
				"NULL '{nullstring}' QUOTE '{quotechar}'; ")

	CREATE_STAGING_CMD = "CREATE TABLE {staging} (LIKE {table});"

	ANALYZE_CMD = "ANALYZE {table};"

	SWAP_AND_DROP_CMD = ("ALTER TABLE {table} RENAME TO {temp};"
						 "ALTER TABLE {staging} RENAME TO {table};"
						 "ALTER TABLE {temp} RENAME TO {staging};"
						 "DROP TABLE {staging};")

	def __init__(self, url):
		Exportable.__init__(self, url)
		Importable.__init__(self, url)


	def execute_import(self, table, filename, csv_params, append, analyze=False, null_string=''):
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
				connection.execute(
					self.CREATE_STAGING_CMD.format(staging=staging, table=table))

			# get psycopg2 cursor object to access copy_expert()
			raw_cursor = connection.connection.cursor()
			with open(filename, 'r') as f:
				raw_cursor.copy_expert(
					self.COPY_CMD.format(table=copy_table, nullstring=null_string, 
											**csv_params), f)
				raw_cursor.close()

			if analyze:
				connection.execute(self.ANALYZE_CMD.format(table=copy_table))

			if not append:
				connection.execute(
					self.SWAP_AND_DROP_CMD.format(table=table, staging=staging, temp=temp))