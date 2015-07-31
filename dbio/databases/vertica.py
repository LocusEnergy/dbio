# Local modules
from base import Exportable, Importable


class Vertica(Exportable, Importable):

	CREATE_STAGING_CMD = "CREATE TABLE {staging} LIKE {table};"

	COPY_CMD = ("COPY {table} FROM STDIN "
				"DELIMITER '{delimiter}' ESCAPE AS '{escapechar}' "
				"RECORD TERMINATOR '{lineterminator}' NULL AS '{nullstring}' "
				"ENCLOSED BY '{quotechar}';")

	SWAP_CMD = ("ALTER TABLE {table}, {staging}, {temp} "
			 			 "RENAME TO {temp}, {table}, {staging};")

	DROP_CMD = "DROP TABLE {staging};"

	def __init__(self, url):
		Exportable.__init__(self, url)
		Importable.__init__(self, url)
		

	def execute_import(self, table, filename, csv_params, append, null_string=''):
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

			raw_cursor = connection.connection.cursor()
			with open(filename, 'r') as f:
				raw_cursor.copy(
					self.COPY_CMD.format(table=copy_table, nullstring=null_string, **csv_params), f)

			if not append:
				connection.execute(
					self.SWAP_CMD.format(table=table, staging=staging, temp=temp))

				connection.execute(self.DROP_CMD.format(staging=staging))


class VerticaODBC(Exportable, Importable):

	CREATE_STAGING_CMD = "CREATE TABLE {staging} LIKE {table};"

	COPY_CMD = ("COPY {table} FROM LOCAL {filename} "
				"DELIMITER '{delimiter}' ESCAPE AS '{escapechar}' "
				"RECORD TERMINATOR '{lineterminator}' NULL AS '{nullstring}' "
				"ENCLOSED BY '{quotechar}';")

	SWAP_AND_DROP_CMD = ("ALTER TABLE {table}, {staging}, {temp} "
			 			 "RENAME TO {temp}, {table}, {staging};"
			 			 "DROP TABLE {staging};")

	def __init__(self, url):
		Exportable.__init__(self, url)
		Importable.__init__(self, url)
		

	def execute_import(self, table, filename, csv_params, append):
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

			connection.execute(
					self.COPY_CMD.format(table=copy_table, filename=filename, 
										nullstring=null_string, **csv_params))

			if not append:
				connection.execute(
					self.SWAP_AND_DROP_CMD.format(table=table, staging=staging, temp=temp))