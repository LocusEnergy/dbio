# PyPI packages
import unicodecsv

# Local modules
from base import Exportable, Importable


class SQLite(Exportable, Importable):

	CREATE_STAGING_CMD = "CREATE TABLE {staging} AS SELECT * FROM {table} WHERE 0;"

	INSERT_CMD = "INSERT INTO {table} VALUES {values};";

	ANALYZE_CMD = "ANALYZE {table};"

	SWAP_AND_DROP_CMDS = ["ALTER TABLE {table} RENAME TO {temp};",
						 "ALTER TABLE {staging} RENAME TO {table};",
						 "ALTER TABLE {temp} RENAME TO {staging};",
						 "DROP TABLE {staging};"]

	INSERT_BATCH = 100

	def __init__(self, url):
		Exportable.__init__(self, url)
		Importable.__init__(self, url)


	def execute_import(self, table, filename, csv_params, append, analyze=False, null_string=''):
		staging = table + '_staging'
		temp = table + '_temp'
		if append:
			insert_table = table
		else:
			insert_table = staging

		eng = self.get_import_engine()
		
		# Start transaction
		with eng.begin() as connection:
			if not append:
				connection.execute(
					self.CREATE_STAGING_CMD.format(staging=staging, table=table))

			with open(filename, 'rb') as f:
				reader = unicodecsv.reader(f, **csv_params)
				rows_read = 0
				values = []
				for row in reader:
					rows_read += 1
					values.append('(\'' + '\',\''.join(row) + '\')')
					if (rows_read % self.INSERT_BATCH) == 0:
						connection.execute(self.INSERT_CMD.format(
											table=insert_table, values=','.join(values)))
						values = []
				if values:
					connection.execute(self.INSERT_CMD.format(
											table=insert_table, values=','.join(values)))

			if analyze:
				connection.execute(self.ANALYZE_CMD.format(table=insert_table))

			if not append:
				for cmd in self.SWAP_AND_DROP_CMDS:
					connection.execute(cmd.format(table=table, staging=staging, temp=temp))