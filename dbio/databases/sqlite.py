# PyPI packages
import unicodecsv

# Local modules
from base import Exportable, Importable


class SQLite(Exportable, Importable):

	SELECT_CREATE_CMD = ("SELECT sql FROM sqlite_master "
						  "WHERE type='table' AND name='{table}';")

	SELECT_INDICES_CMD = ("SELECT name, sql FROM sqlite_master "
						   "WHERE type='index' AND tbl_name='{table}';")

	DROP_INDEX_CMD = "DROP INDEX {index};"

	INSERT_CMD = "INSERT INTO {table} VALUES {values};";

	ANALYZE_CMD = "ANALYZE {table};"

	SWAP_CMDS = ["ALTER TABLE {table} RENAME TO {temp};",
						 "ALTER TABLE {staging} RENAME TO {table};",
						 "ALTER TABLE {temp} RENAME TO {staging};"]
	
	DROP_CMD = "DROP TABLE {staging};"

	TRUNCATE_CMD = "TRUNCATE TABLE {staging};"

	INSERT_BATCH = 100

	def __init__(self, url):
		Exportable.__init__(self, url)
		Importable.__init__(self, url)


	def execute_import(self, table, filename, append, csv_params, null_string, 
						analyze=False, disable_indices=False, create_staging=True,
						expected_rowcount=None):
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
				if create_staging:
					results = connection.execute(self.SELECT_CREATE_CMD.format(table=table))
					create_cmd = results.fetchone()[0]
					results.close()
					connection.execute(create_cmd.replace(table, staging))
				else:
					connection.execute(self.TRUNCATE_CMD.format(staging=staging))
				

			if disable_indices:
				# fetch index information from sqlite_master
				results = connection.execute(self.SELECT_INDICES_CMD.format(table=copy_table))
				index_names = []
				index_creates = []
				for row in results:
					index_names.append(row[0])
					index_creates.append(row[1])
				results.close()

				for index in index_names:
					connection.execute(self.DROP_INDEX_CMD.format(index=index))

			with open(filename, 'rb') as f:
				reader = unicodecsv.reader(f, **csv_params)
				rows_read = 0
				values = []
				for row in reader:
					rows_read += 1
					nulled_row = ['NULL' if field == null_string else field for field in row]
					values.append('(\'' + '\',\''.join(nulled_row) + '\')')
					if (rows_read % self.INSERT_BATCH) == 0:
						connection.execute(self.INSERT_CMD.format(
											table=insert_table, values=','.join(values)))
						values = []
				if values:
					connection.execute(self.INSERT_CMD.format(
											table=insert_table, values=','.join(values)))
					
		with eng.begin() as connection:
			if expected_rowcount is not None:
				self.do_rowcount_check(insert_table, expected_rowcount)

			if disable_indices:
				# create indices from 'sql'
				for index_create_cmd in index_creates:
					connection.execute(index_create_cmd)

			if analyze:
				connection.execute(self.ANALYZE_CMD.format(table=insert_table))

			if not append:
				for cmd in self.SWAP_CMDS:
					connection.execute(cmd.format(table=table, staging=staging, temp=temp))

				if create_staging:
					connection.execute(self.DROP_CMD.format(staging=staging))