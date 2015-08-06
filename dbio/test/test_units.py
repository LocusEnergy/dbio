# Python standard library
import random
import tempfile
import filecmp
import subprocess
import string
import sqlite3

# PyPI packages
import pytest
import unicodecsv
import sqlalchemy

# Local modules
import dbio
import dbio.databases



#############
### Tests ###
#############

""" Run using py.test or python setup.py test """

def test_query(monkeypatch):
	""" Test that query properly fetches all rows and properly formats them into a CSV file. """
	# Mocking
	mock_url = 'mock_url'
	mock_db = MockDatabase(mock_url)

	def mockdb(url):
		return mock_db

	monkeypatch.setattr(dbio.io, '__get_database', mockdb)

	mock_query = 'mock_query'
	mock_results = get_rows(10, 10, 10, get_unicode_alphabet(1000, 2000), True)
	mock_db.engine.connection.results.rows = mock_results
	test_file = tempfile.NamedTemporaryFile()

	# Tested method
	dbio.query(mock_url, mock_query, test_file.name)

	# Check that the query has been executed and results fetched
	assert mock_db.engine.connection.results.all_rows_fetched
	assert mock_db.engine.connection.executed_commands == [mock_query]
	assert mock_db.engine.connection.results.closed

	# Check that query produces the file that we expect
	check_file = tempfile.NamedTemporaryFile()
	write_rows_to_file(mock_results, check_file.name, dbio.databases.DEFAULT_CSV_PARAMS)
	assert filecmp.cmp(test_file.name, check_file.name, shallow=False)

	# Cleanup
	test_file.close()
	check_file.close()


def test_load(monkeypatch):
	""" Test that load executes the commands that we expect it to. """
	# Mocking
	mock_url = 'mock_url'
	mock_db = MockDatabase(mock_url)

	def mockdb(url):
		return mock_db

	monkeypatch.setattr(dbio.io, '__get_database', mockdb)

	mock_table = 'mock_table'
	mock_fname = 'mock_fname'
	mock_append = True
	mock_csv_params = {}
	mock_null_string = 'mock_null_string'
	mock_analyze = True
	

	check_execute_import_args = [mock_table, mock_fname, mock_append, mock_csv_params, 
								 mock_null_string, mock_analyze]
	mock_db.cmds = ['mock_cmd1', 'mock_cmd2', 'mock_cmd3']

	# Tested method
	dbio.load(mock_url, mock_table, mock_fname, True, csv_params=mock_csv_params, 
			  null_string=mock_null_string, analyze=mock_analyze)

	# Check commands are passed to execute_import correctly
	assert check_execute_import_args == mock_db.execute_import_args


def test_replicate(monkeypatch):
	""" Test that reader and writer processes are opened and allowed to finish without error. """
	# Mocking
	mock_url = 'mock_url'
	mock_query = 'mock_query'
	mock_table = 'mock_table'
	mock_append = True

	mock_reader = MockPopen()
	mock_writer = MockPopen()

	mock_db = MockDatabase(mock_url)

	def mockdb(url):
		return mock_db

	monkeypatch.setattr(dbio.io, '__get_database', mockdb)

	def mockpopen(args, **kwargs):
		# Check that Popen is called correcty.
		assert args[0] == 'dbio'
		if 'load' in args:
			return mock_reader
		elif 'query' in args:
			return mock_writer
		else:
			assert False, "Unexpected dbio script called." + str(args)


	monkeypatch.setattr(subprocess, 'Popen', mockpopen)

	# Tested method
	dbio.replicate(mock_url, mock_url, mock_query, mock_table, mock_append)

	# Both processes have terminated with success
	assert mock_reader.returncode == 0
	assert mock_writer.returncode == 0


def test_replicate_with_failing_reader(monkeypatch):
	""" Test that replicate fails nicely when the reader process fails. """
	# Mocking
	mock_url = 'mock_url'
	mock_query = 'mock_query'
	mock_table = 'mock_table'
	mock_append = True

	mock_reader = MockPopen()
	mock_reader.returncode = -1
	mock_writer = MockPopen()

	mock_db = MockDatabase(mock_url)

	def mockdb(url):
		return mock_db

	monkeypatch.setattr(dbio.io, '__get_database', mockdb)

	def mockpopen(args, **kwargs):
		# Check that Popen is called correcty.
		assert args[0] == 'dbio'
		if 'load' in args:
			return mock_reader
		elif 'query' in args:
			return mock_writer
		else:
			assert False, "Unexpected dbio script called." + str(args)


	monkeypatch.setattr(subprocess, 'Popen', mockpopen)

	# Tested method
	try:
		dbio.replicate(mock_url, mock_url, mock_query, mock_table, mock_append)
	except dbio.io.ReaderError:
		pass
	except Exception as e:
		assert False, "Unexcpted error thrown by replicate: " + e.message
	else:
		assert False, "Reader failed but replicate did not."

	# Reader should show failure
	assert mock_reader.returncode != 0
	# Writer should not be alive
	assert mock_writer.returncode is not None


def test_replicate_with_failing_writer(monkeypatch):
	""" Test that replicate fails nicely when the writer process fails. """
	# Mocking
	mock_url = 'mock_url'
	mock_query = 'mock_query'
	mock_table = 'mock_table'
	mock_append = True

	mock_reader = MockPopen()
	mock_writer = MockPopen()
	mock_writer.returncode = -1

	mock_db = MockDatabase(mock_url)

	def mockdb(url):
		return mock_db

	monkeypatch.setattr(dbio.io, '__get_database', mockdb)

	def mockpopen(args, **kwargs):
		# Check that Popen is called correcty.
		assert args[0] == 'dbio'
		if 'load' in args:
			return mock_reader
		elif 'query' in args:
			return mock_writer
		else:
			assert False, "Unexpected dbio script called." + str(args)


	monkeypatch.setattr(subprocess, 'Popen', mockpopen)

	# Tested method
	try:
		dbio.replicate(mock_url, mock_url, mock_query, mock_table, mock_append)
	except dbio.io.WriterError:
		pass
	except:
		assert False, "Unexcpted error thrown by replicate"
	else:
		assert False, "Writer failed but replicate did not."

	# Reader should not be alive
	assert mock_reader.returncode is not None
	# Writer should show failure
	assert mock_writer.returncode != 0


def test_replicate_with_failing_rw(monkeypatch):
	""" Test that replicate fails nicely when both processes fail. """
	# Mocking
	mock_url = 'mock_url'
	mock_query = 'mock_query'
	mock_table = 'mock_table'
	mock_append = True

	mock_reader = MockPopen()
	mock_reader.returncode = -1
	mock_writer = MockPopen()
	mock_writer.returncode = -1

	mock_db = MockDatabase(mock_url)

	def mockdb(url):
		return mock_db

	monkeypatch.setattr(dbio.io, '__get_database', mockdb)

	def mockpopen(args, **kwargs):
		# Check that Popen is called correcty.
		assert args[0] == 'dbio'
		if 'load' in args:
			return mock_reader
		elif 'query' in args:
			return mock_writer
		else:
			assert False, "Unexpected dbio script called." + str(args)


	monkeypatch.setattr(subprocess, 'Popen', mockpopen)

	# Tested method
	try:
		dbio.replicate(mock_url, mock_url, mock_query, mock_table, mock_append)
	except dbio.io.ReaderError:
		pass
	except dbio.io.WriterError:
		pass
	except:
		assert False, "Unexcpted error thrown by replicate"
	else:
		assert False, "Writer and reader failed but replicate did not."

	assert mock_reader.returncode != 0
	assert mock_writer.returncode != 0


def test_replicate_no_fifo(monkeypatch):
	""" Because we have already tested query and load, this function only tests that query and load are called, and called correctly """
	mock_url = 'mock_url'
	mock_query = 'mock_query'
	mock_table = 'mock_table'
	mock_filename = 'mock_filename'
	mock_append = True
	mock_query_is_file = True
	mock_analyze = True

	query_called_with = {}
	load_called_with = {}

	mock_db = MockDatabase(mock_url)

	def mockdb(url):
		return mock_db

	monkeypatch.setattr(dbio.io, '__get_database', mockdb)

	def mock_query(*args, **kwargs):
		query_called_with['args'] = args
		query_called_with['kwargs'] = kwargs

	def mock_load(*args, **kwargs):
		load_called_with['args'] = args
		load_called_with['kwargs'] = kwargs

	monkeypatch.setattr(dbio.io, 'load', mock_load)
	monkeypatch.setattr(dbio.io, 'query', mock_query)

	dbio.replicate_no_fifo(mock_url, mock_url, mock_query, mock_table, 
						mock_append, query_is_file=mock_query_is_file, 
						analyze=mock_analyze)

	fname = query_called_with['args'][2]

	correct_query_args = (mock_url, mock_query, fname)
	correct_query_kwargs = {'query_is_file' : mock_query_is_file, 'csv_params' : dbio.databases.DEFAULT_CSV_PARAMS,
							'null_string' : dbio.databases.DEFAULT_NULL_STRING}
	correct_load_args = (mock_url, mock_table, fname, mock_append)
	correct_load_kwargs = {'analyze' : mock_analyze, 'csv_params' : dbio.databases.DEFAULT_CSV_PARAMS,
							'null_string' : dbio.databases.DEFAULT_NULL_STRING}

	assert load_called_with['args'] == correct_load_args
	assert load_called_with['kwargs'] == correct_load_kwargs
	assert query_called_with['args'] == correct_query_args
	assert query_called_with['kwargs'] == correct_query_kwargs


def test_sqlite():
	""" Creates two sqlite databases in tempfiles. Fake data is created,
		loaded into one database, replicated to the other, queried to a CSV,
		and compared with the original data to ensure integrity across the entire process. """
		
	# Data constants
	num_rows = 250
	num_fields = 5
	max_field_length = 20
	alphabet = string.digits

	# SQLite setup
	query_db_file = tempfile.NamedTemporaryFile()
	query_db_url = 'sqlite:///' + query_db_file.name
	query_table = 'query_table'
	create_sqlite_table(num_fields, max_field_length, query_table, query_db_url)
	
	import_db_file = tempfile.NamedTemporaryFile()
	import_db_url =  'sqlite:///' + import_db_file.name
	import_table = 'import_table'
	create_sqlite_table(num_fields, max_field_length, import_table, import_db_url)

	data_file = tempfile.NamedTemporaryFile()
	
	
	row_data = get_rows(num_rows, num_fields, max_field_length, alphabet, True)
	write_rows_to_file(row_data, data_file.name, dbio.databases.DEFAULT_CSV_PARAMS)

	dbio.load(query_db_url, query_table, data_file.name, False)

	dbio.replicate(query_db_url, import_db_url, 'SELECT * FROM ' + query_table, 
					import_table, False, analyze=True)

	check_file = tempfile.NamedTemporaryFile()
	dbio.query(import_db_url, 'SELECT * FROM ' + import_table, check_file.name)
	
	assert filecmp.cmp(data_file.name, check_file.name, shallow=False)

	# Clean up
	query_db_file.close()
	import_db_file.close()
	check_file.close()
	data_file.close()


####################
### Mock Classes ###
####################

class MockResults():
	""" Mocks a ResultProxy object. """

	def __init__(self):
		self.rows = []
		self.rows_fetched = 0
		self.all_rows_fetched = False
		self.closed = False


	def fetchmany(self, rows_to_fetch):
		assert not self.all_rows_fetched

		new_rows_fetched = self.rows_fetched + rows_to_fetch
		results = self.rows[self.rows_fetched:new_rows_fetched]
		self.rows_fetched = new_rows_fetched

		if not results:
			self.all_rows_fetched = True

		return results


	def close(self):
		self.closed = True


class MockConnection():
	""" Mocks a database Connection object """

	def __init__(self):
		self.results = MockResults()
		self.executed_commands = []
		self.exec_options = {}

	def execute(self, cmd):
		self.executed_commands.append(cmd)
		return self.results


	def execution_options(self, **kwargs):
		self.exec_options.update(kwargs)
		return self

class MockTransaction():
	""" Mocks a database Transaction object. The context managment methods are required. """

	def __init__(self):
		self.connection = MockConnection()


	def __enter__(self):
		return self.connection


	def __exit__(self, type, value, traceback):
		assert type is None


class MockEngine():
	""" Mocks a SQLAlchemy engine object. """

	def __init__(self):
		self.connection = MockConnection()
		self.transaction = MockTransaction()


	def connect(self):
		return self.connection


	def execute(self, cmd):
		return self.connection.execute(cmd)


	def begin(self):
		return self.transaction


class MockDatabase():
	""" Mocks both an Importable and an Exportable object. """

	DEFAULT_CSV_PARAMS = dbio.databases.DEFAULT_CSV_PARAMS

	DEFAULT_NULL_STRING = dbio.databases.DEFAULT_NULL_STRING

	def __init__(self, url):
		self.url = url
		self.engine = MockEngine()
		self.execute_import_args = []
		self.cmds = []


	def get_export_engine(self):
		return self.engine


	def get_import_engine(self):
		return self.engine


	def execute_import(self, table, data_file, append, csv_params, null_string, analyze=False):
		self.execute_import_args = [table, data_file, append, csv_params, null_string, analyze]


class MockPopen():
	""" Mocks subprocess Popen objects. """

	def __init__(self):
		self.returncode = None


	def poll(self):
		if self.returncode is None:
			self.returncode = 0


	def kill(self):
		if self.returncode is not None:
			assert False, "Attempted to kill a dead process."
		self.returncode = -1


	def communicate(self):
		if self.returncode is None:
			self.returncode = 0


########################
### Helper Functions ###
########################

def create_sqlite_table(num_fields, max_field_length, table, url):
	engine = sqlalchemy.create_engine(url)
	fields_list = ['field{i} varchar({length})'.format(i=i, length=max_field_length) for i in xrange(num_fields)]
	create_table_cmd = "CREATE TABLE {table} ({fields})".format(table=table, fields=','.join(fields_list))
	engine.execute(create_table_cmd)


def get_unicode_alphabet(minchar, maxchar):
	""" Return a string containing all unicode characters from minchar to maxchar """
	return ''.join([unichr(i) for i in range(minchar, maxchar)])


def get_rows(num_rows, num_fields, max_field_length, alphabet_str, deterministic):
	""" Creates rows of data (strings) to model SQL rows. 
		alphabet_str: The string to select characters from.
		deterministic: If True, the rows will cycle through the alphabet in a reproducable pattern. If False, the rows will be chosen at random """
	rows = []
	for row_num in range(num_rows):
		row = []
		for field_num in range(num_fields):
			if not deterministic:
				row.append(''.join(random.choice(alphabet_str) 
							for n in range(random.randrange(max_field_length))))
			else:
				row.append(''.join(alphabet_str[n % len(alphabet_str)]
							for n in range((row_num + field_num), (row_num + field_num + max_field_length))))
		rows.append(tuple(row))
	return rows	


def write_rows_to_file(rows, file_name, csv_params):
	""" Writes rows of strings to a CSV file using the given parameters. """
	with open(file_name, 'ab') as csvfile:
		csvwriter = unicodecsv.writer(csvfile, **csv_params)
		csvwriter.writerows(rows)
