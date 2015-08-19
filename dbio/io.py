# Python standard library
import tempfile
import logging
import os
import random
import time
import subprocess
import sys
import string

# PyPI packages
import unicodecsv
import sqlalchemy

# Local modules.
from databases import dialect_driver_class_map, DEFAULT_CSV_PARAMS, DEFAULT_NULL_STRING


# Setup module level logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
logging.getLogger('sqlalchemy').setLevel(logging.NOTSET)

FILE_WRITE_BATCH = 1000000

# Named pipe replication constants
PIPE_WRITE_BATCH = 100
MAX_WRITE_ATTEMPTS = 10
MAX_READ_ATTEMPTS = 10


def query(sqla_url, query, filename, query_is_file=False, 
			batch_size=FILE_WRITE_BATCH, csv_params=DEFAULT_CSV_PARAMS, 
			null_string=DEFAULT_NULL_STRING):
	""" Query a database and write the results to a csv file.

		:param sqla_url: SQLAlchemy engine creation URL for db.
		:param query: SQL query string to execute.
		:param filename: Name of csv file to dump to.
		:param query_is_file: If True, the query argument is a filename.
		:param batch_size: Number of rows to keep in memory before writing to filename.
		:param csv_params: Dictionary of csv parameters.
		:param null_string: String to represent null values with.
		:returns: The number of rows written to the file.

	"""

	logger.info("Querying to csv.")

	if query_is_file:
		query_str = __file_to_str(query)
	else:
		query_str = query

	db = __get_database(sqla_url)
	db_engine = db.get_export_engine()
	connection = db_engine.connect()

	# Stream results with given buffer size. Currently only used by pyscopg2.
	results = (connection.execution_options(stream_results=True, 
				max_row_buffer=batch_size)).execute(query_str)

	rows_written = 0
	with open(filename, 'wb') as f:
		csv_writer = unicodecsv.writer(f, **csv_params)
		rows = results.fetchmany(batch_size)
		while rows:
			if null_string == '':
				csv_writer.writerows(rows)
			else:
				csv_writer.writerows(
					[[null_string if field is None else field for field in row] for row in rows])
			rows_written += len(rows)
			rows = results.fetchmany(batch_size)

	results.close()

	logger.info("Query to csv completed. Rows written: {count}.".format(count=rows_written))
	return rows_written


def load(sqla_url, table, filename, append, disable_indices=False, analyze=False,
		 csv_params=DEFAULT_CSV_PARAMS, null_string=DEFAULT_NULL_STRING, 
		 create_staging=True, expected_rowcount=None):
	""" Import data from a csv file to a database table. 

		:param sqla_url: SQLAlchemy url string to pass to create_engine().
		:param table: Table in database to load data from filename.
		:param filename: Name of csv file to load from.
		:param append: If True, any data already in the table will be preserved.
		:param analyze: If True, the table will be will be analyzed for 
					query optimization immediately after importing.
		:param disable_indices: If True, table will temporarily disable or drop indices
								for the duration of the load in the attempt of speeding 
								up the operation.
		:param csv_params: Dictionary of csv parameters.
		:param null_string: String to represent null values with.
		:param create_staging: If True, the old table will be replaced with a new, identical table.
					If False, there must be an existing table named "table_staging".
		:param expected_rowcount: The number of rows that are expected to be in the loaded table.
					If the count does not much, the loading transaction will raise an error and rollback if possible.
					If the count is set to None, no check will be made. 

	"""

	logger.info("Importing from CSV.")

	db = __get_database(sqla_url)
	db.execute_import(table, filename, append, csv_params, null_string,
						analyze=analyze, disable_indices=disable_indices, 
						create_staging=create_staging, expected_rowcount=expected_rowcount)

	logger.info("Load from csv completed.")


def replicate(query_db_url, load_db_url, query, table, append, analyze=False,
			  disable_indices=False, query_is_file=False, create_staging=True,
			  do_rowcount_check=False):
	""" Load query results into a table using a named pipe to stream the data.

		This method works by simultaneously executing :py:func:`query` and 
		:py:func:`load` with a named pipe shared between the two processes.

		**Unix only.**

		:param query_db_url: SQLAlchemy engine creation URL for query_db.
		:param load_db_url: SQLAlchemy engine creation URL for load_db. 
		:param query: SQL query string to execute.
		:param table: Table in database to load data from filename.
		:param append: If True, any data already in the table will be preserved.
		:param analyze: If True, the table will be will be analyzed for 
					query optimization immediately after importing.
		:param disable_indices: If True, table will temporarily disable or drop indices
								for the duration of the load in the attempt of speeding 
								up the operation.
		:param query_is_file: If True, the query argument is a filename.
		:param create_staging: If True, the old table will be replaced with a new, identical table.
					If False, there must be an existing table named "table_staging".
		:param do_rowcount_check: If True, the replication will only succeed if the query rowcount
					matches the load rowcount.

		:raises ReaderError: Reader process did not execute successfully.
		:raises WriterError: Writer process did not execute successfully.
		
	"""
	logger.info("Beginning replication.")

	load_db = __get_database(load_db_url)
	csv_params = load_db.DEFAULT_CSV_PARAMS
	null_string = load_db.DEFAULT_NULL_STRING

	# Open a UNIX first-in-first-out file (a named pipe).
	pipe_name = 'pipe_' + ''.join(random.SystemRandom().choice(
		string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(10))
	os.mkfifo(pipe_name)
	try:
		# Args for 'dbio' command
		dbio_args = ['dbio']
		root_logger_level = logging.getLogger().level
		if root_logger_level <= logging.DEBUG:
			dbio_args.append('-v')
		elif root_logger_level >= logging.WARNING:
			dbio_args.append('-q')

		# Args for 'load' subcommand
		load_args = ['load', load_db_url, table, pipe_name]
		if append:
			load_args.append('--append')
		if analyze:
			load_args.append('--analyze')
		if not create_staging:
			load_args.append('--staging-exists')
		if disable_indices:
			load_args.append('--disable-indices')
		if do_rowcount_check:
			if query_is_file:
				rowcount = __get_database(query_db_url).get_query_rowcount(__file_to_str(query))
			else:
				rowcount = __get_database(query_db_url).get_query_rowcount(query)

			load_args.append('--expected-rowcount')
			load_args.append(str(rowcount))
			
		__append_csv_args(load_args, csv_params, null_string)
		reader_args = dbio_args + load_args

		# Args for 'query' subcommand
		query_args  = ['query', query_db_url, query, pipe_name, '--batchsize', str(PIPE_WRITE_BATCH)]
		if query_is_file:
			query_args.append('--file')
		__append_csv_args(query_args, csv_params, null_string)
		writer_args = dbio_args + query_args

		# To allow for virtualenvs:
		env = os.environ.copy()
		env['PATH'] += os.pathsep + os.pathsep.join(sys.path)

		logger.debug("Reader call: " + ' '.join(reader_args))
		reader_process = subprocess.Popen(reader_args, env=env)

		logger.debug("Writer call: " + ' '.join(writer_args))
		writer_process = subprocess.Popen(writer_args, env=env)

		try:
			while True:
				writer_process.poll()
				reader_process.poll()
				r_returncode = reader_process.returncode
				w_returncode = writer_process.returncode
				if w_returncode is None:
					if r_returncode is None:
						# Both processes are still running. Check again in one second.
						time.sleep(1)
					else:
						raise ReaderError("Reader finished before writer. Subprocess returncode: " + str(r_returncode))
				elif w_returncode != os.EX_OK:
					raise WriterError("Subprocess returncode: " + str(writer_process.returncode))
				else:
					if r_returncode is None:
						# Wait for reader to finish
						reader_process.communicate()
						r_returncode = reader_process.returncode
						if r_returncode != os.EX_OK:
							raise ReaderError("Subprocess returncode: " + str(r_returncode))
						break
					elif r_returncode != os.EX_OK:
						raise ReaderError("Subprocess returncode: " + str(r_returncode))
					else:
						break
		finally:
			# Ensure no processes are orphaned
			reader_process.poll()
			if reader_process.returncode is None:
					reader_process.kill()
			writer_process.poll()
			if writer_process.returncode is None:
					writer_process.kill()
	finally:
		os.remove(pipe_name)

	logger.info("Replication completed.")


def replicate_no_fifo(query_db_url, load_db_url, query, table, append, analyze=False,
					  disable_indices=False, query_is_file=False, create_staging=True,
					  do_rowcount_check=False):
	""" Identitcal to :py:func:`replicate`, but uses a tempfile and disk I/O instead of a
		named pipe. This method works on any platform and doesn't require the database
		to support loading from named pipes."""

	logger.info("Beginning replication.")

	load_db = __get_database(load_db_url)
	csv_params = load_db.DEFAULT_CSV_PARAMS
	null_string = load_db.DEFAULT_NULL_STRING

	temp_file = tempfile.NamedTemporaryFile()
	try:
		rowcount = query(query_db_url, query, temp_file.name, query_is_file=query_is_file, 
			  csv_params=csv_params, null_string=null_string)

		if not do_rowcount_check:
			rowcount = None

		load(load_db_url, table, temp_file.name, append, analyze=analyze, 
			 disable_indices=disable_indices, csv_params=csv_params, null_string=null_string,
			 create_staging=create_staging, expected_rowcount=rowcount)
	finally:
		temp_file.close()

	logger.info("Replication completed.")


def __file_to_str(fname):
	with open(fname, 'r') as f:
		return f.read()


def __append_csv_args(args, csv_params, null_string):
	args.append('-ns')
	args.append(null_string)
	args.append('-d')
	args.append(csv_params['delimiter'])
	if csv_params['escapechar']:
		args.append('-esc')
		args.append(csv_params['escapechar'])
	args.append('-l')
	args.append(csv_params['lineterminator'])
	args.append('-e')
	args.append(csv_params['encoding'])
	if csv_params['quoting'] == unicodecsv.QUOTE_ALL:
		args.append('-qc')
		args.append(csv_params['quotechar'])


def __get_database(url):
	sqla_url = sqlalchemy.engine.url.make_url(url)
	dialect = sqla_url.get_backend_name()
	driver = sqla_url.get_driver_name()
	try:
		db_class = dialect_driver_class_map[dialect][driver]
	except KeyError as e:
		raise UnsupportedDatabaseError(e.message + " is an unsupported dialect or driver.")
	return db_class(url)


class UnsupportedDatabaseError(Exception): pass
class WriterError(Exception): pass
class ReaderError(Exception): pass