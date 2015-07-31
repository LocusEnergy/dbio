# Python standard library
import tempfile
import logging
import os
import time
import subprocess
import sys

# PyPI packages
import unicodecsv
import sqlalchemy

# Local modules.
from databases import dialect_driver_class_map

# Setup module level logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())
logging.getLogger('sqlalchemy').setLevel(logging.NOTSET)

CSV_PARAMS_DEFAULT = {  'delimiter' : ',', 
						'escapechar' : '\\',
						'lineterminator' : '\r\n',
						'encoding' : 'utf-8',
						'doublequote' : False,
						'quotechar' : '"',
						'quoting' : unicodecsv.QUOTE_ALL}

NULL_STRING_DEFAULT = ''

FILE_WRITE_BATCH = 1000000

# Named pipe replication constants
PIPE_WRITE_BATCH = 100
MAX_WRITE_ATTEMPTS = 10
MAX_READ_ATTEMPTS = 10
PIPE_NAME = 'replication_fifo'


def query(sqla_url, query, filename, query_is_file=False, 
			batch_size=FILE_WRITE_BATCH, csv_params=None, 
			null_string=NULL_STRING_DEFAULT):
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

	if csv_params is None:
		csv_params = CSV_PARAMS_DEFAULT

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

	logger.info("Query to csv completed.")
	return rows_written


def load(sqla_url, table, filename, append, csv_params=None, 
			null_string=NULL_STRING_DEFAULT):
	""" Import data from a csv file to a database table. 

		:param sqla_url: SQLAlchemy url string to pass to create_engine().
		:param table: Table in database to load data from filename.
		:param filename: Name of csv file to load from.
		:param append: If True, any data already in the table will be preserved.
		:param csv_params: Dictionary of csv parameters.
		:param null_string: String to represent null values with.

	"""

	logger.info("Importing from CSV.")

	if csv_params is None:
		csv_params = CSV_PARAMS_DEFAULT

	db = __get_database(sqla_url)
	db.execute_import(table, filename, csv_params, append)

	logger.info("Load from csv completed.")


def replicate(query_db_url, load_db_url, query, table, append, query_is_file=False, 
				csv_params=None, null_string=NULL_STRING_DEFAULT):
	""" Load query results into a table using a named pipe to stream the data.

		This method works by simultaneously executing :py:func:`query` and 
		:py:func:`load` with a named pipe shared between the two processes.

		**Unix only.**

		:param query_db_url: SQLAlchemy engine creation URL for query_db.
		:param load_db_url: SQLAlchemy engine creation URL for load_db. 
		:param query: SQL query string to execute.
		:param table: Table in database to load data from filename.
		:param append: If True, any data already in the table will be preserved.
		:param query_is_file: If True, the query argument is a filename.
		:param csv_params: Dictionary of csv parameters.
		:param null_string: String to represent null values with.

		:raises ReaderError: Reader process did not execute successfully.
		:raises WriterError: Writer process did not execute successfully.
		
	"""
	logger.info("Beginning replication.")

	# Open a UNIX first-in-first-out file (a named pipe).
	os.mkfifo(PIPE_NAME)
	try:
		# Subprocess commands setup
		if csv_params is None:
			csv_params = CSV_PARAMS_DEFAULT

		dbio_args = __get_dbio_args(csv_params, null_string)

		load_args = dbio_args + ['load', load_db_url, table, PIPE_NAME]
		if append:
			load_args.append('--append')

		query_args = dbio_args + ['query', query_db_url, query, PIPE_NAME, 
								  '--batchsize', str(PIPE_WRITE_BATCH)]

		if query_is_file:
			query_args.append('--file')

		# To allow for virtualenvs:
		env = os.environ.copy()
		env['PATH'] += os.pathsep + os.pathsep.join(sys.path)

		logger.debug("Reader call: " + ' '.join(load_args))
		reader_process = subprocess.Popen(load_args, env=env)

		logger.debug("Writer call: " + ' '.join(query_args))
		writer_process = subprocess.Popen(query_args, env=env)

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
		os.remove(PIPE_NAME)

	logger.info("Replication completed.")


def replicate_no_fifo(query_db_url, load_db_url, _query, table, append, query_is_file=False):
	""" Identitcal to :py:func:`replicate`, but uses a tempfile and disk I/O instead of a
		named pipe. This method works on any platform and doesn't require the database
		to support loading from named pipes."""

	logger.info("Beginning replication.")


	temp_file = tempfile.NamedTemporaryFile()
	try:
		query(query_db_url, _query, temp_file.name, query_is_file=query_is_file)
		load(load_db_url, table, temp_file.name, append)
	finally:
		temp_file.close()

	logger.info("Replication completed.")


def __file_to_str(fname):
	with open(fname, 'r') as f:
		return f.read()


def __get_dbio_args(csv_params, null_string):
	dbio_args = ['dbio']

	dbio_args.append('-ns')
	dbio_args.append(null_string)
	dbio_args.append('-d')
	dbio_args.append(csv_params['delimiter'])
	dbio_args.append('-esc')
	dbio_args.append(csv_params['escapechar'])
	dbio_args.append('-l')
	dbio_args.append(csv_params['lineterminator'])
	dbio_args.append('-e')
	dbio_args.append(csv_params['encoding'])

	root_logger_level = logging.getLogger().level
	if root_logger_level <= logging.DEBUG:
		dbio_args.append('-v')
	elif root_logger_level >= logging.WARNING:
		dbio_args.append('-q')

	return dbio_args

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