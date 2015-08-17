# Python standard library
import argparse
import logging
import unicodecsv

# Local modules
from databases import DEFAULT_CSV_PARAMS, DEFAULT_NULL_STRING
import io


def load(args):
	csv_params = __get_csv_params(args)
	io.load(args.db_url, args.table, args.filename, args.append, analyze=args.analyze,
			disable_indices=args.disable_indices, csv_params=csv_params,  
			null_string=args.null_string, create_staging=args.create_staging, 
			expected_rowcount=args.expected_rowcount)


def query(args):
	csv_params = __get_csv_params(args)
	io.query(args.db_url, args.query, args.filename, query_is_file=args.from_file, 
				batch_size=args.batch_size, csv_params=csv_params, null_string=args.null_string)


def replicate(args):
	if args.fifo:
		io.replicate(args.query_db_url, args.load_db_url, args.query, args.table, 
					 args.append, analyze=args.analyze, disable_indices=args.disable_indices,
					 query_is_file=args.from_file, create_staging=args.create_staging,
					 do_rowcount_check=args.rowcount_check)
	else:
		io.replicate_no_fifo(args.query_db_url, args.load_db_url, args.query, args.table, 
							 args.append, analyze=args.analyze, 
							 disable_indices=args.disable_indices,
							 query_is_file=args.from_file, create_staging=args.create_staging,
							 do_rowcount_check=args.rowcount_check)


def main():
	# Top level parser: 'dbio'
	parser = argparse.ArgumentParser(prog='dbio', description=("A simple Python module"
			 						"for the following database operations: importing from CSV, "
			 						"querying to CSV, or querying to a table in a database."))
	parser.add_argument('-v', '--verbose', action='store_true')
	parser.add_argument('-q', '--quiet', action='store_true')	

	# Subparsers: 'query','load','replicate'
	subparsers = parser.add_subparsers(title='Operation', description="I/O operation.")
	__setup_replicate_parser(subparsers)
	__setup_query_parser(subparsers)
	__setup_load_parser(subparsers)

	# Handle all arg parsing.
	args = parser.parse_args()

	# Setup logging.
	if args.verbose:
		logging.basicConfig(level=logging.DEBUG)
	elif args.quiet:
		logging.basicConfig(level=logging.WARNING)
	else:
		logging.basicConfig(level=logging.INFO)
	 
	# Call the specified script.
	args.func(args)


def __setup_replicate_parser(subparsers):
	replicate_parser = subparsers.add_parser('replicate', description=("Query any database and load " 
											 "the results into a predefined table in any database."))
	
	replicate_parser.add_argument('query_db_url', help="SQLAlchemy engine creation URL for query_db.")
	replicate_parser.add_argument('load_db_url', help="SQLAlchemy engine creation URL for load_db.")
	replicate_parser.add_argument('query', help=("SQL query string to run against query_db. If -f is " 
											"included, this argument is assumed to be a file name."))
	replicate_parser.add_argument('table', help="Table in 'load_db' that will be filled.")

	replicate_parser.add_argument('-f', '--file', dest='from_file', action='store_true', 
									help="This flag indicates that 'query' is a file name")
	replicate_parser.add_argument('-a', '--append', dest='append', action='store_true', 
									help=("If this flag is included, any data already in table "
											"will be preserved."))
	replicate_parser.add_argument('-z', '--analyze', dest='analyze', action='store_true',
									help=("If this flag is included, a table analysis for query "
										"optimization will be executed immediately after loading."))
	replicate_parser.add_argument('-i', '--disable-indices', dest='disable_indices', action='store_true',
								help=("If this flag is included, any table indices will be dropped "
										"before loading and recreated after."))
	replicate_parser.add_argument('-nf', '--no-fifo', dest='fifo', action='store_false', 
									help="Include to avoid using mkfifo(), a Unix-only operation.")
	replicate_parser.add_argument('-s', '--staging-exists', dest='create_staging', action='store_false',
									help="Include if a table named table_staging already exists.")
	replicate_parser.add_argument('-rc', '--rowcount-check', dest='rowcount_check', action='store_true',
									help="Only succeed if the load table rowcount matches the query rowcount.")
	replicate_parser.set_defaults(func=replicate)


def __setup_query_parser(subparsers):
	query_parser = subparsers.add_parser('query', description=("Query a table and put the results into a csv file."))
	
	query_parser.add_argument('db_url', help="SQLAlchemy engine creation URL for db.")
	query_parser.add_argument('query', help=("SQL query string to run against query_db. If -f is " 
											"included, this argument is assumed to be a file name."))
	query_parser.add_argument('filename', help="File containg data to load into table.")
	query_parser.add_argument('-f', '--file', dest='from_file', action='store_true', 
									help="This flag indicates that 'query' is a file name")
	query_parser.add_argument('-b', '--batchsize', type=int, dest='batch_size', default=io.FILE_WRITE_BATCH)
	
	# CSV ARGS
	query_parser.add_argument('-qc', '--quotechar', default=None, help='Character to enclose fields. If not included, fields are not enclosed.')
	query_parser.add_argument('-ns', '--null-string', default=DEFAULT_NULL_STRING, help='String to replace NULL fields.')
	query_parser.add_argument('-d', '--delimiter', default=DEFAULT_CSV_PARAMS['delimiter'], help='Field separation character.')
	query_parser.add_argument('-esc', '--escapechar', default=DEFAULT_CSV_PARAMS['escapechar'], help='Escape character.')
	query_parser.add_argument('-l', '--lineterminator', default=DEFAULT_CSV_PARAMS['lineterminator'], help='Record terminator.')
	query_parser.add_argument('-e', '--encoding', default=DEFAULT_CSV_PARAMS['encoding'], help='Character encoding.')
	
	query_parser.set_defaults(func=query)


def __setup_load_parser(subparsers):
	load_parser = subparsers.add_parser('load', description=("Load data into a table from a file."))
	
	load_parser.add_argument('db_url', help="SQLAlchemy engine creation URL for db.")
	load_parser.add_argument('table', help="Table in 'db' that will be filled.")
	load_parser.add_argument('filename', help="File containg data to load into table.")
	load_parser.add_argument('-a', '--append', dest='append', action='store_true', 
							 help=("If this flag is included, any data already in table "
								   "will be preserved."))
	load_parser.add_argument('-z', '--analyze', dest='analyze', action='store_true',
								help=("If this flag is included, a table analysis for query "
										"optimization will be executed immediately after loading."))
	load_parser.add_argument('-i', '--disable-indices', dest='disable_indices', action='store_true',
								help=("If this flag is included, any table indices will be dropped "
										"before loading and recreated after."))
	load_parser.add_argument('-s', '--staging-exists', dest='create_staging', action='store_false',
									help="Include if a table named table_staging already exists.")
	load_parser.add_argument('-r', '--expected-rowcount', dest='expected_rowcount', type=int,
									help='Number of rows expected in the table after loading.')
	# CSV ARGS
	load_parser.add_argument('-qc', '--quotechar', default=None, help='Character to enclose fields. If not included, fields are not enclosed.')
	load_parser.add_argument('-ns', '--null-string', default=DEFAULT_NULL_STRING, help='String to replace NULL fields.')
	load_parser.add_argument('-d', '--delimiter', default=DEFAULT_CSV_PARAMS['delimiter'], help='Field separation character.')
	load_parser.add_argument('-esc', '--escapechar', default=DEFAULT_CSV_PARAMS['escapechar'], help='Escape character.')
	load_parser.add_argument('-l', '--lineterminator', default=DEFAULT_CSV_PARAMS['lineterminator'], help='Record terminator.')
	load_parser.add_argument('-e', '--encoding', default=DEFAULT_CSV_PARAMS['encoding'], help='Character encoding.')

	load_parser.set_defaults(func=load)


def __get_csv_params(args):
	csv_params = {}
	csv_params['delimiter'] = args.delimiter
	csv_params['escapechar'] = args.escapechar
	csv_params['lineterminator'] = args.lineterminator
	csv_params['encoding'] = args.encoding
	if args.quotechar:
		csv_params['quoting'] = unicodecsv.QUOTE_ALL
		csv_params['quotechar'] = args.quotechar
	else:
		csv_params['quoting'] = unicodecsv.QUOTE_NONE
	return csv_params


if __name__ == "__main__":
	main()