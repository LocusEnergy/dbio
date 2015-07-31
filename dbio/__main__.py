# Python standard library
import argparse
import logging

# Local modules
import io


def load(args):
	csv_params = __get_csv_params(args)
	io.load(args.db_url, args.table, args.filename, 
					args.append, csv_params=csv_params, null_string=args.nullstring)


def query(args):
	csv_params = __get_csv_params(args)
	io.query(args.db_url, args.query, args.filename, 
					query_is_file=args.from_file, batch_size=args.batch_size, 
					csv_params=csv_params, null_string=args.nullstring)


def replicate(args):
	if args.fifo:
		io.replicate(args.query_db_url, args.load_db_url, args.query, args.table,
					 args.append, query_is_file=args.from_file, null_string=args.nullstring)
	else:
		io.replicate_no_fifo(args.query_db_url, args.load_db_url, args.query, 
							 args.table, args.append, query_is_file=args.from_file, 
							 null_string=args.nullstring)


def main():
	parser = argparse.ArgumentParser(prog='dbio', description=("A simple Python module"
			 						"for the following database operations: importing from CSV, "
			 						"querying to CSV, or querying to a table in a database."))
	parser.add_argument('-v', '--verbose', dest='verbose', action='store_true')
	parser.add_argument('-q', '--quiet', dest='quiet', action='store_true')
	parser.add_argument('-ns', '--null-string', default=io.NULL_STRING_DEFAULT, dest='nullstring')
	parser.add_argument('-d', '--delimiter', default=io.CSV_PARAMS_DEFAULT['delimiter'])
	parser.add_argument('-esc', '--escapechar', default=io.CSV_PARAMS_DEFAULT['escapechar'])
	parser.add_argument('-l', '--lineterminator', default=io.CSV_PARAMS_DEFAULT['lineterminator'])
	parser.add_argument('-e', '--encoding', default=io.CSV_PARAMS_DEFAULT['encoding'])
	subparsers = parser.add_subparsers(title='Operation', description="I/O operation.")

	# Replicate 
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
	replicate_parser.add_argument('-nf', '--no-fifo', dest='fifo', action='store_false', 
									help="Include to avoid using mkfifo(), a Unix-only operation.")
	replicate_parser.set_defaults(func=replicate)

	# Load
	load_parser = subparsers.add_parser('load', description=("Load data into a table from a file."))
	load_parser.add_argument('db_url', help="SQLAlchemy engine creation URL for db.")
	load_parser.add_argument('table', help="Table in 'db' that will be filled.")
	load_parser.add_argument('filename', help="File containg data to load into table.")
	load_parser.add_argument('-a', '--append', dest='append', action='store_true', 
							 help=("If this flag is included, any data already in table "
								   "will be preserved."))
	load_parser.set_defaults(func=load)

	# Query
	query_parser = subparsers.add_parser('query', description=("Query a table and put the results into a csv file."))
	query_parser.add_argument('db_url', help="SQLAlchemy engine creation URL for db.")
	query_parser.add_argument('query', help=("SQL query string to run against query_db. If -f is " 
											"included, this argument is assumed to be a file name."))
	query_parser.add_argument('filename', help="File containg data to load into table.")
	query_parser.add_argument('-f', '--file', dest='from_file', action='store_true', 
									help="This flag indicates that 'query' is a file name")
	query_parser.add_argument('-b', '--batchsize', type=int, dest='batch_size', default=io.FILE_WRITE_BATCH)
	
	query_parser.set_defaults(func=query)

	# Handle all arg parsing.
	args = parser.parse_args()

	if args.verbose:
		logging.basicConfig(level=logging.DEBUG)
	elif args.quiet:
		logging.basicConfig(level=logging.WARNING)
	else:
		logging.basicConfig(level=logging.INFO)
	 
	# Call the specified script.
	args.func(args)


def __get_csv_params(args):
	csv_params = io.CSV_PARAMS_DEFAULT.copy()
	csv_params['delimiter'] = args.delimiter
	csv_params['escapechar'] = args.escapechar
	csv_params['lineterminator'] = args.lineterminator
	csv_params['encoding'] = args.encoding
	return csv_params


if __name__ == "__main__":
	main()