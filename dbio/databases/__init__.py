import importlib

from base import Importable

DEFAULT_CSV_PARAMS = Importable.DEFAULT_CSV_PARAMS
DEFAULT_NULL_STRING = Importable.DEFAULT_NULL_STRING

""" This fancy dynamic importing is just to allow for use of the library without having
	all of the DB API modules installed. """

dialect_driver_class_map = {
	'mysql' : {
		'mysqldb' : getattr(importlib.import_module('dbio.databases.mysql'), 'MySQL')
	},
	'postgresql' : {
		'psycopg2' : getattr(importlib.import_module('dbio.databases.postgresql'), 'PostgreSQL')
	},
	'sqlite' : {
		'pysqlite' : getattr(importlib.import_module('dbio.databases.sqlite'), 'SQLite')
	},
	'vertica' : {
		'vertica_python' : getattr(importlib.import_module('dbio.databases.vertica'), 'Vertica'),
		'pyodbc' : getattr(importlib.import_module('dbio.databases.vertica'), 'VerticaODBC')
	}
}