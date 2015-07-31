DB I/O
======

A simple Python module for the following database operations: importing
from CSV, querying to CSV, or querying to a table in a database. All
database-specific knowledge is abstracted.

Installation
------------

Use pip or setup utils:

::

  pip install dbio

Additionally, you will need the Python DB API modules for the
database(s) that you wish to use. Any databases that are currently
supported have the modules listed in the *extras\_require* dictionary in
``setup.py``. For example, if you wish to install the modules needed for
using MySQL and Vertica, you can augment the above command:

::

    pip install dbio[MySQL, Vertica]

Note that some Python DB API modules may have OS-dependent pre-requisite
packages to install. Consult the module's installation guide if setup
fails.

Usage
-----

To run an operation from the command line:

::

    dbio dbio_args op_name op_args

For general help, use ``dbio -h``. For operation specific help and a
list of args, use ``dbio op_name -h``. All commands have CSV formatting
options that can be specified via the command-line. For the full list,
view ``dbio -h``.

To call an operation within Python:

.. code:: python

    import dbio   
    dbio.op_name(op_args, op_kwargs)

All operations require an `SQLAlchemy
URL <http://docs.sqlalchemy.org/en/rel_1_0/core/engines.html#database-urls>`__
for each database used in the operation.

Logging is supported via the Python ``logging`` module.

Tests can be run with

::

    python setup.py test -a 'py.test args'

Operations
----------

All operations support custom CSV parameters, which can be supplied via
the command line with the following optional arguments (placed after
``dbio`` and before ``op_name``).

-  ``-ns``: a string to write in place of None (NULL) values. Defaults
   to the empty string.
-  ``-d``: field delimiter character. Defaults to ``','``.
-  ``-esc``: escape character. Defaults to ``'\'``.
-  ``-l``: line/record terminator. Defaults to ``'\r\n'``.
-  ``-e``: encoding. Defaults to ``'utf-8'``.

Some databases (e.g. Vertica, PostgreSQL) allow specifying a specific
string to represent NULL values. MySQL only allows "", so if you wish to
make a MySQL table the target of a **load** or **replicate** operation,
and you also wish to distinguish between NULL and the empty string, use
``dbio -ns "\N" replicate query_db_url mysql_url query mysql_table``.

Replicate
~~~~~~~~~

::

    dbio replicate query_db_url load_db_url query table

Runs ``query`` against the database pointed to by ``query_db_url``,
loading the results into ``table`` in the database pointed to by
``load_db_url``. The rows in the loading database that existed before
replication are removed.

Optional flags:

-  ``-f``: indicates that ``query`` is the name of a file.
-  ``-a``: runs in append mode. Rows that were in the loading database
   before replication are preserved.
-  ``-nf``: does not use ``mkfifo()``. Use this if ``mkfifo()`` is not
   supported by your OS (e.g. Windows).

How it Works
^^^^^^^^^^^^

**Replicate** creates a Unix
`first-in-first-out <http://linux.die.net/man/3/mkfifo>`__ object
(a.k.a. a "named pipe"). Using the ``subprocess`` module, **Load** and
**Query** are then simultaneously executed, with the loading operation
acting as the pipe reader, and the query object acting as the pipe
writer. This allows query results to be streamed directly into the
database's preferred method of import.

Load
~~~~

::

    dbio load db_url table filename

Loads the contents of a csv file at ``filename`` into ``table`` in the
database pointed to by ``load_db_url``. The rows in the loading database
that existed before replication are removed.

Optional flags:

-  ``-a``: runs in append mode. Rows that were in the loading database
   before replication are preserved.

Query
~~~~~

::

    dbio query db_url query filename

Runs ``query`` against the database pointed to by ``query_db_url``,
placing the results into a CSV file at ``filename``.

Optional flags:

-  ``-f``: indicates that ``query`` is the name of a file.
-  ``-b``: specify ``batch_size``, which determines the number of rows. to store in memory before writing to the file. Defaults to 1,000,000.

Databases
---------

MySQL
~~~~~

Include 'MySQL' in the list of extras when installing.

Requires `MySQL-python <https://pypi.python.org/pypi/MySQL-python>`__.

PostgreSQL
~~~~~~~~~~

Include 'PostgreSQL' in the list of extras when installing.

Requires `psycopg2 <https://pypi.python.org/pypi/psycopg2>`__.

SQLite
~~~~~~

Included in the Python standard library. Note that the SQLite python
library has no method designed for bulk-loading from CSV, so batch
insert statements are used, which may cause bottlenecks that are not
present for other databases.

Note: Currently importing NULL values does not work correctly, even with
a specified null\_string. This is a limitation of Python's CSV reader.

Vertica:
~~~~~~~~

Include 'Vertica' in the list of extras when installing.

Requires `vertica-python <https://github.com/uber/vertica-python>`__ and
`sqlalchemy-vertica-python <https://github.com/LocusEnergy/sqlalchemy-vertica-python>`__

Alternatively, there is support for using pyodbc to drive the
connection.

Additional Databases
~~~~~~~~~~~~~~~~~~~~

To add support for a new database:

1. Create a class inside the ``dbio.databases`` subpackage that extends
   ``dbio.databases.base.Exportable`` and/or
   ``dbio.databases.base.Importable`` depending on desired
   functionality. The DB must have a valid SQLAlchemy Dialect. Existing
   supported databases are listed
   `here <http://docs.sqlalchemy.org/en/rel_1_0/core/engines.html#supported-databases>`__,
   but SQLAlchemy also supports `registering new
   dialects <http://sqlalchemy.readthedocs.org/en/latest/core/connections.html#registering-new-dialects>`__.
2. Add a corresponding import and mapping dictionary entry into ``databases.__init__.py``

Examples
--------

Query a Vertica database and stream the results into a MySQL table with a schema matching the results:

::

    dbio -ns "\N" replicate "vertica+vertica_python://user:pwd@host:port/database" "mysql://user:pwd@host:port/database" "SELECT * FROM vertica_table" mysql_table

Load foo.csv with "|" field delimiters into a PostgreSQL table:

::

    dbio -d "|" load "postgresql://user:pwd@host:port/database" foo_table foo.csv


Query a SQLite table using a query file and write the results to a CSV with NULL represented by "NULL" and lines terminated with "\\n".

::

    dbio -ns NULL -l "\n" query "sqlite:///path/to/sqlite/db/file.db" foo_query.sql foo.csv -f