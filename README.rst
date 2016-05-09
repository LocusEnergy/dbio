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

For more detailed information on calling scripts within Python, `check out the documentation <http://pythonhosted.org/dbio/>`__.

Logging is supported via the Python ``logging`` module.

Tests can be run with

::

    python setup.py test -a 'py.test args'

Operations
----------

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
-  ``-z``: analyzes ``table`` for query optimization after completing the load.
-  ``-i``: drops or disable indices while loading, recreating them afterwards.
-  ``-nf``: does not use ``mkfifo()``. Use this if ``mkfifo()`` is not available.
-  ``-s``: expects an table named 'table_staging' to already exist.
-  ``-rc``: performs a check to ensure that the query rowcount matches the load table rowcount.
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

For a detailed explanation, see `this blog post <http://blog.locusenergy.com/2015/08/04/moving-bulk-data/>`__.

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
-  ``-z``: analyzes ``table`` after completing the load.
-  ``-i``: drops or disable indices while loading, recreating them afterwards.
-  ``-s``: expects a table named 'table_staging' to already exist.
-  ``-rc``: the number of rows to ensure are present in the table after loading.
- csv flags:
    * ``-qc``: character to enclose fields. If not included, fields are not enclosed.
    * ``-ns``: string to replace NULL fields. Defaults to "NULL".
    * ``-d``: field separation character. Defaults to ",".
    * ``-esc``: escape character. Defaults to "\".
    * ``-l``: record terminator. Defaults to "\n".
    * ``-e``: character encoding. Defaults to "utf-8"

Query
~~~~~

::

    dbio query db_url query filename

Runs ``query`` against the database pointed to by ``query_db_url``,
placing the results into a CSV file at ``filename``.

Optional flags:

-  ``-f``: indicates that ``query`` is the name of a file.
-  ``-b``: specify ``batch_size``, which determines the number of rows. to store in memory before writing to the file. Defaults to 1,000,000.
- csv flags:
    * ``-qc``: character to enclose fields. If not included, fields are not enclosed.
    * ``-ns``: string to replace NULL fields. Defaults to "NULL".
    * ``-d``: field separation character. Defaults to ",".
    * ``-esc``: escape character. Defaults to "\".
    * ``-l``: record terminator. Defaults to "\n".
    * ``-e``: character encoding. Defaults to "utf-8"


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

    dbio replicate "vertica+vertica_python://user:pwd@host:port/database" "mysql://user:pwd@host:port/database" "SELECT * FROM vertica_table" mysql_table

Load foo.csv with "|" field delimiters into a PostgreSQL table:

::

    dbio load "postgresql://user:pwd@host:port/database" foo_table foo.csv -d "|"


Query a SQLite table using a query file and write the results to a CSV with NULL represented by "NULL" and lines terminated with "\\n".

::

    dbio query "sqlite:///path/to/sqlite/db/file.db" foo_query.sql foo.csv -f -ns NULL -l "\n"

Changelog
---------
- 0.4.9: Deleting staging table prior to creating it to ensure that function does not fail for that reason.
- 0.4.8: Pinning requirement and "extras" requirements to specific versions where applicable 
- 0.4.7: When a new staging table is created with the postgres db, grants are copied over as well
- 0.4.6: Making version number read from all dbio/__init__.py so it doesn't need to copied and pasted everywhere.
- 0.4.5: Explicit SQLAlchemy transaction creation.
- 0.4.4: Added SQL logic to Vertica and MySQL to raise errors when loading has any issues, including truncated data.
- 0.4.3: Minor logging additions.
- 0.4.2: Added a rowcount check option and randomized pipe names.
- 0.4.1: Support for existing staging tables.
- 0.4.0: Support for temporary index disabling.
- 0.3.4: Link to documentation in README.rst.
- 0.3.3: Added public documentation and minor fixes.
- 0.3.2: Fixed minor vertica.py bugs.
- 0.3.1: Fixed critical CLI bug.
- 0.3.0: Handle replication CSV formatting automatically.
- 0.2.0: Add ANALYZE support.
- 0.1.1: Initial public release.
