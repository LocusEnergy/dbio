.. dbio documentation master file, created by
   sphinx-quickstart on Thu Aug  6 09:50:10 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to dbio's documentation!
================================

.. toctree::
   :maxdepth: 2

Operations
----------

*Note: these functions are aliased and can be called directly from the dbio module, e.g. dbio.replicate().*

.. automodule:: dbio.io
	:members:

Database Base Classes
---------------------

To add support for an additional database, extend at least one of the following classes.

.. autoclass:: dbio.databases.base.Exportable
    :members:


.. autoclass:: dbio.databases.base.Importable
    :members:




Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

