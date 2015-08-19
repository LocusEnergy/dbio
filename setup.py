import sys
from setuptools import setup
from setuptools.command.test import test as TestCommand

class PyTest(TestCommand):
	user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]
	def initialize_options(self):
		TestCommand.initialize_options(self)
		self.pytest_args = []

	def finalize_options(self):
		TestCommand.finalize_options(self)
		self.test_args = []
		self.test_suite = True

	def run_tests(self):
		#import here, cause outside the eggs aren't loaded
		import pytest
		errno = pytest.main(self.pytest_args)
		sys.exit(errno)

if (sys.version_info[0], sys.version_info[1]) != (2,7):
	print "WARNING: USING UNTESTED PYTHON VERSION."

setup(
	name='dbio',
	version='0.4.3',
	author='Locus Energy',
	author_email='dbio@locusenergy.com',
	license='MIT',
	url='https://github.com/locusenergy/dbio',
	download_url='https://github.com/LocusEnergy/dbio/tarball/0.4.3',
	description='Simple module for database I/O operations.',
	long_description=open("README.rst").read(),
	classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
    ],

	packages=[
		'dbio',
		'dbio.databases'
	],
	entry_points={
		'console_scripts': [
			'dbio = dbio.__main__:main'
		]
	},
	install_requires=[
		'sqlalchemy',
		'unicodecsv'
	],
	extras_require={
		'MySQL' : ['MySQL-python'],
		'Vertica' : ['vertica-python', 'sqlalchemy-vertica-python'],
		'VerticaODBC' : ['pyodbc', 'vertica-sqlalchemy'],
		'PostgreSQL' : ['psycopg2'],
	},
	tests_require=[
		'pytest',
	],
	cmdclass= { 
		'test' : PyTest,
	},
)