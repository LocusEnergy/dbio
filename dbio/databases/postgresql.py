# PyPI packages
import unicodecsv

# Local modules
from base import Exportable, Importable


class PostgreSQL(Exportable, Importable):

    COPY_CMD = ("COPY {table} FROM STDIN WITH "
                "CSV "
                "DELIMITER '{delimiter}' "
                "NULL '{null_string}' "
                "ESCAPE '{escapechar}';")

    SELECT_INDICES_CMD = ("SELECT indexname, indexdef FROM pg_catalog.pg_indexes "
                            "WHERE tablename='{table}';")

    DROP_INDICES_CMD = ("DROP INDEX {indices};")

    CREATE_STAGING_CMD = "CREATE TABLE {staging} (LIKE {table} INCLUDING ALL);"

    ANALYZE_CMD = "ANALYZE {table};"

    SWAP_CMD = ("ALTER TABLE {table} RENAME TO {temp};"
                         "ALTER TABLE {staging} RENAME TO {table};"
                         "ALTER TABLE {temp} RENAME TO {staging};")

    DROP_CMD = "DROP TABLE IF EXISTS {staging};"

    TRUNCATE_CMD = "TRUNCATE TABLE {staging};"

    GET_GRANTS_CMD = (
        "SELECT 'GRANT ' || string_agg(privilege_type, ',') || ' ON {staging} TO ' || grantee "
        "FROM information_schema.role_table_grants "
        "WHERE table_name='{table}' and grantee != 'root' "
        "GROUP BY grantee"
    )

    DEFAULT_CSV_PARAMS = {
        'delimiter': ',',
        'escapechar': '\\',
        'lineterminator': '\n',
        'encoding': 'utf-8',
        'quoting': unicodecsv.QUOTE_NONE
    }

    DEFAULT_NULL_STRING = 'NULL'

    def __init__(self, url):
        Exportable.__init__(self, url)
        Importable.__init__(self, url)

    def execute_import(self, table, filename, append, csv_params, null_string,
                       analyze=False, disable_indices=False, create_staging=True,
                       expected_rowcount=None):
        staging = table + '_staging'
        temp = table + '_temp'
        if append:
            copy_table = table
        else:
            copy_table = staging

        eng = self.get_import_engine()

        # Start transaction
        with eng.begin() as connection, connection.begin() as tran:
            if not append:
                if create_staging:
                    # Pre drop table in case it already exists
                    connection.execute(self.DROP_CMD.format(staging=staging))
                    connection.execute(
                        self.CREATE_STAGING_CMD.format(staging=staging, table=table))
                    # get list of existing grants for existing table
                    permission_cmds = connection.execute(
                        self.GET_GRANTS_CMD.format(table=table, staging=staging)
                    ).fetchall()
                    # create equal set of grants for the new staging table
                    for cmd, in permission_cmds:
                        connection.execute(cmd)
                else:
                    connection.execute(self.TRUNCATE_CMD.format(staging=staging))

            if disable_indices:
                # fetch index information from pg_catalog
                results = connection.execute(self.SELECT_INDICES_CMD.format(table=copy_table))
                index_names = []
                index_creates = []
                for row in results:
                    index_names.append(row[0])
                    index_creates.append(row[1])
                results.close()

                # drop the entire list of indices in a single DROP INDEX, if any exist
                if index_names:
                    connection.execute(self.DROP_INDICES_CMD.format(indices=','.join(index_names)))

            # get psycopg2 cursor object to access copy_expert()
            raw_cursor = connection.connection.cursor()
            with open(filename, 'r') as f:
                raw_cursor.copy_expert(
                    self.COPY_CMD.format(table=copy_table, null_string=null_string,
                                         **csv_params), f)
                raw_cursor.close()
        with eng.begin() as connection:
            if expected_rowcount is not None:
                self.do_rowcount_check(copy_table, expected_rowcount)

            if disable_indices:
                # create indices from 'indexdef'
                for index_create_cmd in index_creates:
                    connection.execute(index_create_cmd)

            if analyze:
                connection.execute(self.ANALYZE_CMD.format(table=copy_table))

            if not append:
                connection.execute(
                    self.SWAP_CMD.format(table=table, staging=staging, temp=temp))
                if create_staging:
                    connection.execute(self.DROP_CMD.format(staging=staging))