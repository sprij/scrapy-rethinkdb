import rethinkdb as r
from rethinkdb.ast import RqlQuery


class TableNotFound(Exception):
    """Indicates that a table wasn't found """
    pass


class RethinkDBDriver(object):
    def __init__(self, connection_settings):
        """RethinkDB Driver to wrap connection management on top of the
        official driver.

        @param connection_settings: Connection settings to be used to create
        connection.
        """
        if not isinstance(connection_settings, dict):
            raise ValueError('The argument connection_settings should be a '
                             '<dict>. Got <%s> instead'
                             % type(connection_settings))

        self._conn = None
        self.connection_settings = connection_settings

    @property
    def connection(self):
        """Gets/creates connection.

        @returns Connection
        """
        if not self._conn:
            self._conn = r.connect(**self.connection_settings)

        return self._conn

    def table_exists(self, table_name):
        """Checks if a table exists in the database

        @param table_name: table name
        @returns Whether the table was found in the db or not.
        """
        table_list = self.execute(r.table_list())
        return table_name in table_list

    def get_table(self, table_name):
        """Gets table by name.

        @param table_name: table name to be retrieved from the current
        connection.
        @returns Table with the requested name.
        @raises TableNotFound: The table wasn't found in the current
        connection.
        """
        if not self.table_exists(table_name):
            raise TableNotFound("Table with name <%s> not found."
                                % table_name)
        return r.table(table_name)

    def execute(self, stmt):
        """Executes statement against the database.

        @param stmt: statement to run against db.
        @returns Result obtained from running the statement.
        @raises ValueError: The statement is not an instance of
        redirectdb.ast.RqlQuery as expected.
        """
        if not isinstance(stmt, RqlQuery):
            raise ValueError('Expecting <RqlQuery> instance, got <%s>'
                             % type(stmt))

        return stmt.run(self.connection)
