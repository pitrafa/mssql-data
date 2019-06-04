import pyodbc


class MsSQL(object):

    def __init__(self, settings):
        self.c = self.connect(settings)

    def connect(self, credentials):
        try:
            conn = pyodbc.connect(
                credentials,
                autocommit=True
            )
        except Exception as error:
            raise error

        return conn

    def query_object(self, sqlcommand):
        cursor = self.c.cursor()
        cursor.execute(sqlcommand)
        result = self.map_keys(cursor)
        if result:
            result = result[0]
        return result

    def execute_query(self, sqlcommand):
        cursor = self.c.cursor()
        cursor.execute(sqlcommand)
        result = self.map_keys(cursor)
        return result

    def execute_non_query(self, sqlcommand):
        cursor = self.c.cursor()
        cursor.execute(sqlcommand)

    def map_keys(self, cursor):
        result = [
            dict(zip([column[0] for column in cursor.description], row))
            for row in cursor.fetchall()
        ]
        return result

    def __del__(self):
        self.c.close()
