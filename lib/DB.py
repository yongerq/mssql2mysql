import pyodbc
import pymysql

class DB(object):
    @staticmethod
    def msHandle(dsn):
        mssql = DB()

        mssql.conn = pyodbc.connect(dsn)
        mssql.cursor = mssql.conn.cursor()

        return mssql

    @staticmethod
    def myHandle(host, user, password, db, port, charset='utf8'):
        mysql = DB()

        conn = pymysql.connect(host=host, user=user, passwd=password, db=db, charset=charset, port=port, autocommit=True)
        mysql.conn = conn
        mysql.cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

        return mysql

    def getCursor(self):
        return self.cursor

    def getHeaders(self):
        cols = [column[0] for column in self.cursor.description]
        return cols

    def query(self, query):
        self.cursor.execute(query)

    def fetchone(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchone()

    def fetchAll(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def getMssqlRows(self, table, where = ''):
        sql = "SELECT COUNT(*) counter FROM {} WHERE 1=1".format(table)
        if '' != where:
            sql += ' AND ' + where

        # print sql

        self.cursor.execute(sql)

        return self.cursor.fetchone()[0]

    def getMysqlRows(self, table, where = ''):
        sql = "SELECT COUNT(*) counter FROM `{}` WHERE 1=1".format(table)
        if '' != where:
            sql += ' AND ' + where

        # print sql

        self.cursor.execute(sql)

        return self.cursor.fetchone()['counter']

    def disconnect(self):
        self.cursor.close()
        self.conn.commit()
        self.conn.close()
