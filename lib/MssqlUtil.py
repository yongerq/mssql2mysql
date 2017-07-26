#coding=utf-8

class MssqlUtil:
    def setMssqlHandle(self, handle):
        self.handle = handle

    def getPrimaryKey(self, table):
        table = table[table.find('.')+1:len(table)]

        primaryKeySqlTemplate = '''
        select
            kcu.column_name
        from information_schema.table_constraints tc
                inner join information_schema.key_column_usage kcu
                    on kcu.constraint_name = tc.constraint_name
        where tc.constraint_type='PRIMARY KEY'
          AND tc.table_name='{}'
        '''

        self.handle.execute(primaryKeySqlTemplate.format(table))

        results = self.handle.fetchall()

        if len(results) <= 0:
            return ()

        return [result[0] for result in results]

    def _genPrimaryKeySql(self, table):
        primaryKeys = self.getPrimaryKey(table)

        primarySql = ',PRIMARY KEY('
        for result in primaryKeys:
            primarySql += '`' + result + '`,'

        primarySql = primarySql[:-1]
        primarySql += ')'

        return primarySql

    def _getTableColumn(self, table):
        sql = "SELECT * FROM sys.syscolumns WHERE id = OBJECT_ID('%s')" % table
        self.handle.execute(sql)
        columns = self.handle.fetchall()

        # print 'sql:'
        # print sql

        return columns

    def _createTableDefaultColumn(self):
        sql = 'sdg_gmt_modify timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,'

        return sql

    def genCreateTableSql(self, table, createTableName, dataTypes):
        attr = ""
        noLength = [56, 58, 61]

        columns = self._getTableColumn(table)
        # print columns
        primaryKeySql = self._genPrimaryKeySql(table)

        for col in columns:
            colType = dataTypes[int(col.xtype)]

            if col.xtype == 60:
                colType = "float"
                attr += col.name + ' ' + colType + "(" + str(col.length) + "),"
            elif col.xtype == 34:
                colType = 'varbinary'
                attr += col.name + ' ' + colType + "(" + str(2048) + "),"
            elif col.xtype in noLength:
                attr += col.name + ' ' + colType + ","
            else:
                attr += col.name + ' ' + colType + "(" + str(col.length) + "),"

        defaultCreateColumnSql = self._createTableDefaultColumn()
        attr += defaultCreateColumnSql

        attr = attr[:-1]

        createSql = "CREATE TABLE IF NOT EXISTS `" + createTableName + "` (" + attr + primaryKeySql + ");"

        return createSql
