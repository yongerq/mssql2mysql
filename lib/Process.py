# coding=utf-8
from lib.Utils import Utils
import lib.data as data

class Process:
    def setLogging(self, logging):
        self.logging = logging

    def setDBHandle(self, mysqlHandle, mssqlHandle):
        self.mysqlHandle = mysqlHandle
        self.mssqlHandle = mssqlHandle

    def refreshData(self, myTable, msTable, pk):
        # mssqlRows = self.mssqlHandle.getMssqlRows(msTable)
        # mysqlRows = self.mysqlHandle.getMysqlRows(myTable)
        # blockSize = 1000

        mssqlHandleRows = self.mssqlHandle.fetchAll("SELECT * FROM {0}".format(msTable))
        mssqlHandleHeaders = self.mssqlHandle.getHeaders()

        mysqlHandleRows = self.mysqlHandle.fetchAll("SELECT * FROM `{0}`".format(str(myTable)))

        mssqlHandleResults = []
        mysqlHandleResults = list(mysqlHandleRows)

        for mssqlHandleRow in mssqlHandleRows:
            mssqlHandleResults.append(data.getDict(mssqlHandleHeaders, mssqlHandleRow))

        for dict in mssqlHandleResults:
            try:
                find = Utils.checkSameData(mysqlHandleResults, dict, pk)
            except Exception as e:
                find = None
                self.logging.debug("Record not found in destination database. Setting variable to none. error:{}".format(e))

            if dict != find:
                query = data.repQuery(dict, find, myTable, pk)

                try:
                    self.mysqlHandle.query(query[0])
                except Exception as e:
                    self.logging.error("{0} failed to update/insert with the following error: {1}".format(str(dict), e))
            else:
                self.logging.debug("the same data, unwant sync. {}".format(str(dict)))

        del mssqlHandleResults[:]
        del mysqlHandleResults[:]

    def deleteData(self, myTable, msTable, pk):
        mssqlHandleCount = self.mssqlHandle.getMssqlRows(msTable)
        mysqlHandleCount = self.mysqlHandle.getMysqlRows(myTable)

        self.logging.info('In deleteData func, mssqlHandleCount:' + str(mssqlHandleCount) + ' mysqlHandleCount:' + str(mysqlHandleCount))

        if mysqlHandleCount > mssqlHandleCount:
            self.logging.info('In deleteData func, mysqlHandleCount > mssqlHandleCount to delete data.')

            selectSql = Utils.genPkSelect(pk)

            mssqlQuery = 'SELECT ' + selectSql + ' FROM {}'.format(msTable)

            mssqlHandlePk = self.mssqlHandle.fetchAll(mssqlQuery)

            mssqlHandlePksToDelete = data.listToDict(mssqlHandlePk)

            diffWhere = Utils.genWhere(mssqlHandlePksToDelete, True, 'AND')
            diffSql = 'SELECT ' + selectSql + ' FROM {} WHERE '.format(myTable) + diffWhere

            self.logging.info('In deleteData func, diffSql:' + diffSql)

            mysqlHandleDiff = self.mysqlHandle.fetchAll(diffSql)

            # for toDelete in mysqlHandleDiff:
            delWhere = Utils.genWhere(mysqlHandleDiff)

            query = data.delQueryWithWhere(myTable, delWhere)

            try:
                self.mysqlHandle.query(query)

                self.logging.info(query)
            except Exception as e:
                self.logging.debug("Failed to delete with the following error: {}".format(e))
        else:
            self.logging.info("In deleteData func, No records to delete.")
