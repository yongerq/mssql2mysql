#coding=utf-8

import sys
import logging
import multiprocessing
from lib.DB import DB
from lib.Config import Config
from lib.Process import Process as DataProcess
from lib.MssqlUtil import MssqlUtil
from lib.Utils import Utils

reload(sys)
sys.setdefaultencoding('utf-8')

def processData(mssqlConfig, mysqlConfig, logFile, tables):
    logging.basicConfig(filename=logFile, filemode='a', level=logging.INFO, format='%(asctime)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    mysqlServer = mysqlConfig['host']
    mysqlPort = mysqlConfig['port']
    mysqlUsername = mysqlConfig['username']
    mysqlPassword = mysqlConfig['password']
    mysqlDB = mysqlConfig['db']
    mysqlHandle = DB.myHandle(mysqlServer, mysqlUsername, mysqlPassword, mysqlDB, mysqlPort)

    print 'mysqlServer:' + mysqlServer + ' mysqlPort:' + str(mysqlPort) + ' mysqlUserName:' + mysqlUsername
    print 'mysqlPassword:' + mysqlPassword + ' mysqlDB:' + mysqlDB

    mssqlDriver = mssqlConfig['driver']
    mssqlServer = mssqlConfig['host']
    mssqlUsername = mssqlConfig['username']
    mssqlPassword = mssqlConfig['password']
    mssqlDB = mssqlConfig['db']
    mssqlPort = mssqlConfig['port']
    mssqlDSN = 'Driver={};Server={};Database={};Uid={};Pwd={};Port={};TDS_Version=8.0;'.format(mssqlDriver, mssqlServer,
                                                                                               mssqlDB, mssqlUsername,
                                                                                               mssqlPassword, mssqlPort)

    print 'mssqlDriver:' + mssqlDriver + ' mssqlServer:' + mssqlServer + ' mssqlUserName:' + mssqlUsername
    print 'mssqlPassword:' + mssqlPassword + ' mssqlDB:' + mssqlDB + ' mssqlPort:' + str(mssqlPort)

    mssqlHandle = DB.msHandle(mssqlDSN)

    mssqlUtil = MssqlUtil()
    mssqlUtil.setMssqlHandle(mssqlHandle.getCursor())
    dataTypes = Utils.getDataTypes()

    process = DataProcess()
    process.setDBHandle(mysqlHandle, mssqlHandle)
    process.setLogging(logging)

    for table in tables:
        msTable = table
        myTable = '{}_{}'.format(mssqlServer, table)

        pk = mssqlUtil.getPrimaryKey(table)

        # Auto create table
        createSql = mssqlUtil.genCreateTableSql(table, myTable, dataTypes)

        # print 'createSql:'
        # print createSql

        mysqlHandle.getCursor().execute(createSql)

        # Update/Insert records in destination DB that do not match source DB.
        logging.info("Preparing to insert/update records.")

        process.refreshData(myTable, msTable, pk)

        logging.info("Insert/update records complete.")

        # Delete records from destination DB that no longer exist in source DB.
        logging.info("Preparing to delete records.")

        process.deleteData(myTable, msTable, pk)

        logging.info("Delete records complete.")

    mssqlHandle.disconnect()
    mysqlHandle.disconnect()

if __name__ == '__main__':
    logFile = Config.getLogFile()
    logging.basicConfig(filename=logFile, filemode='a', level=logging.INFO, format='%(asctime)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    logging.info("Database replication started.")

    mysqlConfig = Config.getMysqlConfig()
    mssqlConfigs = Config.getMssqlConfig()

    # 异步多进程并发运行任务
    pool = multiprocessing.Pool()

    for mssqlConfig in mssqlConfigs:
        mssqlDB = mssqlConfig['db']

        tables = Config.getSyncTableWithDB(mssqlDB)

        multiprocessing.freeze_support()
        pool.apply_async(processData, (mssqlConfig, mysqlConfig, logFile, tables))

    pool.close()
    pool.join()

    logging.info("Database replication complete.")