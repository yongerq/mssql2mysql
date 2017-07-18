#coding=utf-8

class Config:
    @staticmethod
    def getMssqlConfig():
        # for linux:{FreeTDS}; for windows:{SQL Server}
        # return [{
        #     # for linux:{FreeTDS}; for windows:{SQL Server}
        #     # 'driver': '{FreeTDS}',
        #     'driver': '{SQL Server}',
        #     'host': '192.168.10.58',
        #     'port': 1433,
        #     'username': 'hzqq',
        #     'password': 'xmmx2017',
        #     'db': 'md_1026'
        # },
        # {
        #     'driver': '{SQL Server}',
        #     'host': '192.168.10.33',
        #     'port': 1433,
        #     'username': 'hzqq',
        #     'password': 'xmmx2017',
        #     'db': 'md_1014'
        # }
        # ]
        return [{
            'driver': '{FreeTDS}',
            'host': '10.17.11.12',
            'port': 1433,
            'username': 'callcenter_query',
            'password': 'CnuY1COllFQxcOQP',
            'db': 'QuickCCMV3.1'
        }]

    @staticmethod
    def getMysqlConfig():
        # return {
        #     'host': '115.236.161.19',
        #     'port': 3309,
        #     'username': 'item_sync',
        #     'password': 'wGkA2Z9PeI2079ax',
        #     'db': 'xiashang_item'
        # }
        return {
            'host': '10.17.1.63',
            'port': 3306,
            'username': 'yangqiuhua',
            'password': 'uixH3sUTACWPW9aQ',
            'db': 'callcenter_test'
        }

    @staticmethod
    def getSyncTable():
        # return ['[1026].tbGoods', '[1026].tbStocks']
        return ['Device', 'Agent', 'AcdGroupAgent']

    @staticmethod
    def getSyncTableWithDB(DB):
        map = {
            'md_1026': ['[1026].tbGoods', '[1026].tbStocks'],
            'md_1014': ['[1014].tbGoods', '[1014].tbStocks']
        }

        # return map[DB]

        # return ['tbGoods', 'tbStocks']
        return ['Device', 'Agent', 'AcdGroupAgent']

    @staticmethod
    def getLogFile():
        # Log file config
        return '/data/www/logs/apps/mscollector/default.log'
