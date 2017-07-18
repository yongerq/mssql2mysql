# coding=utf-8
import os

class Utils:
    # data1 source data, data2 dest data
    @staticmethod
    def checkSameData(data1, data2, pk):
        for item in data1:
            sign = 0

            for key in pk:
                if item[key] == data2[key]:
                    continue
                else:
                    sign += 1

            if 0 == sign:
                return item

        return None

    @staticmethod
    def getDictWithKey(datas, keys):
        if len(datas) <= 0 or len(keys) <= 0:
            return []

        results = []
        for data in datas:
            line = {}
            for key in keys:
                line[key] = data[key]
            results.append(line)

        return results

    @staticmethod
    def genPkSelect(pk):
        query = ''
        params = []

        for key in pk:
            query += '{},'
            params.append(key)

        query = query[:-1]

        return query.format(*params)

    @staticmethod
    def genWhere(datas, isNot = False, connector = 'OR'):
        query = ''

        if isNot:
            symbol = '!='
        else:
            symbol = '='

        for data in datas:
            query += ' ('

            for key in data.keys():
                query += "`{}`{}'{}' AND ".format(key, symbol, data[key])

            query = query[:-4]
            query += ') ' + connector

        if 'OR' == connector:
            query = query[:-2]
        else:
            query = query[:-3]

        return query

    @staticmethod
    def getDataTypes():
        dataTypeFile = os.path.join(os.path.dirname(__file__), 'sqlserver_datatypes.txt').replace('\\', '/')

        typesFile = open(dataTypeFile, 'r').readlines()

        dataTypes = {}
        for row in typesFile:
            rows = row.split(',')
            dataTypes[int(rows[0].strip())] = rows[1].strip()

        return dataTypes
