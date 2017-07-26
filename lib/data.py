from datetime import datetime

def getDict(headers, data):
    if data != None:
        dictionary = dict(zip(headers, data))
        return dictionary
    else:
        return None

def dataToString(data):
    if data == None:
        data = "NULL"
    else:
        data = "'" + str.replace(str(data), "'", "''") + "'"
    return data

def listToDict(datas):
    results = []

    for data in datas:
        descriptions = data.cursor_description
        line = {}

        for key in range(len(descriptions)):
            fieldName = descriptions[key][0]
            line[fieldName] = data[key]

        results.append(line)

    return results

def listToString(data):
    string = ""
    i = 0
    l = len(data) - 1
    for item in data:
        struct = dataToString(item[0])
        if i == l:
            string = string + struct
        else:
            string = string + struct + ", "
        i += 1

    return string

def dictToString(dict):
    headers = ""
    i = 0
    l = len(dict) - 1
    for item in dict:
        if i == l:
            headers = headers + str(item)
        else:
            headers = headers + str(item) + ", "
        i += 1
    return headers

def insertStr(dict):
    string = ""
    i = 0
    l = len(dict) - 1
    for key, item in dict.items():
        struct = dataToString(item)
        if i == l:
            string = string + struct
        else:
            string = string + struct + ", "
        i += 1
    return string

def updateStr(dict):
    string = ""
    i = 0
    l = len(dict) - 1
    for key, item in dict.items():
        struct = dataToString(item)
        if i == l:
            string = string + key + " = " + struct
        else:
            string = string + key + " = " + struct + ", "
        i += 1
    return string

def delQuery(pk_value, pk, table):
    query = "DELETE FROM `{0}` WHERE {1} = {2}".format(table, pk, pk_value)

    msg = "{0} deleted from the destination database.".format(pk_value)

    return query, msg

def delQueryWithWhere(table, where):
    query = "DELETE FROM `{}` WHERE {}".format(table, where)

    return query

def repQuery(dict01, dict02, table, pk):
    if dict02 == None:

        #INSERT INTO DESTINATION DB
        headers = dictToString(dict01)
        insert = insertStr(dict01)
        query = "INSERT INTO `{0}` ({1}) VALUES ({2})".format(table, headers, insert)

        # msg = "{0} inserted into the destination database.".format(dict01[pk])
        msg = ''

    else:
        #UPDATE DESTINATION DB
        update = updateStr(dict01)
        query = "UPDATE `{}` SET {} WHERE 1=1"
        params = [table, update]

        for key in pk:
            query += " AND {} = '{}'"
            params.append(key)
            params.append(dict01[key])

        query = query.format(*params)

        # msg = "{0} updated in the destination database.".format(dict01[pk])
        msg = ''

    return query, msg
