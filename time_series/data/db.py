import pymysql

MySQL_HOST = 'CohasMac.local'
MySQL_PORT = 3306
MySQL_USER = 'coha'
MySQL_PASSWD = 'cocohaha'
MySQL_DB = 'time_series'

def execute_query(query):
    connection = pymysql.connect(host=MySQL_HOST, port=MySQL_PORT, user=MySQL_USER, passwd=MySQL_PASSWD, db=MySQL_DB)
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    cursor.close()
    result = connection.commit()
    connection.close()
    return result

def get_query_result(query):
    connection = pymysql.connect(host=MySQL_HOST, port=MySQL_PORT, user=MySQL_USER, passwd=MySQL_PASSWD, db=MySQL_DB, use_unicode=True, charset='utf8')
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)

    result = []
    for r in cursor.fetchall():
        result.append(r)
    connection.close()
    return result
