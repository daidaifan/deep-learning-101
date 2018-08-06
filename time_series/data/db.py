import numpy as np
import pdb
import pymysql
from collections import namedtuple, deque

Instance = namedtuple('Instance', ['id', 'date', 'volume', 'cost', 'open', 'high', 'low', 'close', 'label', 'ntrans'])

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

def get_one_instance(r):
    result = []
    for k in ('id', 'date', 'volume', 'cost', 'open', 'high', 'low', 'close', 'label', 'ntrans'):
        result.append(r[k])
    return result

def get_one_time_interval(sid, start_date, end_date):
    query = """
        SELECT
            id, date, volume, cost, open, high, low, close, label, ntrans
        FROM
            daily_performance
        WHERE
            id = '{}'
            and date >= '{}'
            and date <= '{}'
        """.format(sid, start_date, end_date)
    print(query)
    reply = get_query_result(query)
    date2data = {}
    for r in reply:
        sid, date, volume, cost, open_p, high, low, close_p, label, ntrans = get_one_instance(r)
        date2data[date] = Instance(sid, date, volume, cost, open_p, high, low, close_p, label, ntrans)
    return date2data

def get_all_time_interval(start_date, end_date):
    query = """
        SELECT
            id, date, volume, cost, open, high, low, close, label, ntrans
        FROM
            daily_performance
        WHERE
            date >= '{}'
            and date <= '{}'
        """.format(start_date, end_date)
    print(query)
    reply = get_query_result(query)
    sid2date2data = {}
    for r in reply:
        sid, date, volume, cost, open_p, high, low, close_p, label, ntrans = get_one_instance(r)
        if sid not in sid2date2data:
            sid2date2data[sid] = {}
        sid2date2data[sid][date] = Instance(sid, date, volume, cost, open_p, high, low, close_p, label, ntrans)
    return sid2date2data

def strategy_volume(Ins, volumes, closes, maxlen):
    this_volume = Ins.volume
    this_close = Ins.close
    if len(volumes) == maxlen:
        avg_volume = np.mean(volumes)
        avg_close = np.mean(closes)
        if this_volume > avg_volume * 1.5 and this_close > avg_close * 1.03:
            print('sid {} date {} this_volume {} avg_volume {} this_close {} avg_close {} {}'.format(Ins.id, Ins.date, this_volume, avg_volume, this_close, avg_close, Ins))

def strategy_boolean_tunnel(Ins, volumes, closes, maxlen):
    alpha = 2.1
    this_volume = Ins.volume
    this_close = Ins.close
    if len(volumes) == maxlen:
        avg_volume = np.mean(volumes)
        avg_close = np.mean(closes)
        std_volume = np.std(volumes)
        std_close = np.std(closes)
        bound = avg_close + alpha * std_close
        if this_close > bound:
            print('sid {} date {} this_volume {} avg_volume {} this_close {} bound_close {} avg_close {} std_close {} {}'.format(Ins.id, Ins.date, this_volume, avg_volume, this_close, bound, avg_close, std_close, Ins))


def main():
    start_date = '20180621'
    end_date = '20180806'

    # r = get_one_time_interval('2330', start_date, end_date)
    sid2date2data = get_all_time_interval(start_date, end_date)

    for sid in sorted(sid2date2data.keys()):
        maxlen = 20
        volumes = deque(maxlen=maxlen)
        closes = deque(maxlen=maxlen)
        for date in sorted(sid2date2data[sid].keys()):
            Ins = sid2date2data[sid][date]
            #strategy_volume(Ins, volumes, closes, maxlen)
            strategy_boolean_tunnel(Ins, volumes, closes, maxlen)
            volumes.append(Ins.volume)
            closes.append(Ins.close)
    return

if __name__ == '__main__':
    main()
