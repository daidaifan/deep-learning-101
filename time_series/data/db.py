import talib
import numpy as np
import pdb
import pymysql
import itertools
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

def get_specific_time_interval(sids, start_date, end_date):
    query = """
        SELECT
            id, date, volume, cost, open, high, low, close, label, ntrans
        FROM
            daily_performance
        WHERE
            id in ({})
            and date >= '{}'
            and date <= '{}'
        """.format(','.join('\'{}\''.format(sid) for sid in sids), start_date, end_date)
    print(query)
    reply = get_query_result(query)
    sid2date2data = {}
    for r in reply:
        sid, date, volume, cost, open_p, high, low, close_p, label, ntrans = get_one_instance(r)
        if sid not in sid2date2data:
            sid2date2data[sid] = {}
        sid2date2data[sid][date] = Instance(sid, date, volume, cost, open_p, high, low, close_p, label, ntrans)
    return sid2date2data

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

def strategy_volume(Ins, highs, lows, volumes, closes, maxlen):
    this_high = Ins.high
    this_volume = Ins.volume
    this_close = Ins.close
    this_low = Ins.low
    if len(volumes) == maxlen:
        avg_volume = np.mean(volumes)
        avg_close = np.mean(closes)
        highs, lows, closes = map(list, [highs, lows, closes])
        highs, lows, closes = np.array(highs), np.array(lows), np.array(closes)
        #MA_Type: 0=SMA, 1=EMA, 2=WMA, 3=DEMA, 4=TEMA, 5=TRIMA, 6=KAMA, 7=MAMA, 8=T3 (Default=SMA)
        #k, d = talib.STOCH(high=highs, low=lows, close=closes, fastk_period=9, slowk_matype=0, slowk_period=3, slowd_period=3, slowd_matype=0)
        ks, ds = [], []
        for i in (0, 1, 2, 3, 5, 6):
            k, d = talib.STOCH(highs, lows, closes, 9, 3, i, 3, i)
            k, d = k[-1], d[-1]
            ks.append(k)
            ds.append(d)
        #if this_volume > avg_volume * 1.5 and this_close > avg_close * 1.03:
        if this_volume > avg_volume * 1.5 and this_close > avg_close and this_close > closes[-1] * 1.03:
            print('sid {} date {} k {} d {} this_volume {} avg_volume {} this_close {} avg_close {} {}'.format(Ins.id, Ins.date, ks, ds, this_volume, avg_volume, this_close, avg_close, Ins))

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
    start_date = '20180121'
    end_date = '20180807'

    # r = get_one_time_interval('2330', start_date, end_date)
    sid2date2data = get_specific_time_interval(['2454', '2330'], start_date, end_date)
    # sid2date2data = get_all_time_interval(start_date, end_date)

    for sid in sorted(sid2date2data.keys()):
        maxlen = 20
        volumes = deque(maxlen=maxlen)
        closes = deque(maxlen=maxlen)
        highs = deque(maxlen=maxlen)
        lows = deque(maxlen=maxlen)
        for date in sorted(sid2date2data[sid].keys()):
            Ins = sid2date2data[sid][date]
            strategy_volume(Ins, highs, lows, volumes, closes, maxlen)
            #strategy_boolean_tunnel(Ins, volumes, closes, maxlen)
            volumes.append(Ins.volume)
            closes.append(Ins.close)
            highs.append(Ins.high)
            lows.append(Ins.low)
    return

if __name__ == '__main__':
    main()
