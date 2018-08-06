import twstock
from twstock import Stock

import sys
import os
import pdb

def dump_updated_sid(codes):
    """
    stock = Stock('2330')
    ma_p = stock.moving_average(stock.price, 5)       # 計算五日均價
    ma_c = stock.moving_average(stock.capacity, 5)    # 計算五日均量
    ma_p_cont = stock.continuous(ma_p)                # 計算五日均價持續天數
        ma_br = stock.ma_bias_ratio(5, 10)                # 計算五日、十日乖離值
    """
    dir_path = 'updated'

    for sid in codes:
        print('code {}'.format(sid))
        stock = Stock(sid)
        stock.fetch_from(2018, 7)
        # Data(date=datetime.datetime(2017, 5, 2, 0, 0), capacity=45851963, turnover=9053856108, open=198.5, high=199.0, low=195.5, close=196.5, change=2.0, transaction=15718)

        sid2date2data = {}
        for ins in stock.data:
            sys.stdout.write('.')
            date = ins.date.strftime('%Y%m%d')
            open_p = ins.open
            close_p = ins.close
            high = ins.high
            low = ins.low
            change = ins.change
            capacity = ins.capacity
            transaction = ins.transaction
            turnover = ins.turnover
            if sid not in sid2date2data:
                sid2date2data[sid] = {}
            data = (high, low, open_p, close_p, change, capacity, transaction, turnover)
            sid2date2data[sid][date] = data
        for sid in sid2date2data:
            file_path = os.path.join(dir_path, '{}.csv'.format(sid))
            with open(file_path, 'w') as w:
                for date in sorted(sid2date2data[sid].keys()):
                    high, low, open_p, close_p, change, capacity, transaction, turnover = sid2date2data[sid][date]
                    w.write(','.join(map(str, [sid, date, open_p, close_p, high, low, change, capacity, transaction, turnover])) + '\n')
        print('')


def get_all_codes():
    raw_codes = twstock.codes
    codes = []
    for code, ins in raw_codes.items():
        if ins.market != '上市'  or ins.type != '股票':
            continue
        codes.append(code)
    return codes

if __name__ == '__main__':
    codes = get_all_codes()
    dump_updated_sid(codes)
