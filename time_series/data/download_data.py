#!/Users/coha/.pyenv/shims/python

from grs import BestFourPoint
from grs import Stock
from grs import TWSENo
import threading
#from concurrent.futures import ThreadPoolExecutor

stock_no_list = TWSENo().all_stock_no


def WorkerJob(stock_index, days):
    print('# index {}'.format(stock_index))
    stock = Stock(stock_index, days)
    #stock.out_putfile('data/{}.csv'.format(stock_index))

"""
with ThreadPoolExecutor(max_workers=10) as executor:
    for i in sorted(stock_no_list):
        if len(i) != 4:
            continue
        #stock = Stock(i, 12 * 30)
        #stock = Stock(i, 3 * 1)
        #stock.out_putfile('data/{}.csv'.format(i))
        print(type(i))
        a = executor.submit(WorkerJob, i, 3)

        #threading.Thread(target = WorkerJob, args = (i, 3)).start()
    print(a.result())
"""
for i in sorted(stock_no_list):
    if len(i) != 4:
        continue
    print('[compute] adgroup {}'.format(i))
    #stock = Stock(i, 12 * 30)
    stock = Stock(i, 1)
    stock.out_putfile('/Users/coha/git/time-series-predictor/data/{}.csv'.format(i))
