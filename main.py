import datetime as dt
import requests as req
import xml.etree.ElementTree as ET
import pandas as pd
import time

class DataLoader:

    def __init__(self, url, date):
        self.url = url
        self.date = date
        self.start = 0
        self.buffer = []

    def load_moex(self):
        data = req.get(URL.format(self.date, self.start))
        tree = ET.fromstring(data.text)
        self.process(tree)
        cursor = tree[1][1][0].attrib
        if cursor['TOTAL'] > cursor['INDEX'] + cursor['PAGESIZE']:
            self.start += int(cursor['PAGESIZE'])
            self.load_moex()

    def process(self, tree):
        data_rows = tree[0][1]
        for row in data_rows:
            if row.attrib['NUMTRADES'] == '0' or row.attrib['CLOSE'] == '0':
                continue
            self.buffer.append(row.attrib)

    def get_buffer(self):
        return self.buffer


def iterate_dates(start):
    date = start
    inc = dt.timedelta(days=1)
    while date < dt.date.today():
        date = date + inc
        yield date

measure_start = time.time()

URL = ('https://iss.moex.com/iss/history/engines/'
       'currency/markets/selt/boards/cets/'
       'securities.xml?date={}&start={}')
DATE_START = dt.date(2022, 3, 1)

records = []

for date in iterate_dates(DATE_START):
    print('Loading for {}...'.format(date))
    loader = DataLoader(URL, date)
    loader.load_moex()
    records += loader.get_buffer()
    time.sleep(0.1)

frame = pd.DataFrame.from_records(records)
frame.to_excel('currencies.xlsx')

measure_end = time.time()
print('Loaded in {:.2f} seconds'.format(measure_end - measure_start))