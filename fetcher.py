#!/usr/bin/python3
# -*- coding: utf-8 -*-
# encoding: utf-8
# 
# BitMEXHistoryFetcher
#
# Copyright 2018 FawkesPan
# Contact : i@fawkex.me / Telegram@FawkesPan
#
# Do What the Fuck You Want To Public License
#
try:
    import requests
except ModuleNotFoundError:
    print('Module requests not installed yet. Install it with: pip install requests')
    exit(0)
import sys
if len(sys.argv) != 4:
    print('Usage: ./fetcher.py {SYMBOL} {MINUTES} {FORMAT}\nExample: ./fetcher.py XBTUSD 1440 csv\nSupported formats: csv,h5df.')
    exit(0)
if sys.argv[3] == 'h5df':
    try:
        import h5py
    except ModuleNotFoundError:
        print('Module h5py not installed yet. Install it with: pip install h5py')
        exit(0)
    try:
        import numpy
    except ModuleNotFoundError:
        print('Module numpy not installed yet. Install it with: pip install numpy')
        exit(0)

elif sys.argv[3] != 'csv':
    print('Unsupported output format.\nSupported formats: csv,h5df.')
else:
    import csv
import time
import json

endPoint = 'https://www.bitmex.com/api/udf/history?symbol=%s&resolution=%d&from=%d&to=%d'
length = int(sys.argv[2])*60

data = {
    'o' : [],
    'h' : [],
    'l' : [],
    'c' : [],
    'v' : [],
    't' : []
}

reqSession = requests.Session()

def getData(start, end):
    url = endPoint % (sys.argv[1], 1, start, end)
    data = reqSession.get(url).json()
    time.sleep(2)
    return data

end = int(time.time())

while length>0:
    if length<86400:
      _len = length
      length = 0
    else:
      _len = 86400
      length-=86400
    start = end - _len
    _data = getData(start, end)
    for key in data.keys():
      data[key] = _data[key] + data[key]
      del data[key][0]
    try:
        print('%.2f %% Completed.' % (float(len(data['o'])/int(sys.argv[2]))*100))
    except:
        print('100.00% Completed.')
    end-=_len

if sys.argv[3] == 'h5df':
    with h5py.File(sys.argv[1]+'.h5', 'w') as F:
        for key in data.keys():
            F.create_dataset(key, data=numpy.array(data[key]))
else:
    with open(sys.argv[1]+'.csv', 'w') as F:
        writer = csv.writer(F)
        writer.writerow(data.keys())
        for i in range(0, len(data['o'])):
            writer.writerow([data[x][i] for x in data.keys()])
exit()
