import music
import query
import hamming
import csv
import plotly
import plotly.plotly as py
import plotly.graph_objs as go
import plotly.figure_factory as FF
import scipy.stats as stats
import matplotlib.pyplot as plt
import sys

import numpy as np
import pandas as pd


# __REPORT_FILE_ = None

def init(report_file):
    plotly.tools.set_credentials_file(username='younghyunjo', api_key='E7FEeG8cgHgJI81jE5Qg')
    return

def graph():
    hash_smc = []

    with open('/home/younghyun/work/younghyunjo/ki/report/report3.csv') as reader:
        flag = 1
        for line in reader:
            fields = line.split(',')
            if flag:
                flag = 0
                continue
            hash_smc.append(abs(float(fields[4]) - float(fields[3])))

    print (len(hash_smc))
    plt.hist(hash_smc)
    plt.show()


def do(music, queries):
    for  q_index in range(len(queries)):
        filename = '/home/younghyun/work/younghyunjo/ki/report%d.csv' % q_index
        f = open(filename, 'w')
        fields = ['query', 'musicFile', 'musicId', 'hash_smc', 'real-smc', 'index']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        csv_writer = csv.writer(f)

        query = queries.queries[q_index]

        for m in music.db:
            for i in range(len(m['hash_table'])):
                hashed_smc = hamming.smc(query['hashed'], m['hash_table'][i])
                real_smc = hamming.smc(query['codes'], m['codes'][i:i+155])
                writer.writerow({'query':q_index, 'musicFile': m['file'], 'musicId': m['id'], 'hash_smc':hashed_smc, 'real-smc':real_smc, 'index':i*155})
        f.close()

