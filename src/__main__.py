import os
import argparse
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

import time
import parsing as music_query_parser
import parallel_search
import hdhash

THRESHOLD = 0.35
NR_BIT_PER_WORD = 32

#parser.add_argument('nums', nargs=2)

def argument_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('-mfile', type=argparse.FileType('r'), nargs='+', help='music files    ex)-mfile song0.db, song1.db sonb2.db song3.db')
    parser.add_argument('-qfile', type=argparse.FileType('r'), help='query file    ex) -qfile query.bin')

    args = parser.parse_args()
    mfile = []
    for f in args.mfile:
        mfile.append(f.name)

    qfile = args.qfile.name

    return qfile, mfile


def print_result(searched, qid):
    if not searched:
        print ('Search failed: Query[%d]' % qid)
    else:
        for s in searched:
            filename = os.path.split(s['file'])[1]
            print ('Search success: Query[%d] hit on %s, songid(%d), code_idx(%d)~code_idx(%d)'
                   %(s['qid'], filename, s['mid'], s['col']*5, s['col']*5 + window_size))

def spark_init():
    conf = SparkConf()
    conf.set("spark.Master", "local[*]")

    sc = SparkContext(appName='SearchMusic', conf=conf)
    ss = SparkSession.builder\
                     .appName('SearchMusic')\
                     .master('local[*]')\
                     .getOrCreate()

    print(sc._conf.getAll())
    return sc, ss

if __name__ == '__main__':
    qfile, mfile = argument_parse()

    sc, ss = spark_init()

    window_size = 155
    band = 31
    row = 8
    band_size = int(window_size/band)
    t1 = time.time()


    query_df, music_df = music_query_parser.do(sc, ss, qfile, mfile)


    #debug
    print ('befor search')

    lsh_hash = hdhash.HdHash(band_size * NR_BIT_PER_WORD, row)
    search = parallel_search.ParallelSearch(ss, query_df, music_df,
                                            window_size, band, row,
                                            lsh_hash, hdhash.hamming_distance)

    t2 = time.time()

    # qid = 7
    # searched = search.by_qid(qid, THRESHOLD)
    # if not searched:
    #     print ('Search failed: Query[%d]' % qid)
    # else:
    #     for s in searched:
    #         print ('Search success: Query[%d] hit on %s, songid(%d), code_idx(%d)~code_idx(%d)' %(s['qid'], s['file'], s['mid'], s['col']*5, s['col']*5 + window_size))

    nr_query = 30
    for i in range(nr_query):
        searched = search.by_qid(i, THRESHOLD)
        print_result(searched, i)
    print ('After search %f' %(time.time() - t2))

    ss.stop()
    sc.stop()

