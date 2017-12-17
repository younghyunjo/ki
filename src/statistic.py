import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf

WINDOW_SIZE = 155
THRESHOLD = 0.35

def _distance(a, b):
    return bin(a ^ b)[2:].count('1')

def _hamming_distance_list(a, b):
    if len(a) != len(b):
        return 0.0
    sum_distance = 0
    for i in range(len(a)):
        sum_distance += _distance(a[i], b[i])
    return round(sum_distance/(len(a)*32), 2)

def _brute_force_do(qcode, mcode):
    window_size = WINDOW_SIZE
    distances = []
    sindex = []
    for i in range(0, len(mcode)):
        if (i+window_size) > len(mcode):
            i = len(mcode) - window_size
        hamming_distance = _hamming_distance_list(qcode, mcode[i:i + window_size])
        # print (i, hamming_distance)
        if (hamming_distance <= 0.35):
            distances.append(hamming_distance)
            sindex.append(i)
            # print('-' * 100)
    return distances, sindex

def mk_music_query_distance_rdd(r):
    rows = []
    hamming_distance, sindex = _brute_force_do(r.qcode, r.mcode)
    for i in range(0, len(hamming_distance)):
        single_row = pyspark.sql.Row(qid=r.qid, mid=r.mid, hamming_distance=hamming_distance[i], sindex=sindex[i])
        rows.append(single_row)
    return rows

def brute_force(meta_df, music_df, query_df):
    # df = query_df.crossJoin(music_df)
    # print ('df count:%d' % df.rdd.count())
    music_query_distance_rdd= query_df.where('qid = 7').crossJoin(music_df).rdd.flatMap(lambda row : mk_music_query_distance_rdd(row))

    if music_query_distance_rdd.isEmpty():
        print ('RDD is empty')
        return

    music_query_distance_df = music_query_distance_rdd.toDF()
    music_query_distance_df.write.format('json').save('q')
