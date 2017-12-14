from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf


import parsing as music_query_parser
import hashing


THRESHOLD = 0.65
WINDOW_HASH_SIZE = 5
WINDOW_SIZE = 155


def distance(a, b):
    return bin(a^b)[2:].count('1')

#Simple Measure Coefficient = 1 - HammingDistance/length
def smc_list(a, b):
    if len(a) != len(b):
        return 0.0
    sum_distance = 0
    for i in range(len(a)):
        sum_distance += distance(a[i], b[i])
    return (1 - sum_distance/(len(a)*32))

def real_smc_list(qcode, mcode, mstart):
    return smc_list(qcode, mcode[mstart:mstart+WINDOW_SIZE])


if __name__ == '__main__':
    #TODO Get DB File, Query File by Arguments

    sc = SparkContext(appName='SearchMusic')
    ss = SparkSession.builder.appName('SearchMusic').master('local[*]').getOrCreate()

    meta_df, music_df, query_df = music_query_parser.do(sc, ss)

    query_hashed_df = hashing.query(query_df)
    music_hashed_df = hashing.music(music_df)

    smc_list_udf = udf(smc_list, DoubleType())
    real_smc_list_udf = udf(real_smc_list, DoubleType())

    # TODO Change 0.60 to ERROR
    crossjoined_df = query_hashed_df.crossJoin(music_hashed_df)
    similar = crossjoined_df.withColumn('hashed_smc', smc_list_udf(crossjoined_df.qhashed, crossjoined_df.mhashed))\
        .where('hashed_smc >= 0.55').select('qid', 'mid', 'mhashstart')

    similar_query_joined_df = query_df.join(similar, 'qid')
    similar_query_music_joined_df = similar_query_joined_df.join(music_df, 'mid')
    # similar_query_music_joined_df.show()

    # TODO Change 0.65 to THRESHOLD
    hits = similar_query_music_joined_df\
        .withColumn('real_smc', real_smc_list_udf(similar_query_music_joined_df.qcode, similar_query_music_joined_df.mcode, similar_query_music_joined_df.mhashstart))\
        .where('real_smc >= 0.65').join(meta_df, 'mid').drop('mcode','qcode').orderBy('qid')

    # hits.show()

    hits.rdd.foreach(lambda h :
                     print('Search success:Query[%d] hit on %s ,songid(%d), code_idx(%d) ~ code_idx(%d)'
                           % (h[1], h[4], h[0], h[2], h[2]+WINDOW_SIZE)))

    #TODO Handle Search Failed

    sc.stop()