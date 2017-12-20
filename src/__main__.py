from pyspark import SparkContext
from pyspark.sql import SparkSession

import parsing as music_query_parser
import parallel_search
import hdhash

THRESHOLD = 0.35
NR_BIT_PER_WORD = 32

if __name__ == '__main__':
    #TODO Get DB File, Query File by Arguments

    window_size = 155
    band = 31
    row = 8
    band_size = int(window_size/band)

    sc = SparkContext(appName='SearchMusic')
    ss = SparkSession.builder.appName('SearchMusic').master('local[*]').getOrCreate()

    meta_df, music_df, query_df = music_query_parser.do(sc, ss)
    h = hdhash.HdHash(band_size * NR_BIT_PER_WORD, row)
    search = parallel_search.ParallelSearch(ss, query_df, music_df, meta_df, window_size, band, row, h)

    search.by_qid(0, THRESHOLD)

    ss.stop()
    sc.stop()