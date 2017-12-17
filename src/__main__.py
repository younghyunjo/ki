from pyspark import SparkContext
from pyspark.sql import SparkSession
import parsing as music_query_parser
import search
import time

THRESHOLD = 0.35
WINDOW_HASH_SIZE = 5
WINDOW_SIZE = 155

def debug2(qdf, mdf, meta_df):
    s = search.Search(qdf, mdf, meta_df, 50, WINDOW_SIZE)

    # qdf.describe().show()
    # mdf.describe().show()


    sa = time.time()
    # s.by_query_id_music_id(7, 45, THRESHOLD)
    s.by_query_id_music_id(7, None, THRESHOLD)
    # for i in range(30):
    #     s.by_query_id_music_id(i, None, THRESHOLD)
    print (time.time() - sa)


if __name__ == '__main__':
    #TODO Get DB File, Query File by Arguments

    sc = SparkContext(appName='SearchMusic')
    ss = SparkSession.builder.appName('SearchMusic').master('local[*]').getOrCreate()

    meta_df, music_df, query_df = music_query_parser.do(sc, ss)

    debug2(query_df, music_df, meta_df)

    ss.stop()
    sc.stop()