
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

import time
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
    t1 = time.time()


    conf = SparkConf()

    conf.set("spark.Master", "local[*]")
    # conf.set("spark.executor.memory", "6g")
    # conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    # conf.set("spark.executor.extraJavaOptions", "-XX:+UseCompressedOops -XX:+PrintGCDetails -XX:+PrintGCTimeStamps")


    sc = SparkContext(appName='SearchMusic', conf=conf)
    ss = SparkSession.builder\
                     .appName('SearchMusic')\
                     .master('local[*]')\
                     .getOrCreate()

    print(sc._conf.getAll())

    meta_df, music_df, query_df = music_query_parser.do(sc, ss)
    # music_df.persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
    # query_df.persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
    # meta_df.persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
    h = hdhash.HdHash(band_size * NR_BIT_PER_WORD, row)
    search = parallel_search.ParallelSearch(ss, query_df, music_df, meta_df, window_size, band, row, h, hdhash.hamming_distance)
    print ('After ParallelSearch %f' % (time.time() - t1))

    t2 = time.time()
    search.by_qid(7, THRESHOLD)
    # for i in range(30):
    #     print ('QID : %d' %i)
    #     search.by_qid(i, THRESHOLD)
    print ('After search %f' %(time.time() - t2))

    ss.stop()
    sc.stop()