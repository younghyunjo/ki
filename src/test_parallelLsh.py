from unittest import TestCase

from pyspark import SparkContext
from pyspark.sql import SparkSession

import parallel_lsh
import parsing as music_query_parser
import hdhash

NR_BIT_PER_WORD = 32



music_file = [
              '/home/younghyun/work/younghyunjo/ki/given/data/songdb_1.bin',
              '/home/younghyun/work/younghyunjo/ki/given/data/songdb_0.bin',
              '/home/younghyun/work/younghyunjo/ki/given/data/songdb_2.bin',
              '/home/younghyun/work/younghyunjo/ki/given/data/songdb_3.bin'
]
query_file = "/home/younghyun/work/younghyunjo/ki/given/data/query.bin"


class TestParallelLsh(TestCase):

    window_size = 155
    band = 31
    row = 8
    band_size = int(window_size/band)

    sc = SparkContext(appName='SearchMusic')
    ss = SparkSession.builder.appName('SearchMusic').master('local[*]').getOrCreate()

    query_df, music_df = music_query_parser.do(sc, ss, query_file, music_file)
    h = hdhash.HdHash(band_size * NR_BIT_PER_WORD, row)
    search = parallel_search.ParallelSearch(ss, query_df, music_df, window_size, band, row, h, hdhash.hamming_distance)

    def test_lookup_table(self):
        mdf = self.music_df
        mdf = self.p.lookup_table(mdf, 'mcode', 'mid')
        mdf.persist()
        mdf.show()