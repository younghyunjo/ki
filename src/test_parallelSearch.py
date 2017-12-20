from unittest import TestCase

from pyspark import SparkContext
from pyspark.sql import SparkSession

import parsing as music_query_parser
import parallel_search
import hdhash

NR_BIT_PER_WORD = 32

class TestParallelSearch(TestCase):
    window_size = 155
    band = 31
    row = 8
    band_size = int(window_size/band)

    sc = SparkContext(appName='SearchMusic')
    ss = SparkSession.builder.appName('SearchMusic').master('local[*]').getOrCreate()

    sc._conf.get('spark.driver.memory')

    meta_df, music_df, query_df = music_query_parser.do(sc, ss)

    h = hdhash.HdHash(band_size * NR_BIT_PER_WORD, row)

    search = parallel_search.ParallelSearch(ss, query_df, music_df, meta_df, window_size, band, row, h)
    def test_by_qid_mid(self):
        self.search.by_qid_and_mid(21, 42, 0.35)

    # def test_by_qid(self):
    #     for i in range(30):
    #         self.search.by_qid(i, 0.35)

    # def test_by_qid(self):
        # self.search.by_qid(None, 0.35)
