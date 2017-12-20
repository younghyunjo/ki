from unittest import TestCase

from pyspark import SparkContext
from pyspark.sql import SparkSession

import parallel_lsh
import parsing as music_query_parser
import hdhash

NR_BIT_PER_WORD = 32

class TestParallelLsh(TestCase):

    window_size = 155
    band = 31
    row = 8
    band_size = int(window_size/band)

    sc = SparkContext(appName='SearchMusic')
    ss = SparkSession.builder.appName('SearchMusic').master('local[*]').getOrCreate()

    meta_df, music_df, query_df = music_query_parser.do(sc, ss)

    h = hdhash.HdHash(band_size * NR_BIT_PER_WORD, row)

    p = parallel_lsh.ParallelLsh(ss, window_size, band, row, h)


    def test_lookup_table(self):
        mdf = self.music_df
        mdf = self.p.lookup_table(mdf, 'mcode', 'mid')
        mdf.persist()
        mdf.show()