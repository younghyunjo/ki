import pyspark
from pyspark.sql.types import *

# from pyspark.sql.functions import *

from pyspark.sql.functions import count
from pyspark.sql.functions import col
from pyspark.sql.functions import explode
from pyspark.sql.functions import udf
from pyspark.sql.window import Window

import parallel_lsh


NR_BIT_PER_WORD = 32

class ParallelSearch():
    def __init__(self, ss, qdf, mdf, meta_df, window_size, band, row, hash, distance_calc):
        self._lsh = parallel_lsh.ParallelLsh(ss, window_size, band, row, hash)
        self._qdf = qdf.persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER_2)
        self._mdf = mdf.persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER_2)
        self._lookup_qdf = self._lsh.lookup_table(qdf, 'qcode', 'qid').persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER_2)
        self._lookup_mdf = self._lsh.lookup_table(mdf, 'mcode', 'mid').persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER_2)
        self._meta_df = meta_df
        self._window_size = window_size
        self._band_size = int(window_size/band)
        self._distance_calc = distance_calc
        self._lookup_mdf.count()
        self._lookup_qdf.count()

    def _candidate_get(self, lookup_qdf, lookup_mdf):
        return lookup_qdf.drop('lookup_value').join(lookup_mdf, 'lookup_key', 'inner') \
            .select(explode('lookup_value'), 'mid', 'qid') \
            .groupBy('col', 'mid', 'qid').count() \
            .filter(col('count') > 4) \
            .sort('count', ascending=False)

    def _candidate_verify(self, candidate_df, threshold):
        def __distance(window_size, band_size, distance_calc):
            def _closure(qcode, mcode, index):
                start_index = index * band_size
                music = mcode[start_index:start_index + window_size]
                return distance_calc(qcode, music)
            return _closure

        w = Window.partitionBy('qid').orderBy(col('count').desc())
        candidate_df = candidate_df.select('qid', 'mid', 'col', count('count').over(w).alias('count'))
        candidate_df = candidate_df.where(col('count') <= 4).drop()

        mdf = self._mdf
        qdf = self._qdf
        df = candidate_df.join(qdf, 'qid', 'inner').join(mdf, 'mid', 'inner')

        distance_get = __distance(self._window_size, self._band_size, self._distance_calc)
        udf_distance = udf(distance_get, FloatType())
        searched_df = df.withColumn('threshold', udf_distance(df['qcode'], df['mcode'], df['col']))\
                        .where(col('threshold') <= threshold)

        return searched_df

    def by_qid(self, qid, threshold):
        return self.by_qid_and_mid(qid, None, threshold)

    def by_qid_and_mid(self, qid, mid, theshold):
        lookup_qdf = self._lookup_qdf
        if qid != None:
            lookup_qdf = lookup_qdf.where(col('qid') == qid)

        lookup_mdf = self._lookup_mdf
        if mid != None:
            lookup_mdf = lookup_mdf.where(col('mid') == mid)

        candidate_df = self._candidate_get(lookup_qdf, lookup_mdf)
        searched_df = self._candidate_verify(candidate_df, theshold)
        searched_df.show()
        return searched_df

