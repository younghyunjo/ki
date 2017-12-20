from pyspark.sql.types import *
from pyspark.sql.functions import *
import parallel_lsh
import parallel_search

NR_BIT_PER_WORD = 32

def distance(a, b):
    return bin(a^b)[2:].count('1')

def _actual_hamming_distance_list(a, b):
    if len(a) != len(b):
        return 0.0
    sum_distance = 0
    for i in range(len(a)):
        sum_distance += distance(a[i], b[i])
    return round(sum_distance/(len(a)*NR_BIT_PER_WORD), 2)


class ParallelSearch():
    def __init__(self, ss, qdf, mdf, meta_df, window_size, band, row, hash):
        self._lsh = parallel_lsh.ParallelLsh(ss, window_size, band, row, hash)
        self._qdf = qdf
        self._mdf = mdf
        self._lookup_qdf = self._lsh.lookup_table(qdf, 'qcode', 'qid')#.persist()
        self._lookup_mdf = self._lsh.lookup_table(mdf, 'mcode', 'mid')#.persist()
        self._meta_df = meta_df

    def by_qid(self, qid, threshold):
        return self.by_qid_and_mid(qid, None, threshold)

    def by_qid_and_mid(self, qid, mid, theshold):
        qdf = self._lookup_qdf

        if qid != None:
            qdf = qdf.where(col('qid') == qid)

        mdf = self._lookup_mdf
        if mid != None:
            mdf = mdf.where(col('mid') == mid)

        estimated_df = qdf.drop('lookup_value').join(mdf, 'lookup_key', 'inner') \
               .select(explode('lookup_value'), 'mid', 'qid')\
               .groupBy('col', 'mid', 'qid').count()


        estimated_df = estimated_df.sort('count', ascending=False)
        estimated_df.show(3)


        #TODO PRINT RESULT

