from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.functions import abs
from pyspark.sql.functions import mean


import signature

NR_BIT_PER_BYTE = 8
NR_BYTE_PER_WORD = 4
NR_BIT_PER_WORD = NR_BIT_PER_BYTE * NR_BYTE_PER_WORD

def distance(a, b):
    return bin(a^b)[2:].count('1')

def _actual_hamming_distance_list(a, b):
    if len(a) != len(b):
        return 0.0
    sum_distance = 0
    for i in range(len(a)):
        sum_distance += distance(a[i], b[i])
    return round(sum_distance/(len(a)*NR_BIT_PER_WORD), 2)

class Search():
    def __init__(self, qdf, mdf, meta_df, signature_length, window_size):
        self._signature = signature.Signature(signature_length, window_size)
        self._qdf = qdf
        self._mdf = mdf
        self._meta_df = meta_df
        self._query_alias = 'q'
        self._music_alias = 'm'

    def _signatured_qdf(self, qid=None):
        qdf = self._qdf
        if (qid != None):
            qdf = self._qdf.where(col('qid') == qid)
        return self._signature.do(qdf, 'qcode', 'qid')

    def _signatured_mdf(self, mid=None):
        mdf = self._mdf
        if (mid != None):
            mdf = self._mdf.where(col('mid') == mid)
        return self._signature.do(mdf, 'mcode', 'mid')

    def _candidate_get(self, signatured_qdf, signatured_mdf, threshold):
        return self._signature.hamming_distance(signatured_mdf, signatured_qdf.rdd.first()['signature'], threshold)

    def _debug(self, searched_df):
        searched_df = searched_df.withColumn('diff', abs(searched_df.hamming_distance - searched_df.actual_hamming_distance))
        searched_df.select(mean('diff')).show()
        searched_df.show(searched_df.count())

    def _actual_get(self, candidate_df, threshold):
        hamming_distance_udf = udf(_actual_hamming_distance_list, DoubleType())
        searched_df =  candidate_df.withColumn('actual_hamming_distance', hamming_distance_udf(candidate_df['window' + self._query_alias], candidate_df['window' + self._music_alias]))\
                                   .drop('window'+self._music_alias, 'window'+self._query_alias, 'origin'+self._query_alias)

        return searched_df.filter(col('actual_hamming_distance') <= threshold)

    def by_query_id_music_id(self, qid, mid, threshold):
        signatured_qdf = self._signatured_qdf(qid)
        signatured_qdf.persist()
        # signatured_qdf.describe().show()

        # if signatured_qdf.isEmpty():
        #     return False
        # signatured_qdf.show()

        signatured_mdf = self._signatured_mdf(mid)

        signatured_mdf.persist()
        # signatured_mdf.describe().show()

        # signatured_mdf.show()

        candidate_df = self._candidate_get(signatured_qdf, signatured_mdf, threshold)
        candidate_df.describe().show()
        candidate_df.show()
        # candidate_df.persist()
        #
        # signatured_qdf.unpersist()
        # signatured_mdf.unpersist()

        # searched_df = self._actual_get(candidate_df, threshold)
        # candidate_df.unpersist()
        # searched_df.describe().show()

        # self._debug(searched_df)

        # return searched_df