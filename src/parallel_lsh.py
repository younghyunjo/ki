from pyspark.sql.functions import *
from pyspark.sql.types import *

import lsh

class ParallelLsh():
    COLUMN_HASHED = 'hashed'
    def __init__ (self, ss, window_size, band, row, hash):
        self._ss = ss
        self._window_size = window_size
        self._band = band
        self._row = row
        self._lsh = lsh.Lsh(window_size, band, row, hash)

    def _hashing(self, df, column_code):
        udf_hashing = udf(self._lsh.hashing, ArrayType(LongType(), False))
        return df.withColumn(self.COLUMN_HASHED, udf_hashing(df[column_code])).persist()

    def _lookup_rdd_get(self, hashed_df, column_id, column_code):
        column_hash = self.COLUMN_HASHED
        lookup_table= self._lsh.lookup_table

        return hashed_df.rdd.map(lambda r: (r[column_id], lookup_table(r[column_hash])))\
                     .flatMapValues(lambda r: r) \
                     .map(lambda w: (w[0], w[1][0], w[1][1]))

    def _lookup_df_get(self, lookup_rdd, column_id, column_code):
        schema = StructType([StructField(column_id, IntegerType(), False),
                             StructField('lookup_key', StringType(), False),
                             StructField('lookup_value', ArrayType(LongType(), False), False)
                             #StructField(column_code, ArrayType(LongType(), False), False)
                             ])
        lookup_df = self._ss.createDataFrame(lookup_rdd, schema)
        return lookup_df

    def lookup_table(self, df, column_code, column_id):
        hashed_df = self._hashing(df, column_code)
        lookup_rdd = self._lookup_rdd_get(hashed_df, column_id, column_code)
        lookup_df = self._lookup_df_get(lookup_rdd, column_id, column_code)
        return lookup_df
