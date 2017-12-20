from pyspark.sql.functions import *
from pyspark.sql.types import *

import lsh

class ParallelLsh():
    def __init__(self, ss, window_size, band, raw, hash):
        self._row = row
        self._band = band
        self._window_size = window_size
        self._ss = ss
        self._signature = lsh.Lsh(window_size, band, row, hash)
        return

    def lookup_table(self, df, column_code, column_id):
        return self._signing(df, column_code, column_id)

    def signing(self, df, code_column, id_column):
        col_signature = code_column + 'sign'
        _signing_udf = udf(self._signature.signing, ArrayType(IntegerType(), False))
        _lookup_make= self._signature.lookup_table

        signed_df = df.withColumn(col_signature, _signing_udf(df[code_column])).drop(code_column)
        lookup_rdd = signed_df.rdd.map(lambda r: (r[id_column], _lookup_make(r[col_signature])))\
                                  .flatMapValues(lambda r:r)\
                                  .map(lambda w: (w[0], w[1][0], w[1][1]))

        schema = StructType([StructField(id_column, IntegerType(), False),
                             StructField('lookup_key', StringType(), False),
                             StructField('lookup_value', ArrayType(LongType(), False), False)
                            ])
        lookup_df = self._ss.createDataFrame(lookup_rdd, schema).persist()
        return lookup_df


class ParallelLsh():
    COLUMN_HASHED = 'hashed'
    def __init__ (self, ss, window_size, band, row, hash):
        self._ss = ss
        self._window_size = window_size
        self._band = band
        self._row = row
        self._lsh = lsh.Lsh(window_size, band, row, hash)

    def _hashing(self, df, column_code):
        udf_hashing = udf(self._lsh.hashing, ArrayType(IntegerType(), False))
        return df.withColumn(self.COLUMN_HASHED, udf_hashing(df[column_code])).drop(column_code).persist()

    def _lookup_rdd_get(self, hashed_df, column_id):
        column_hash = self.COLUMN_HASHED
        lookup_table= self._lsh.lookup_table

        return hashed_df.rdd.map(lambda r: (r[column_id], lookup_table(r[column_hash])))\
                     .flatMapValues(lambda r: r) \
                     .map(lambda w: (w[0], w[1][0], w[1][1]))

    def _lookup_df_get(self, lookup_rdd, column_id):
        schema = StructType([StructField(column_id, IntegerType(), False),
                             StructField('lookup_key', StringType(), False),
                             StructField('lookup_value', ArrayType(LongType(), False), False)
                             ])
        lookup_df = self._ss.createDataFrame(lookup_rdd, schema)
        return lookup_df

    def lookup_table(self, df, column_code, column_id):
        hashed_df = self._hashing(df, column_code)
        lookup_rdd = self._lookup_rdd_get(hashed_df, column_id)
        lookup_df = self._lookup_df_get(lookup_rdd, column_id)
        return lookup_df

