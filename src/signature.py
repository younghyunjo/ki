import pyspark
import random

from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.functions import col

NR_BIT_PER_BYTE = 8
NR_BYTE_PER_WORD = 4
NR_BIT_PER_WORD = NR_BIT_PER_BYTE * NR_BYTE_PER_WORD

WINDOW_STEP = 4

def _sampling_index_init(nr, max_range):
    sampling_index = []

    for i in range(nr):
        while (True):
            r = random.randrange(0, max_range)
            try:
                sampling_index.index(r)
            except:
                sampling_index.append(r)
                break
    return sampling_index

def _sample_index_to_word_and_bit_index(p):
    word_indnex = int(p / (NR_BYTE_PER_WORD * NR_BIT_PER_BYTE))
    bit_index = int(p % (NR_BYTE_PER_WORD * NR_BIT_PER_BYTE))
    return word_indnex, bit_index

def _sampling(data, word_index, bit_index):
    word = data[word_index]
    # return int(format(word, '032b')[bit_index])
    return format(word, '032b')[bit_index]

class Signature():
    def __init__(self, signature_length, window_size):
        self._signature_length = signature_length
        self._window_size = window_size
        self._sampling_index = _sampling_index_init(signature_length, window_size * NR_BIT_PER_WORD)

    def _slicing(self, data):
        sliced_window = []
        limit = len(data) - self._window_size + 1

        for i in range(0, limit, WINDOW_STEP):
            if(i + self._window_size) > len(data):
                i = len(data) - self._window_size
            w = data[i:i + self._window_size]
            sliced_window.append((w, i))
        return sliced_window

    def _signaturing(self, window):
        signature = ''

        for sample_index in self._sampling_index:
            word_index, bit_index = _sample_index_to_word_and_bit_index(sample_index)
            signature += _sampling(window, word_index, bit_index)
        return signature

    def _slicing_and_signaturing(self, data):
        sliced_window = []
        limit = len(data) - self._window_size + 1

        for i in range(0, limit, WINDOW_STEP):
            if(i + self._window_size) > len(data):
                i = len(data) - self._window_size
            w = data[i:i + self._window_size]
            w = self._signaturing(w)
            sliced_window.append((w, i))
        return sliced_window

    def _slice_and_signature(self, df, data_col, meta_col):
        slicing = self._slicing_and_signaturing
        sliced_rdd = df.select(meta_col, data_col).rdd.\
            map(lambda w : (w[meta_col], slicing(w[data_col]))).\
            flatMapValues(lambda sliced : sliced)
        return sliced_rdd.map(lambda w : pyspark.sql.Row(meta=w[0], signature=w[1][0], origin=w[1][1])).toDF()

    def _hamming_distance_calc(self, a, b):
        x = int(a, 2) ^ int(b, 2)
        return round(((bin(x)[2:].count('1')) / (self._signature_length)), 2)

    def hamming_distance(self, df1, reference, threshold):
        # df1.describe().show()
        return  df1.rdd.map(lambda r : pyspark.sql.Row(meta=r['meta'],
                                                       origin=r['origin'],
                                                       hamming_distance=self._hamming_distance_calc(r['signature'], reference)))\
                   .filter(lambda v : v['hamming_distance'] <= threshold)\
                   .toDF()

    def do(self, df, data_col, meta_col):
        return self._slice_and_signature(df, data_col, meta_col)